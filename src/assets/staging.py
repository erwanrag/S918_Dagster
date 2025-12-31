"""
============================================================================
Assets STAGING - RAW → STAGING avec éclatement EXTENT
============================================================================
FIX: Utilise config_name pour chercher metadata (pour tables lisval)
============================================================================
"""

from dagster import AssetExecutionContext, asset, AssetIn
from tenacity import retry, stop_after_attempt, wait_fixed, RetryError

from src.core.staging.transformer import create_staging_table, load_raw_to_staging
from src.core.staging.extent import count_expanded_columns, get_extent_columns
from src.db.metadata import get_table_metadata


@asset(
    name="staging_tables",
    group_name="staging",
    required_resource_keys={"postgres"},
    ins={"raw_sftp_tables": AssetIn(key="raw_sftp_tables")},
    description="""
    Tables STAGING normalisées avec éclatement EXTENT.

    Transformations appliquées:
    • Colonnes EXTENT éclatées avec split_part(colonne, ';', N)
    • Colonnes ETL (_etl_run_id, _etl_valid_from)

    Exemple transformation EXTENT:
    - RAW : znu TEXT = "123.45;0;0;0;0"
    - STAGING : znu_1="123.45", znu_2="0", znu_3="0", znu_4="0", znu_5="0"
    """,
)
def staging_tables(
    context: AssetExecutionContext,
    raw_sftp_tables: dict,
) -> dict:
    """Transform RAW → STAGING avec éclatement EXTENT"""
    
    if not raw_sftp_tables.get("results"):
        return {
            "results": [], 
            "total_rows": 0, 
            "run_id": raw_sftp_tables.get("run_id", "empty")
        }
    
    run_id = raw_sftp_tables["run_id"]
    context.log.info(f"Starting STAGING transformation with run_id: {run_id}")
    
    results = []
    total_rows = 0
    total_extent_columns_expanded = 0

    # ✅ BOUCLE FOR
    for table_info in raw_sftp_tables["results"]:
        table_name = table_info["table"]
        config_name = table_info.get("config_name")  # ✅ RÉCUPÉRER config_name
        physical_name = table_info.get("physical_name", table_name)
        load_mode = table_info.get("mode", "FULL")
        
        # ✅ CRÉER UNE VRAIE NOUVELLE CONNEXION
        import psycopg2
        import os
        
        conn = None
        try:
            conn = psycopg2.connect(
                host=os.getenv("ETL_PG_HOST"),
                port=int(os.getenv("ETL_PG_PORT", "5432")),
                database=os.getenv("ETL_PG_DATABASE"),
                user=os.getenv("ETL_PG_USER"),
                password=os.getenv("ETL_PG_PASSWORD"),
            )
            conn.autocommit = False
            
            # ✅ Récupérer métadonnées avec config_name si présent
            metadata = get_table_metadata(conn, table_name, config_name=config_name)
            if not metadata:
                context.log.warning(f"No metadata found for {table_name} (config: {config_name}), skipping")
                conn.rollback()
                conn.close()
                continue
            
            columns_metadata = metadata["columns"]
            extent_cols = get_extent_columns(columns_metadata)
            total_cols_expanded = count_expanded_columns(columns_metadata)
            
            if extent_cols:
                context.log.info(
                    f"[{physical_name}] EXTENT columns: {len(extent_cols)} "
                    f"→ {total_cols_expanded} columns after split_part()"
                )
            
            # ✅ FONCTION RETRY
            @retry(stop=stop_after_attempt(2), wait=wait_fixed(3))
            def process_table_with_retry():
                create_staging_table(table_name, load_mode, conn, config_name=config_name)
                return load_raw_to_staging(
                    table_name=table_name,
                    physical_name=physical_name,
                    run_id=run_id,
                    load_mode=load_mode,
                    conn=conn,
                    config_name=config_name  # ✅ Passer config_name
                )
            
            # Exécuter avec retry
            rows = process_table_with_retry()
            
            # ✅ COMMIT IMMÉDIAT
            conn.commit()
            conn.close()
            
            total_rows += rows
            total_extent_columns_expanded += (total_cols_expanded - len(columns_metadata))
            
            results.append({
                "table": table_name,
                "config_name": config_name,  # ✅ Inclure config_name
                "physical_name": physical_name,
                "rows": rows,
                "mode": load_mode,
                "base_columns": len(columns_metadata),
                "extent_columns": len(extent_cols),
                "total_columns_expanded": total_cols_expanded,
            })
            
            context.log.info(f"✅ STAGING completed: {physical_name} ({rows:,} rows)")

        except RetryError as e:
            if conn:
                conn.rollback()
                conn.close()
            last_error = e.last_attempt.exception()
            context.log.error(
                f"❌ STAGING failed after retries for {physical_name}: {last_error}"
            )
            continue
            
        except Exception as e:
            if conn:
                conn.rollback()
                conn.close()
            context.log.error(f"❌ STAGING failed for {physical_name}: {str(e)}")
            continue
    
    # ================================================================
    # RÉSUMÉ FINAL
    # ================================================================
    context.log.info("=" * 80)
    context.log.info("STAGING TRANSFORMATION SUMMARY")
    context.log.info("=" * 80)
    context.log.info(f"Tables processed        : {len(results)}")
    context.log.info(f"Total rows loaded       : {total_rows:,}")
    context.log.info(f"Total EXTENT columns    : {total_extent_columns_expanded}")
    context.log.info(f"Run ID                  : {run_id}")
    context.log.info("=" * 80)
    
    # Détail par table
    for res in results:
        extent_info = ""
        if res["extent_columns"] > 0:
            extent_info = (
                f"EXTENT: {res['extent_columns']} cols → "
                f"{res['total_columns_expanded']} after split"
            )
        
        context.log.info(
            f"✅ {res['physical_name']:30s} | "
            f"{res['mode']:12s} | "
            f"{res['rows']:>8,} rows | "
            f"{extent_info}"
        )
    
    # Ajouter métadonnées Dagster
    context.add_output_metadata({
        "tables_processed": len(results),
        "total_rows": total_rows,
        "total_extent_columns_expanded": total_extent_columns_expanded,
        "run_id": run_id,
    })

    return {
        "tables_processed": len(results),
        "total_rows": total_rows,
        "results": results,
        "run_id": run_id,
        "total_extent_columns_expanded": total_extent_columns_expanded,
    }