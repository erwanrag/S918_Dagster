"""
============================================================================
Assets STAGING - RAW → STAGING avec AssetMaterializations riches
============================================================================
"""

from dagster import AssetExecutionContext, asset, AssetIn, Output
from tenacity import retry, stop_after_attempt, wait_fixed, RetryError
import time

# ✅ IMPORT MATERIALIZATION
from src.core.materialization import MaterializationBuilder, AssetLayer

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
    Génère AssetMaterializations détaillées avec métriques de transformation.
    """,
)
def staging_tables(
    context: AssetExecutionContext,
    raw_sftp_tables: dict,
) -> dict:
    """Transform RAW → STAGING avec AssetMaterializations riches"""
    
    if not raw_sftp_tables.get("results"):
        # ✅ YIELD Output au lieu de return
        yield Output({
            "results": [], 
            "total_rows": 0, 
            "run_id": raw_sftp_tables.get("run_id", "empty")
        })
        return
    
    run_id = raw_sftp_tables["run_id"]
    context.log.info(f"Starting STAGING transformation with run_id: {run_id}")
    
    results = []
    total_rows = 0
    total_extent_columns_expanded = 0

    # BOUCLE FOR
    for table_info in raw_sftp_tables["results"]:
        # ✅ TRACKING TEMPS PAR TABLE
        table_start = time.time()
        
        table_name = table_info["table"]
        config_name = table_info.get("config_name")
        physical_name = table_info.get("physical_name", table_name)
        load_mode = table_info.get("mode", "FULL")
        raw_rows = table_info.get("rows", 0)  # ✅ Rows depuis RAW
        
        # CRÉER NOUVELLE CONNEXION
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
            
            # Récupérer métadonnées
            metadata = get_table_metadata(conn, table_name, config_name=config_name)
            if not metadata:
                context.log.warning(f"No metadata for {table_name} (config: {config_name}), skipping")
                conn.rollback()
                conn.close()
                continue
            
            columns_metadata = metadata["columns"]
            extent_cols = get_extent_columns(columns_metadata)
            total_cols_expanded = count_expanded_columns(columns_metadata)
            base_cols_count = len(columns_metadata)
            extent_cols_count = len(extent_cols)
            
            if extent_cols:
                context.log.info(
                    f"[{physical_name}] EXTENT columns: {extent_cols_count} "
                    f"→ {total_cols_expanded} columns after split_part()"
                )
            
            # FONCTION RETRY
            @retry(stop=stop_after_attempt(2), wait=wait_fixed(3))
            def process_table_with_retry():
                create_staging_table(table_name, load_mode, conn, config_name=config_name)
                return load_raw_to_staging(
                    table_name=table_name,
                    physical_name=physical_name,
                    run_id=run_id,
                    load_mode=load_mode,
                    conn=conn,
                    config_name=config_name
                )
            
            # Exécuter avec retry
            rows = process_table_with_retry()
            
            # COMMIT IMMÉDIAT
            conn.commit()
            conn.close()
            
            # ✅ CALCUL DURATION
            table_duration = time.time() - table_start
            
            total_rows += rows
            extent_expanded_count = total_cols_expanded - base_cols_count
            total_extent_columns_expanded += extent_expanded_count
            
            # ✅ CRÉER MATERIALIZATION STAGING
            builder = MaterializationBuilder(AssetLayer.STAGING, physical_name)
            
            # Volumétrie (avec source_rows depuis RAW)
            builder.with_volumetry(
                rows_loaded=rows,
                rows_failed=0,
                source_rows=raw_rows  # ✅ Comparaison RAW vs STAGING
            )
            
            builder.with_performance(table_duration)
            builder.with_load_mode(load_mode)
            
            # ✅ MÉTADONNÉES SPÉCIFIQUES STAGING
            builder.metadata.update({
                "base_columns": base_cols_count,
                "extent_columns": extent_cols_count,
                "total_columns_after_expansion": total_cols_expanded,
                "extent_columns_added": extent_expanded_count,
            })
            
            # ✅ DÉTAILS TRANSFORMATIONS
            transformations = []
            if extent_cols_count > 0:
                transformations.append(
                    f"EXTENT explosion: {extent_cols_count} cols → {total_cols_expanded} cols"
                )
            transformations.append("Added _etl_run_id column")
            transformations.append("Added _etl_valid_from timestamp")
            
            builder.metadata["transformations_applied"] = "\n".join(
                f"• {t}" for t in transformations
            )
            
            # ✅ YIELD MATERIALIZATION
            yield builder.build()
            
            results.append({
                "table": table_name,
                "config_name": config_name,
                "physical_name": physical_name,
                "rows": rows,
                "mode": load_mode,
                "base_columns": base_cols_count,
                "extent_columns": extent_cols_count,
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
            
            # ✅ MATERIALIZATION D'ÉCHEC
            table_duration = time.time() - table_start
            builder = MaterializationBuilder(AssetLayer.STAGING, physical_name)
            builder.with_volumetry(0, 0)
            builder.with_performance(table_duration)
            builder.with_error(str(last_error))
            yield builder.build()
            
            continue
            
        except Exception as e:
            if conn:
                conn.rollback()
                conn.close()
            context.log.error(f"❌ STAGING failed for {physical_name}: {str(e)}")
            
            # ✅ MATERIALIZATION D'ÉCHEC
            table_duration = time.time() - table_start
            builder = MaterializationBuilder(AssetLayer.STAGING, physical_name)
            builder.with_volumetry(0, 0)
            builder.with_performance(table_duration)
            builder.with_error(str(e))
            yield builder.build()
            
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

    yield Output({
        "tables_processed": len(results),
        "total_rows": total_rows,
        "results": results,
        "run_id": run_id,
        "total_extent_columns_expanded": total_extent_columns_expanded,
    })