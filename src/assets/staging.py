"""
============================================================================
Assets STAGING - RAW → STAGING avec éclatement EXTENT
============================================================================
Transformation des colonnes EXTENT (TEXT) en colonnes multiples (TEXT)
Exemple: zal ("AAA;;;;") → zal_1 ("AAA"), zal_2 (""), zal_3 (""), ...
============================================================================
"""

from dagster import AssetExecutionContext, asset
from datetime import datetime

from src.core.staging.transformer import create_staging_table, load_raw_to_staging
from src.core.staging.extent import count_expanded_columns, get_extent_columns
from src.db.metadata import get_table_metadata


@asset(
    name="staging_tables",
    group_name="staging",
    required_resource_keys={"postgres"},
    description="""
    Tables STAGING normalisées avec éclatement EXTENT.

    Transformations appliquées:
    • Colonnes EXTENT éclatées avec split_part(colonne, ';', N)
    • Typage flexible (TEXT) pour validation en ODS
    • Hashdiff MD5 pour déduplication
    • Colonnes ETL (_etl_hashdiff, _etl_run_id, _etl_valid_from)
    
    Exemple transformation EXTENT:
    - RAW : znu TEXT = "123.45;0;0;0;0"
    - STAGING : znu_1="123.45", znu_2="0", znu_3="0", znu_4="0", znu_5="0"
    """,
)
def staging_tables(
    context: AssetExecutionContext,
    raw_sftp_tables: dict,
) -> dict:
    """
    Transformer RAW → STAGING avec éclatement EXTENT
    
    Args:
        raw_sftp_tables: Résultats du chargement RAW
    
    Returns:
        Statistiques de transformation avec détails EXTENT
    """
    run_id = f"dagster_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    context.log.info(f"Starting STAGING transformation with run_id: {run_id}")
    
    results = []
    total_rows = 0
    total_extent_columns_expanded = 0

    with context.resources.postgres.get_connection() as conn:
        for table_info in raw_sftp_tables["results"]:
            table_name = table_info["table"]
            physical_name = table_info.get("physical_name", table_name)
            load_mode = table_info.get("mode", "FULL")
            
            try:
                # Récupérer métadonnées pour compter colonnes EXTENT
                metadata = get_table_metadata(conn, table_name)
                
                if not metadata:
                    context.log.warning(
                        f"No metadata found for {table_name}, skipping"
                    )
                    continue
                
                columns_metadata = metadata["columns"]
                extent_cols = get_extent_columns(columns_metadata)
                total_cols_expanded = count_expanded_columns(columns_metadata)
                
                # Log détails EXTENT
                if extent_cols:
                    context.log.info(
                        f"[{physical_name}] EXTENT columns: {len(extent_cols)} "
                        f"→ {total_cols_expanded} columns after split_part()"
                    )
                    for col_name, extent in extent_cols.items():
                        context.log.debug(
                            f"  {col_name} (extent={extent}) → "
                            f"{col_name}_1...{col_name}_{extent}"
                        )
                
                # Créer table STAGING
                create_staging_table(table_name, load_mode, conn)
                
                # Charger données avec éclatement
                rows = load_raw_to_staging(table_name, run_id, load_mode, conn)
                
                conn.commit()
                
                total_rows += rows
                total_extent_columns_expanded += (total_cols_expanded - len(columns_metadata))
                
                results.append({
                    "table": table_name,
                    "physical_name": physical_name,
                    "rows": rows,
                    "mode": load_mode,
                    "base_columns": len(columns_metadata),
                    "extent_columns": len(extent_cols),
                    "total_columns_expanded": total_cols_expanded,
                })
                
                context.log.info(
                    f"STAGING load completed: {physical_name} "
                    f"({rows:,} rows, {total_cols_expanded} columns)"
                )

            except Exception as e:
                conn.rollback()
                context.log.error(
                    f"STAGING failed for {table_name}: {str(e)}"
                )
                # Continue avec les autres tables
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