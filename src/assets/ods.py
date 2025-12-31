"""
============================================================================
Assets ODS - STAGING → ODS avec SCD2 (historisation)
============================================================================
FIX: Utilise config_name pour chercher metadata (pour tables lisval)
============================================================================
"""

from dagster import AssetExecutionContext, asset, AssetIn

from src.core.ods.merger import merge_staging_to_ods
from src.db.metadata import get_table_metadata
from src.core.staging.extent import get_extent_columns


@asset(
    name="ods_tables",
    group_name="ods",
    required_resource_keys={"postgres"},
    description="""
    Tables ODS consolidées avec SCD2 (historisation complète).

    Transformations appliquées:
    • STAGING (TEXT) → ODS (types stricts depuis métadonnées)
    • Colonnes EXTENT éclatées typées selon progress_type
    • Historisation SCD2 complète :
      - _etl_valid_from / _etl_valid_to : Période de validité
      - _etl_is_current : TRUE pour ligne active
      - _etl_is_deleted : TRUE pour ligne supprimée (soft delete)
    
    Modes de chargement:
    • INCREMENTAL : Historise modifications + insère nouvelles
    • FULL : + Détecte suppressions (soft delete)
    • FULL_RESET : Réinitialisation complète
    
    Exemple transformation EXTENT:
    - STAGING : znu_1="123.45" (TEXT)
    - ODS : znu_1=123.45 (NUMERIC(32,4))
    """,
    ins={
        "staging_tables": AssetIn(
            key="staging_tables",
            metadata={"description": "Tables STAGING prêtes"}
        )
    },
    metadata={
        "dagster/priority": "high",
        "sla_minutes": 30
    }
)
def ods_tables(
    context: AssetExecutionContext,
    staging_tables: dict,
) -> dict:
    """
    Merger STAGING → ODS avec SCD2 complet
    
    Args:
        staging_tables: Résultats du layer STAGING
    
    Returns:
        Statistiques de merge avec détails SCD2
    """
    results = []
    total_rows = 0
    run_id = staging_tables["run_id"]
    
    context.log.info(f"Starting ODS SCD2 merge with run_id: {run_id}")

    with context.resources.postgres.get_connection() as conn:
        for table_info in staging_tables["results"]:
            table_name = table_info["table"]
            config_name = table_info.get("config_name")  # ✅ RÉCUPÉRER config_name
            physical_name = table_info.get("physical_name", table_name)
            load_mode = table_info["mode"]

            try:
                # ✅ Récupérer métadonnées avec config_name si présent
                metadata = get_table_metadata(conn, table_name, config_name=config_name)
                
                if not metadata:
                    context.log.warning(f"No metadata found for {table_name} (config: {config_name}), skipping")
                    continue
                
                columns_metadata = metadata["columns"]
                extent_cols = get_extent_columns(columns_metadata)
                
                if extent_cols:
                    context.log.info(
                        f"[{physical_name}] Typing {len(extent_cols)} EXTENT "
                        f"column(s) from TEXT to strict types"
                    )
                
                # ✅ Merge avec SCD2 en passant config_name
                rows = merge_staging_to_ods(
                    table_name=table_name,
                    run_id=run_id,
                    load_mode=load_mode,
                    conn=conn,
                    config_name=config_name  # ✅ Passer config_name
                )
                
                conn.commit()
                
                total_rows += rows
                results.append({
                    "table": table_name,
                    "config_name": config_name,  # ✅ Inclure config_name
                    "physical_name": physical_name,
                    "rows": rows,
                    "mode": load_mode,
                })
                
                context.log.info(
                    f"ODS SCD2 merge completed: {physical_name} ({rows:,} rows)"
                )
                
            except Exception as e:
                conn.rollback()
                context.log.error(f"ODS merge failed for {table_name}: {e}")
                # Continue avec les autres tables
                continue
    
    # ================================================================
    # RÉSUMÉ FINAL
    # ================================================================
    context.log.info("=" * 80)
    context.log.info("ODS SCD2 MERGE SUMMARY")
    context.log.info("=" * 80)
    context.log.info(f"Tables merged     : {len(results)}")
    context.log.info(f"Total rows        : {total_rows:,}")
    context.log.info(f"Run ID            : {run_id}")
    context.log.info("=" * 80)
    
    # Détail par table
    for res in results:
        context.log.info(
            f"✅ {res['physical_name']:30s} | "
            f"{res['mode']:12s} | "
            f"{res['rows']:>8,} rows"
        )
    
    # Ajouter métadonnées Dagster
    context.add_output_metadata({
        "tables_merged": len(results),
        "total_rows": total_rows,
        "run_id": run_id,
    })

    return {
        "tables_merged": len(results),
        "total_rows": total_rows,
        "run_id": run_id,
        "results": results,
    }