"""
============================================================================
Assets ODS - STAGING → ODS avec SCD2 + AssetMaterializations riches
============================================================================
"""

from dagster import AssetExecutionContext, asset, AssetIn, Output
import time

# ✅ IMPORT MATERIALIZATION
from src.core.materialization import MaterializationBuilder, AssetLayer

from src.core.ods.merger import merge_staging_to_ods
from src.db.metadata import get_table_metadata
from src.core.staging.extent import get_extent_columns


@asset(
    name="ods_tables",
    group_name="ods",
    required_resource_keys={"postgres"},
    description="""
    Tables ODS consolidées avec SCD2 (historisation complète).
    Génère AssetMaterializations avec métriques SCD2 détaillées.
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
    Merger STAGING → ODS avec SCD2 complet + AssetMaterializations riches
    """
    results = []
    total_rows = 0
    run_id = staging_tables["run_id"]
    
    context.log.info(f"Starting ODS SCD2 merge with run_id: {run_id}")

    # ✅ TRACKING SCD2 GLOBAL
    global_scd2_stats = {
        "new_records": 0,
        "updated_records": 0,
        "closed_records": 0,
        "deleted_records": 0,
    }

    with context.resources.postgres.get_connection() as conn:
        for table_info in staging_tables["results"]:
            # ✅ TRACKING TEMPS PAR TABLE
            table_start = time.time()
            
            table_name = table_info["table"]
            config_name = table_info.get("config_name")
            physical_name = table_info.get("physical_name", table_name)
            load_mode = table_info["mode"]
            staging_rows = table_info.get("rows", 0)  # ✅ Rows depuis STAGING

            try:
                # Récupérer métadonnées
                metadata = get_table_metadata(conn, table_name, config_name=config_name)
                
                if not metadata:
                    context.log.warning(f"No metadata for {table_name} (config: {config_name}), skipping")
                    continue
                
                columns_metadata = metadata["columns"]
                extent_cols = get_extent_columns(columns_metadata)
                
                if extent_cols:
                    context.log.info(
                        f"[{physical_name}] Typing {len(extent_cols)} EXTENT "
                        f"column(s) from TEXT to strict types"
                    )
                
                # ✅ MERGE AVEC SCD2 (doit retourner dict avec stats)
                # ATTENTION: Il faut modifier merge_staging_to_ods pour retourner stats SCD2
                merge_result = merge_staging_to_ods(
                    table_name=table_name,
                    run_id=run_id,
                    load_mode=load_mode,
                    conn=conn,
                    config_name=config_name
                )
                
                # ✅ GÉRER RETOUR (rows simple OU dict avec stats)
                if isinstance(merge_result, dict):
                    rows = merge_result.get("total_rows", 0)
                    scd2_new = merge_result.get("new_records", 0)
                    scd2_updated = merge_result.get("updated_records", 0)
                    scd2_closed = merge_result.get("closed_records", 0)
                    scd2_deleted = merge_result.get("deleted_records", 0)
                else:
                    # Fallback si merge_staging_to_ods retourne juste int
                    rows = merge_result
                    scd2_new = 0
                    scd2_updated = 0
                    scd2_closed = 0
                    scd2_deleted = 0
                
                conn.commit()
                
                # ✅ CALCUL DURATION
                table_duration = time.time() - table_start
                
                total_rows += rows
                
                # Agrégation stats SCD2
                global_scd2_stats["new_records"] += scd2_new
                global_scd2_stats["updated_records"] += scd2_updated
                global_scd2_stats["closed_records"] += scd2_closed
                global_scd2_stats["deleted_records"] += scd2_deleted
                
                # ✅ CRÉER MATERIALIZATION ODS
                builder = MaterializationBuilder(AssetLayer.ODS, physical_name)
                
                # Volumétrie
                builder.with_volumetry(
                    rows_loaded=rows,
                    rows_failed=0,
                    source_rows=staging_rows  # ✅ Comparaison STAGING vs ODS
                )
                
                builder.with_performance(table_duration)
                builder.with_load_mode(load_mode)
                
                # ✅ MÉTRIQUES SCD2 (méthode spécifique)
                if scd2_new > 0 or scd2_updated > 0 or scd2_closed > 0:
                    builder.metadata.update({
                        "scd2_new_records": scd2_new,
                        "scd2_updated_records": scd2_updated,
                        "scd2_closed_records": scd2_closed,
                    })
                    
                    if scd2_deleted > 0:
                        builder.metadata["scd2_deleted_records"] = scd2_deleted
                    
                    # ✅ SUMMARY MARKDOWN SCD2
                    builder.metadata["scd2_summary"] = f"""
### 🔄 SCD2 Changes

| Type | Count |
|------|-------|
| 🆕 New | {scd2_new:,} |
| 🔄 Updated | {scd2_updated:,} |
| 🔒 Closed | {scd2_closed:,} |
| 🗑️ Deleted | {scd2_deleted:,} |
                    """
                
                # ✅ TYPE CASTING EXTENT
                if extent_cols:
                    builder.metadata["extent_columns_typed"] = len(extent_cols)
                    builder.metadata["type_casting"] = f"TEXT → strict types ({len(extent_cols)} cols)"
                
                # ✅ YIELD MATERIALIZATION
                yield builder.build()
                
                results.append({
                    "table": table_name,
                    "config_name": config_name,
                    "physical_name": physical_name,
                    "rows": rows,
                    "mode": load_mode,
                    "scd2_new": scd2_new,
                    "scd2_updated": scd2_updated,
                    "scd2_closed": scd2_closed,
                })
                
                context.log.info(
                    f"✅ ODS SCD2: {physical_name} ({rows:,} rows) | "
                    f"New: {scd2_new} | Updated: {scd2_updated} | Closed: {scd2_closed}"
                )
                
            except Exception as e:
                conn.rollback()
                context.log.error(f"❌ ODS merge failed for {table_name}: {e}")
                
                # ✅ MATERIALIZATION D'ÉCHEC
                table_duration = time.time() - table_start
                builder = MaterializationBuilder(AssetLayer.ODS, physical_name)
                builder.with_volumetry(0, 0)
                builder.with_performance(table_duration)
                builder.with_error(str(e))
                yield builder.build()
                
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
    context.log.info("")
    context.log.info("SCD2 Global Stats:")
    context.log.info(f"  🆕 New records      : {global_scd2_stats['new_records']:,}")
    context.log.info(f"  🔄 Updated records  : {global_scd2_stats['updated_records']:,}")
    context.log.info(f"  🔒 Closed records   : {global_scd2_stats['closed_records']:,}")
    context.log.info(f"  🗑️ Deleted records  : {global_scd2_stats['deleted_records']:,}")
    context.log.info("=" * 80)
    
    # Détail par table
    for res in results:
        scd2_detail = ""
        if res.get("scd2_new", 0) > 0 or res.get("scd2_updated", 0) > 0:
            scd2_detail = (
                f"SCD2: New={res['scd2_new']} | "
                f"Upd={res['scd2_updated']} | "
                f"Closed={res['scd2_closed']}"
            )
        
        context.log.info(
            f"✅ {res['physical_name']:30s} | "
            f"{res['mode']:12s} | "
            f"{res['rows']:>8,} rows | "
            f"{scd2_detail}"
        )
    
    # Métadonnées Dagster
    context.add_output_metadata({
        "tables_merged": len(results),
        "total_rows": total_rows,
        "scd2_new_total": global_scd2_stats["new_records"],
        "scd2_updated_total": global_scd2_stats["updated_records"],
        "scd2_closed_total": global_scd2_stats["closed_records"],
        "run_id": run_id,
    })

    yield Output({
        "tables_merged": len(results),
        "total_rows": total_rows,
        "run_id": run_id,
        "results": results,
        "scd2_stats": global_scd2_stats,
    })