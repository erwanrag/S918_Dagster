"""
============================================================================
Assets ODS - STAGING → ODS avec SCD2 + AssetMaterializations riches
============================================================================
"""

from dagster import AssetExecutionContext, asset, AssetIn, Output
import time

from src.core.materialization import MaterializationBuilder, AssetLayer
from src.core.ods.merger import merge_staging_to_ods
from src.db.metadata import get_table_metadata
from src.core.staging.extent import get_extent_columns


@asset(
    name="ods_tables",
    group_name="ods",
    required_resource_keys={"postgres"},
    ins={
        "staging_tables": AssetIn(key="staging_tables"),
    },
    metadata={
        "dagster/priority": "high",
        "sla_minutes": 30,
    },
)
def ods_tables(
    context: AssetExecutionContext,
    staging_tables: dict,
) -> Output:
    """
    Merger STAGING → ODS avec SCD2 complet + logs et métriques sécurisées
    """

    # ------------------------------------------------------------------
    # 🔒 RUN ID — FAIL FAST
    # ------------------------------------------------------------------
    run_id = staging_tables.get("run_id")
    if not run_id:
        raise ValueError("Missing run_id in staging_tables")

    context.log.info("Starting ODS SCD2 merge", extra={"run_id": run_id})

    results = []
    total_rows = 0

    global_scd2_stats = {
        "new_records": 0,
        "updated_records": 0,
        "closed_records": 0,
        "deleted_records": 0,
    }

    with context.resources.postgres.get_connection() as conn:
        for table_info in staging_tables["results"]:
            table_start = time.time()

            table_name = table_info["table"]
            config_name = table_info.get("config_name")
            physical_name = table_info.get("physical_name", table_name)
            load_mode = table_info["mode"]
            staging_rows = table_info.get("rows", 0)

            try:
                metadata = get_table_metadata(conn, table_name, config_name=config_name)
                if not metadata:
                    context.log.warning(
                        "No metadata found, skipping table",
                        extra={"table": table_name, "run_id": run_id},
                    )
                    continue

                extent_cols = get_extent_columns(metadata["columns"])
                if extent_cols:
                    context.log.info(
                        "Typing EXTENT columns",
                        extra={
                            "table": physical_name,
                            "extent_columns": len(extent_cols),
                        },
                    )

                merge_result = merge_staging_to_ods(
                    table_name=table_name,
                    run_id=run_id,
                    load_mode=load_mode,
                    conn=conn,
                    config_name=config_name,
                )

                rows = merge_result.get("total_rows", 0)
                scd2_new = merge_result.get("new_records", 0)
                scd2_updated = merge_result.get("updated_records", 0)
                scd2_closed = merge_result.get("closed_records", 0)
                scd2_deleted = merge_result.get("deleted_records", 0)

                conn.commit()

                duration = time.time() - table_start
                total_rows += rows

                # ------------------------------------------------------------------
                # ⚠️ WARNING SI STAGING ≠ 0 MAIS ODS = 0
                # ------------------------------------------------------------------
                if staging_rows > 0 and rows == 0:
                    context.log.warning(
                        "ODS merge produced no rows",
                        extra={
                            "table": physical_name,
                            "run_id": run_id,
                            "staging_rows": staging_rows,
                            "load_mode": load_mode,
                        },
                    )

                # Agrégation SCD2
                global_scd2_stats["new_records"] += scd2_new
                global_scd2_stats["updated_records"] += scd2_updated
                global_scd2_stats["closed_records"] += scd2_closed
                global_scd2_stats["deleted_records"] += scd2_deleted

                # ------------------------------------------------------------------
                # 📦 MATERIALIZATION
                # ------------------------------------------------------------------
                builder = MaterializationBuilder(AssetLayer.ODS, physical_name)
                builder.with_volumetry(
                    rows_loaded=rows,
                    rows_failed=0,
                    source_rows=staging_rows,
                )
                builder.with_performance(duration)
                builder.with_load_mode(load_mode)

                builder.metadata.update(
                    {
                        "scd2_new_records": scd2_new,
                        "scd2_updated_records": scd2_updated,
                        "scd2_closed_records": scd2_closed,
                        "scd2_deleted_records": scd2_deleted,
                    }
                )

                yield builder.build()

                # ------------------------------------------------------------------
                # 📊 LOG STRUCTURÉ
                # ------------------------------------------------------------------
                context.log.info(
                    "ODS merge completed",
                    extra={
                        "layer": "ods",
                        "table": physical_name,
                        "run_id": run_id,
                        "rows": rows,
                        "scd2_new": scd2_new,
                        "scd2_updated": scd2_updated,
                        "scd2_closed": scd2_closed,
                        "scd2_deleted": scd2_deleted,
                        "load_mode": load_mode,
                        "duration_sec": round(duration, 2),
                    },
                )

                results.append(
                    {
                        "table": table_name,
                        "physical_name": physical_name,
                        "rows": rows,
                        "mode": load_mode,
                        "scd2_new": scd2_new,
                        "scd2_updated": scd2_updated,
                        "scd2_closed": scd2_closed,
                        "scd2_deleted": scd2_deleted,
                    }
                )

            except Exception as e:
                conn.rollback()
                context.log.error(
                    "ODS merge failed",
                    extra={"table": table_name, "run_id": run_id, "error": str(e)},
                )

                builder = MaterializationBuilder(AssetLayer.ODS, physical_name)
                builder.with_error(str(e))
                yield builder.build()

    # ----------------------------------------------------------------------
    # 🧾 RÉSUMÉ FINAL
    # ----------------------------------------------------------------------
    context.log.info(
        "ODS SCD2 summary",
        extra={
            "tables_merged": len(results),
            "total_rows": total_rows,
            "run_id": run_id,
            **global_scd2_stats,
        },
    )

    yield Output(
        {
            "tables_merged": len(results),
            "total_rows": total_rows,
            "run_id": run_id,
            "results": results,
            "scd2_stats": global_scd2_stats,
        },
        metadata={
            "tables_merged": len(results),
            "total_rows": total_rows,
            "run_id": run_id,
            "scd2_new_total": global_scd2_stats["new_records"],
            "scd2_updated_total": global_scd2_stats["updated_records"],
            "scd2_closed_total": global_scd2_stats["closed_records"],
            "scd2_deleted_total": global_scd2_stats["deleted_records"],
        },
    )
