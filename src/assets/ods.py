"""
============================================================================
Assets ODS - STAGING → ODS
============================================================================
"""

from dagster import AssetExecutionContext, asset

from src.core.ods.merger import merge_staging_to_ods
from src.utils.logging import get_logger

logger = get_logger(__name__)


@asset(name="ods_tables_merged", group_name="ods")
def ods_tables_merged(
    context: AssetExecutionContext,
    staging_tables_ready: dict,
) -> dict:
    """Merger STAGING → ODS"""
    results = []
    total_rows = 0

    for table_info in staging_tables_ready["results"]:
        table_name = table_info["table"]
        load_mode = table_info["mode"]
        run_id = staging_tables_ready["run_id"]

        try:
            rows = merge_staging_to_ods(table_name, run_id, load_mode)
            total_rows += rows

            results.append({"table": table_name, "rows": rows, "mode": load_mode})
            context.log.info(f"[OK] {table_name}: {rows:,} rows")

        except Exception as e:
            context.log.error(f"[ERROR] {table_name}: {e}")
            continue

    context.log.info(f"ODS Merge Complete: {len(results)} tables, {total_rows:,} rows")

    return {"tables_merged": len(results), "total_rows": total_rows, "results": results}
