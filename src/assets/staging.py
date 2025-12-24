"""
============================================================================
Assets STAGING - RAW → STAGING
============================================================================
"""

from dagster import AssetExecutionContext, asset
from src.core.staging.transformer import create_staging_table, load_raw_to_staging


@asset(
    name="staging_tables",
    group_name="staging",
    required_resource_keys={"postgres"},
    description="""
    Tables STAGING normalisées.

    • Typage
    • Clés techniques
    • Préparation pour ODS
    """,
)
def staging_tables(
    context: AssetExecutionContext,
    raw_sftp_tables: dict,
) -> dict:
    from datetime import datetime

    run_id = f"dagster_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    results = []
    total_rows = 0

    for table_info in raw_sftp_tables["results"]:
        table_name = table_info["table"]
        load_mode = table_info.get("mode", "FULL")  # Fallback

        try:
            create_staging_table(table_name, load_mode)
            rows = load_raw_to_staging(table_name, run_id, load_mode)
            total_rows += rows

            results.append(
                {"table": table_name, "rows": rows, "mode": load_mode}
            )

            context.log.info(
                "STAGING load completed",
                table=table_name,
                rows=rows,
                load_mode=load_mode,
            )

        except Exception as e:
            context.log.error("STAGING failed", table=table_name, error=str(e))

    return {
        "tables_processed": len(results),
        "total_rows": total_rows,
        "results": results,
        "run_id": run_id,  # ← AJOUTER CETTE LIGNE
    }