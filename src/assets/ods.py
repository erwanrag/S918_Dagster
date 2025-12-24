"""Assets ODS"""
from dagster import AssetExecutionContext, asset

from src.core.ods.merger import merge_staging_to_ods


@asset(
    name="ods_tables",
    group_name="ods",
    required_resource_keys={"postgres"},
    description="""
    Tables ODS consolidées.

    • Logique métier
    • Fusion / historisation
    • Source unique pour dbt
    """,
)
def ods_tables_merged(
    context: AssetExecutionContext,
    staging_tables_ready: dict,
) -> dict:
    results = []
    total_rows = 0
    run_id = staging_tables_ready["run_id"]

    with context.resources.postgres.get_connection() as conn:
        for table_info in staging_tables_ready["results"]:
            table_name = table_info["table"]
            load_mode = table_info["mode"]

            try:
                rows = merge_staging_to_ods(
                    table_name=table_name,
                    run_id=run_id,
                    load_mode=load_mode,
                    conn=conn,
                )
                total_rows += rows
                results.append({"table": table_name, "rows": rows})
                context.log.info(f"[OK] {table_name}: {rows:,} rows")
            except Exception as e:
                context.log.error(f"[ERROR] {table_name}: {e}")
                # rollback géré par la resource
                continue

    return {"tables_merged": len(results), "total_rows": total_rows}