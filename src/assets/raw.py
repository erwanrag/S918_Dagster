"""
============================================================================
Assets RAW - Chargement SFTP → RAW
============================================================================
"""

from pathlib import Path
from typing import Tuple, Optional

from dagster import AssetExecutionContext, asset, MetadataValue

from src.core.raw.loader import load_parquet_to_raw
from src.db.monitoring import log_sftp_file
from src.utils.parallel_utils import process_files_by_size_strategy


@asset(
    name="raw_sftp_tables",
    group_name="raw",
    required_resource_keys={"postgres"},
    description="""
    Tables RAW issues du SFTP.

    • Données brutes
    • 1 fichier = 1 table RAW
    • Aucune logique métier
    """,
)
def raw_sftp_tables(
    context: AssetExecutionContext,
    sftp_parquet_inventory: list[dict],
) -> dict:
    file_paths = [Path(f["path"]) for f in sftp_parquet_inventory]
    file_metadata = {Path(f["path"]): f for f in sftp_parquet_inventory}

    def process_single_file(
        parquet_path: Path,
    ) -> Tuple[str, str, bool, Optional[str]]:
        metadata = file_metadata[parquet_path]
        table_name = metadata["table_name"]
        load_mode = metadata["load_mode"]

        try:
            with context.resources.postgres.get_connection() as conn:
                log_id = log_sftp_file(
                    conn=conn,
                    file_path=parquet_path,
                    table_name=table_name,
                    load_mode=load_mode,
                )

                rows = load_parquet_to_raw(
                    parquet_path=parquet_path,
                    table_name=table_name,
                    log_id=log_id,
                    conn=conn,
                )

            context.log.info(
                "RAW load completed",
                table=table_name,
                rows=rows,
                load_mode=load_mode,
            )
            return (table_name, load_mode, True, None)

        except Exception as e:
            return (table_name, load_mode, False, str(e))

    results = process_files_by_size_strategy(file_paths, process_single_file)

    total_rows = sum(
        file_metadata[Path(r["file"])]["size_bytes"] // 1000
        for r in results["success"]
    )
    
    # Formatter les résultats avec mode
    formatted_results = [
        {
            "table": r["table"],
            "mode": file_metadata[Path(r["file"])]["load_mode"],
            "rows": file_metadata[Path(r["file"])]["size_bytes"] // 1000,
        }
        for r in results["success"]
    ]

    context.add_output_metadata({
        "files_total": results["total"],
        "files_success": len(results["success"]),
        "files_failed": len(results["failed"]),
        "total_rows": total_rows,
        "load_modes": MetadataValue.text(
            ", ".join(sorted({m["load_mode"] for m in file_metadata.values()}))
        ),
    })

    return {
        "results": formatted_results,  # ← Utiliser formatted_results au lieu de results["success"]
        "total_rows": total_rows,
    }