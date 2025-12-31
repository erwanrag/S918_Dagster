"""
============================================================================
Assets RAW - Chargement SFTP → RAW + ARCHIVAGE
============================================================================
"""

import os
from pathlib import Path
from typing import Tuple, Optional

from dagster import AssetExecutionContext, asset

from src.core.raw.loader import load_parquet_to_raw
from src.core.raw.archive import archive_and_cleanup
from src.db.monitoring import log_sftp_file
from src.utils.parallel_utils import process_files_by_size_strategy
from src.utils.metadata_utils import get_extent_columns, count_extent_columns


@asset(
    name="raw_sftp_tables",
    group_name="raw",
    required_resource_keys={"postgres"},
)
def raw_sftp_tables(
    context: AssetExecutionContext,
    sftp_parquet_inventory: list[dict],
) -> dict:
    """
    Charge les fichiers Parquet dans RAW PostgreSQL
    + archive les fichiers SFTP après succès.
    """
    if not sftp_parquet_inventory:
        context.log.info("No files to process")
        return {"results": [], "total_rows": 0}

    sftp_root = Path(os.getenv("ETL_SFTP_ROOT", "/data/sftp_cbmdata01"))
    incoming_data_dir = sftp_root / "Incoming" / "data"
    processed_root = sftp_root / "Processed"

    file_paths = [Path(f["path"]) for f in sftp_parquet_inventory]
    file_metadata = {Path(f["path"]): f for f in sftp_parquet_inventory}

    def process_single_file(
        parquet_path: Path,
    ) -> Tuple[str, str, bool, Optional[str], Optional[int]]:
        metadata = file_metadata[parquet_path]
        physical_name = metadata["physical_name"]

        try:
            # Monitoring
            with context.resources.postgres.get_connection() as conn:
                log_id = log_sftp_file(
                    conn=conn,
                    file_path=parquet_path,
                    table_name=metadata["table_name"],
                    load_mode=metadata["load_mode"],
                )

            # Load RAW
            rows = load_parquet_to_raw(
                parquet_path=parquet_path,
                table_name=metadata["table_name"],
                columns_metadata=metadata["columns"],
                log_id=log_id,
                conn=None,
            )

            # Archivage (uniquement après succès)
            archive_and_cleanup(
                base_filename=parquet_path.stem,
                archive_root=processed_root,
                incoming_data_dir=incoming_data_dir,
                logger=context.log,
            )

            return (physical_name, metadata["load_mode"], True, None, rows)

        except Exception as e:
            context.log.error(f"RAW load failed: {physical_name} - {e}")
            return (physical_name, metadata["load_mode"], False, str(e), None)

    results = process_files_by_size_strategy(file_paths, process_single_file)

    total_rows = sum(r.get("rows", 0) for r in results["success"] if r.get("rows"))
    total_extent_columns = sum(
        count_extent_columns(file_metadata[Path(r["file"])]["columns"])
        for r in results["success"]
    )

    context.add_output_metadata({
        "files_total": results["total"],
        "files_success": len(results["success"]),
        "files_failed": len(results["failed"]),
        "total_rows": total_rows,
        "total_extent_columns": total_extent_columns,
    })

    return {
        "results": results["success"],
        "total_rows": total_rows,
        "success_count": len(results["success"]),
        "failed_count": len(results["failed"]),
    }
