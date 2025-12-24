"""
============================================================================
Assets Ingestion - Inventaire SFTP
============================================================================
"""

from dagster import AssetExecutionContext, asset
from src.core.sftp.scanner import scan_parquet_files


@asset(
    name="sftp_parquet_inventory",
    group_name="ingestion",
    required_resource_keys={"postgres"},
    description="""
    Inventaire des fichiers Parquet disponibles sur le SFTP.

    • Aucun chargement
    • Lecture seule
    • Sert de point d’entrée au pipeline
    """,
)
def sftp_parquet_inventory(context: AssetExecutionContext) -> list[dict]:
    files = scan_parquet_files()

    discovered = [
        {
            "path": str(f.path),
            "table_name": f.table_name,
            "load_mode": f.load_mode,
            "size_bytes": f.size_bytes,
        }
        for f in files
    ]

    context.log.info("SFTP inventory built", file_count=len(discovered))
    return discovered
