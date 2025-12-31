"""
============================================================================
Assets Ingestion - SFTP INVENTORY (scan + enrichissement)
============================================================================
"""

from dagster import AssetExecutionContext, Config, asset
from src.core.sftp.scanner import scan_parquet_files
from src.db.metadata import get_table_metadata
from src.config.constants import LoadMode


class IngestionConfig(Config):
    """Configuration pour filtrer les tables à traiter"""
    tables: list[str] | None = None
    validate_parquet: bool = True


@asset(
    name="sftp_parquet_inventory",
    group_name="ingestion",
    required_resource_keys={"postgres"},
)
def sftp_parquet_inventory(
    context: AssetExecutionContext,
    config: IngestionConfig,
) -> list[dict]:
    """
    Scan SFTP et enrichit avec métadonnées PostgreSQL.
    Aucun effet de bord.
    """
    context.log.info("Starting SFTP scan...")
    files = scan_parquet_files(validate_files=config.validate_parquet)

    if not files:
        context.log.warning("No files discovered on SFTP")
        return []

    # Filtrage optionnel
    if config.tables:
        files = [f for f in files if f.physical_name in config.tables]
        context.log.info(f"After filter: {len(files)} file(s)")

    enriched_files = []

    with context.resources.postgres.get_connection() as conn:
        for file in files:
            table_meta = get_table_metadata(conn, file.table_name)
            file_dict = file.to_dict()

            if table_meta:
                file_dict["primary_keys"] = table_meta["primary_keys"]
                file_dict["has_timestamps"] = table_meta["has_timestamps"]
                file_dict["force_full"] = table_meta["force_full"]
                file_dict["table_description"] = table_meta.get("description", "")

                if table_meta["force_full"]:
                    file_dict["load_mode"] = LoadMode.FULL.value
            else:
                file_dict.update({
                    "primary_keys": [],
                    "has_timestamps": False,
                    "force_full": False,
                    "table_description": "",
                })

            enriched_files.append(file_dict)

    context.log.info(f"SFTP inventory completed: {len(enriched_files)} file(s)")
    return enriched_files
