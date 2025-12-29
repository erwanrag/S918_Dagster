"""
============================================================================
Assets Ingestion - Inventaire SFTP
============================================================================
"""

from dagster import AssetExecutionContext, Config, asset
from src.core.sftp.scanner import scan_parquet_files


class IngestionConfig(Config):
    """Configuration pour filtrer les tables à traiter"""
    tables: list[str] | None = None  # None = toutes les tables


@asset(
    name="sftp_parquet_inventory",
    group_name="ingestion",
    required_resource_keys={"postgres"},
    description="""
    Inventaire des fichiers Parquet disponibles sur le SFTP.

    • Aucun chargement
    • Lecture seule
    • Sert de point d'entrée au pipeline
    • Config optionnelle : 'tables' pour filtrer les tables à traiter
    """,
)
def sftp_parquet_inventory(
    context: AssetExecutionContext,
    config: IngestionConfig,
) -> list[dict]:
    files = scan_parquet_files()

    # Filtrer par tables si config fournie
    if config.tables:
        context.log.info(f"Filtrage activé : {len(config.tables)} table(s) sélectionnée(s)")
        files = [f for f in files if f.table_name in config.tables]
        context.log.info(f"Après filtre : {len(files)} fichier(s)")

    discovered = [
        {
            "path": str(f.path),
            "table_name": f.table_name,
            "load_mode": f.load_mode,
            "size_bytes": f.size_bytes,
        }
        for f in files
    ]

    context.log.info(f"SFTP inventory built: {len(discovered)} files")
    
    if config.tables:
        context.log.info(f"Tables sélectionnées : {', '.join(config.tables)}")
    
    return discovered