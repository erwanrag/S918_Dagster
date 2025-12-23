"""
============================================================================
SFTP Scanner - Détection fichiers parquet
============================================================================
"""

from dataclasses import dataclass
from pathlib import Path

from src.config.settings import get_settings
from src.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class SftpFile:
    """Représente un fichier SFTP découvert"""

    path: Path
    table_name: str
    load_mode: str
    date_str: str
    size_bytes: int


def scan_parquet_files() -> list[SftpFile]:
    """
    Scanner le répertoire SFTP pour les fichiers .parquet

    Returns:
        Liste des fichiers découverts
    """
    settings = get_settings()
    parquet_dir = settings.sftp_parquet_dir

    if not parquet_dir.exists():
        logger.error("SFTP directory not found", path=str(parquet_dir))
        return []

    parquet_files = list(parquet_dir.glob("*.parquet"))

    discovered = []
    for path in parquet_files:
        # Parser le nom de fichier: table_YYYYMMDD_HHMMSS.parquet
        parts = path.stem.split("_")

        if len(parts) < 3:
            logger.warning("Invalid filename format", file=path.name)
            continue

        table_name = "_".join(parts[:-2])
        date_str = parts[-2]
        load_mode = parts[-1].upper()
        size_bytes = path.stat().st_size

        discovered.append(
            SftpFile(
                path=path,
                table_name=table_name,
                load_mode=load_mode,
                date_str=date_str,
                size_bytes=size_bytes,
            )
        )

    logger.info("SFTP files scanned", count=len(discovered), directory=str(parquet_dir))
    return discovered
