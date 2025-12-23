"""
============================================================================
SFTP Parser - Lecture fichiers metadata.json
============================================================================
"""

import json
from pathlib import Path
from typing import Any

from src.utils.logging import get_logger

logger = get_logger(__name__)


def read_metadata_json(parquet_path: Path) -> dict[str, Any] | None:
    """
    Lire le fichier metadata.json associé au parquet

    Args:
        parquet_path: Chemin du fichier .parquet

    Returns:
        Dictionnaire des métadonnées ou None
    """
    metadata_path = parquet_path.with_suffix(".metadata.json")

    if not metadata_path.exists():
        logger.debug("Metadata file not found", parquet=parquet_path.name)
        return None

    try:
        with open(metadata_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)
            logger.debug("Metadata loaded", parquet=parquet_path.name)
            return metadata
    except Exception as e:
        logger.error("Failed to read metadata", parquet=parquet_path.name, error=str(e))
        return None
