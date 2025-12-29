"""
============================================================================
SFTP Scanner - Détection fichiers parquet (3 dossiers comme Prefect)
============================================================================
"""

from dataclasses import dataclass
from pathlib import Path
import json

from src.config.settings import get_settings
from src.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class SftpFile:
    """Représente un fichier SFTP découvert"""

    path: Path
    table_name: str
    load_mode: str
    size_bytes: int


def scan_parquet_files() -> list[SftpFile]:
    """
    Scanner les fichiers parquet avec leurs metadata et status associés.
    
    Structure:
    - /data/sftp_cbmdata01/Incoming/data/parquet/*.parquet
    - /data/sftp_cbmdata01/Incoming/data/metadata/*_metadata.json
    - /data/sftp_cbmdata01/Incoming/data/status/*_status.json

    Returns:
        Liste des fichiers découverts avec metadata
    """
    settings = get_settings()
    
    # Dossiers
    parquet_dir = Path("/data/sftp_cbmdata01/Incoming/data/parquet")
    metadata_dir = Path("/data/sftp_cbmdata01/Incoming/data/metadata")
    status_dir = Path("/data/sftp_cbmdata01/Incoming/data/status")

    if not parquet_dir.exists():
        logger.error("Parquet directory not found", path=str(parquet_dir))
        return []

    parquet_files = list(parquet_dir.glob("*.parquet"))

    discovered = []
    for parquet_path in parquet_files:
        base_name = parquet_path.stem  # "client_20251229_140021"
        
        # Chemins metadata et status
        metadata_path = metadata_dir / f"{base_name}_metadata.json"
        status_path = status_dir / f"{base_name}_status.json"
        
        # Lire metadata (obligatoire)
        if not metadata_path.exists():
            logger.warning("No metadata file, skipping", file=parquet_path.name)
            continue
        
        try:
            with open(metadata_path, "r", encoding="utf-8") as f:
                metadata = json.load(f)
            
            table_name = metadata.get("table_name", "unknown")
            load_mode = metadata.get("load_mode", "UNKNOWN")
            
            # Lire status (optionnel)
            if status_path.exists():
                with open(status_path, "r", encoding="utf-8") as f:
                    status = json.load(f)
                # Override load_mode si présent dans status
                load_mode = status.get("load_mode", load_mode)
            
            size_bytes = parquet_path.stat().st_size
            
            discovered.append(
                SftpFile(
                    path=parquet_path,
                    table_name=table_name,
                    load_mode=load_mode,
                    size_bytes=size_bytes,
                )
            )
            
            logger.debug(
                "File scanned",
                file=parquet_path.name,
                table=table_name,
                mode=load_mode
            )
            
        except Exception as e:
            logger.error("Failed to read metadata", file=parquet_path.name, error=str(e))
            continue

    logger.info("SFTP files scanned", count=len(discovered), directory=str(parquet_dir))
    return discovered