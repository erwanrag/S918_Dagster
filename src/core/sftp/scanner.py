"""
============================================================================
SFTP Scanner - Détection fichiers parquet avec métadonnées enrichies
============================================================================
"""

from dataclasses import dataclass, asdict
from pathlib import Path
from datetime import datetime
import json
from typing import Optional

import pyarrow.parquet as pq

from src.config.settings import get_settings
from src.config.constants import LoadMode
from src.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class SftpFile:
    """Représente un fichier SFTP découvert avec métadonnées complètes"""
    
    # Informations fichier
    path: Path
    file_name: str
    size_bytes: int
    
    # Informations table
    table_name: str
    config_name: Optional[str]
    physical_name: str
    
    # Mode de chargement
    load_mode: str
    extraction_date: datetime
    
    # Métadonnées extraction
    row_count: int
    actual_row_count: Optional[int]
    columns: list[dict]  # Métadonnées colonnes ENRICHIES
    progress_database: str
    
    # Statut fichiers
    has_metadata: bool
    has_status: bool
    status_complete: bool
    status_error: Optional[str]
    
    # Validation
    checksum_md5: Optional[str]
    
    # Infos INCREMENTAL
    incremental_info: Optional[dict]
    
    def to_dict(self) -> dict:
        """Convertir en dict pour Dagster"""
        data = asdict(self)
        data["path"] = str(self.path)
        data["extraction_date"] = self.extraction_date.isoformat()
        return data
    
    def get_extent_columns(self) -> list[dict]:
        """Retourne les colonnes avec extent > 0"""
        return [
            col for col in self.columns 
            if col.get("extent", 0) > 0
        ]


def validate_parquet_file(
    parquet_path: Path, 
    expected_rows: int,
    expected_checksum: Optional[str] = None
) -> tuple[bool, int, Optional[str]]:
    """
    Valider qu'un fichier parquet est lisible et cohérent
    
    Args:
        parquet_path: Chemin du fichier parquet
        expected_rows: Nombre de lignes attendu
        expected_checksum: Checksum MD5 attendu (optionnel)
    
    Returns:
        (is_valid, actual_row_count, error_message)
    """
    try:
        # Valider lisibilité
        parquet_file = pq.ParquetFile(parquet_path)
        actual_rows = parquet_file.metadata.num_rows
        
        # Vérifier cohérence row count
        if expected_rows > 0:
            diff = abs(actual_rows - expected_rows)
            tolerance = max(10, expected_rows * 0.01)  # 1% ou 10 rows
            
            if diff > tolerance:
                return (
                    False,
                    actual_rows,
                    f"Row count mismatch: expected {expected_rows}, got {actual_rows}"
                )
        
        # Valider checksum si fourni
        if expected_checksum:
            import hashlib
            hash_md5 = hashlib.md5()
            with open(parquet_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    hash_md5.update(chunk)
            
            actual_checksum = hash_md5.hexdigest()
            if actual_checksum != expected_checksum:
                return (
                    False,
                    actual_rows,
                    f"Checksum mismatch: expected {expected_checksum[:16]}..., "
                    f"got {actual_checksum[:16]}..."
                )
        
        return (True, actual_rows, None)
        
    except Exception as e:
        return (False, 0, f"Parquet read error: {str(e)}")


def scan_parquet_files(validate_files: bool = True) -> list[SftpFile]:
    """
    Scanner les fichiers parquet avec validation complète
    
    Args:
        validate_files: Si True, valide que les parquets sont lisibles
    
    Returns:
        Liste des fichiers découverts avec métadonnées complètes
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
    logger.info("Scanning parquet files", count=len(parquet_files))

    discovered = []
    skipped = {
        "no_metadata": 0, 
        "not_completed": 0, 
        "validation_failed": 0,
        "invalid_metadata": 0
    }
    
    for parquet_path in parquet_files:
        base_name = parquet_path.stem  # "client_20251229_140021"
        
        # Chemins metadata et status
        metadata_path = metadata_dir / f"{base_name}_metadata.json"
        status_path = status_dir / f"{base_name}_status.json"
        
        # ================================================================
        # 1. LIRE METADATA (OBLIGATOIRE)
        # ================================================================
        if not metadata_path.exists():
            logger.warning("No metadata file, skipping", file=parquet_path.name)
            skipped["no_metadata"] += 1
            continue
        
        try:
            with open(metadata_path, "r", encoding="utf-8") as f:
                metadata = json.load(f)
        except Exception as e:
            logger.error("Failed to read metadata", file=parquet_path.name, error=str(e))
            skipped["no_metadata"] += 1
            continue
        
        # Validation structure metadata
        required_fields = ["table_name", "columns", "row_count", "load_mode"]
        if not all(field in metadata for field in required_fields):
            logger.error(
                "Invalid metadata structure, missing fields",
                file=parquet_path.name,
                missing=[f for f in required_fields if f not in metadata]
            )
            skipped["invalid_metadata"] += 1
            continue
        
        # Extraction données metadata
        table_name = metadata.get("table_name", "unknown")
        config_name = metadata.get("config_name")
        physical_name = metadata.get("physical_name") or config_name or table_name
        load_mode = metadata.get("load_mode", LoadMode.INCREMENTAL.value)
        
        extraction_date_str = metadata.get("extract_timestamp")
        try:
            extraction_date = datetime.fromisoformat(extraction_date_str)
        except:
            extraction_date = datetime.now()
        
        row_count = metadata.get("row_count", 0)
        columns = metadata.get("columns", [])
        progress_database = metadata.get("source", "unknown")
        
        # Infos fichier
        file_info = metadata.get("file_info", {})
        checksum_md5 = file_info.get("checksum_md5")
        
        # Infos INCREMENTAL
        incremental_info = metadata.get("incremental_info")
        
        # ================================================================
        # 2. LIRE STATUS (OPTIONNEL)
        # ================================================================
        has_status = status_path.exists()
        status_complete = True
        status_error = None
        
        if has_status:
            try:
                with open(status_path, "r", encoding="utf-8") as f:
                    status = json.load(f)
                
                # Vérifier statut extraction
                status_value = status.get("status", "completed")
                status_complete = (status_value in ["completed", "ready_for_phase2"])
                
                if not status_complete:
                    status_error = status.get("error", f"Status: {status_value}")
                    logger.warning(
                        "File extraction not completed",
                        file=parquet_path.name,
                        status=status_value,
                        error=status_error
                    )
                    skipped["not_completed"] += 1
                    continue
                
                # Override load_mode si présent dans status
                if "load_mode" in status:
                    load_mode = status["load_mode"]
                    
            except Exception as e:
                logger.error("Failed to read status", file=parquet_path.name, error=str(e))
                # Continue quand même si status illisible
        
        # ================================================================
        # 3. VALIDATION PARQUET (OPTIONNEL)
        # ================================================================
        actual_row_count = None
        if validate_files:
            is_valid, actual_row_count, error = validate_parquet_file(
                parquet_path, 
                row_count,
                checksum_md5
            )
            
            if not is_valid:
                logger.error(
                    "Parquet validation failed",
                    file=parquet_path.name,
                    error=error
                )
                skipped["validation_failed"] += 1
                continue
        
        # ================================================================
        # 4. CRÉER OBJET SFTPFILE
        # ================================================================
        size_bytes = parquet_path.stat().st_size
        
        discovered.append(
            SftpFile(
                path=parquet_path,
                file_name=parquet_path.name,
                size_bytes=size_bytes,
                table_name=table_name,
                config_name=config_name,
                physical_name=physical_name,
                load_mode=load_mode,
                extraction_date=extraction_date,
                row_count=row_count,
                actual_row_count=actual_row_count,
                columns=columns,
                progress_database=progress_database,
                has_metadata=True,
                has_status=has_status,
                status_complete=status_complete,
                status_error=status_error,
                checksum_md5=checksum_md5,
                incremental_info=incremental_info,
            )
        )
        
        # Log colonnes EXTENT détectées
        extent_cols = [col for col in columns if col.get("extent", 0) > 0]
        if extent_cols:
            logger.debug(
                "EXTENT columns detected",
                file=parquet_path.name,
                table=table_name,
                extent_count=len(extent_cols)
            )
        
        logger.debug(
            "File scanned",
            file=parquet_path.name,
            table=table_name,
            physical=physical_name,
            mode=load_mode,
            rows=row_count
        )
    
    # ================================================================
    # 5. GESTION DOUBLONS (garder le plus récent par physical_name)
    # ================================================================
    from collections import defaultdict
    
    files_by_table = defaultdict(list)
    for file in discovered:
        files_by_table[file.physical_name].append(file)
    
    final_files = []
    for physical_name, files in files_by_table.items():
        if len(files) > 1:
            logger.warning(
                "Multiple files for same table, keeping most recent",
                table=physical_name,
                count=len(files),
                files=[f.file_name for f in files]
            )
            # Trier par extraction_date (plus récent en premier)
            files.sort(key=lambda f: f.extraction_date, reverse=True)
        
        final_files.append(files[0])
    
    # ================================================================
    # RÉSUMÉ
    # ================================================================
    logger.info(
        "SFTP scan completed",
        total_parquet=len(parquet_files),
        discovered=len(final_files),
        skipped_no_metadata=skipped["no_metadata"],
        skipped_not_completed=skipped["not_completed"],
        skipped_validation_failed=skipped["validation_failed"],
        skipped_invalid_metadata=skipped["invalid_metadata"]
    )
    
    return final_files