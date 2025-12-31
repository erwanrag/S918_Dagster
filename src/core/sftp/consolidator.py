"""
Consolidation intelligente selon load_mode
"""
from pathlib import Path
from datetime import datetime
import pyarrow.parquet as pq
import pyarrow as pa
from src.config.settings import get_settings

settings = get_settings()

def handle_duplicate_files(
    table_name: str,
    file_list: list,  # Liste de SftpFile objects
    consolidated_dir: Path,
    superseded_dir: Path,
    logger
) -> dict:
    """
    Gérer fichiers multiples selon load_mode
    
    Strategy:
    - FULL/FULL_RESET → Garder dernier, archiver le reste
    - INCREMENTAL → Consolider tous les fichiers
    
    Returns:
        dict avec le fichier à charger
    """
    if len(file_list) == 1:
        return file_list[0]
    
    # ✅ Trier par extraction_date (pas modified_time)
    sorted_files = sorted(file_list, key=lambda x: x.extraction_date)
    
    last_file = sorted_files[-1]
    last_mode = last_file.load_mode
    
    # =====================================================================
    # CAS 1 : FULL ou FULL_RESET → Garder dernier, archiver le reste
    # =====================================================================
    if last_mode in ['FULL', 'FULL_RESET']:
        old_files = sorted_files[:-1]
        
        logger.warning(
            f"{table_name}: Mode {last_mode}, keeping latest file, "
            f"archiving {len(old_files)} old files"
        )
        logger.info(f"  Kept: {last_file.path.name}")
        
        # Archiver les anciens
        _archive_old_files(old_files, superseded_dir, logger)
        
        return last_file
    
    # =====================================================================
    # CAS 2 : INCREMENTAL → Consolider tous les fichiers
    # =====================================================================
    logger.info(
        f"{table_name}: Mode INCREMENTAL, consolidating {len(sorted_files)} files"
    )
    
    # Lire et fusionner tous les parquets
    tables = []
    total_rows = 0
    
    for i, file_info in enumerate(sorted_files, 1):
        table = pq.read_table(file_info.path)
        tables.append(table)
        total_rows += len(table)
        logger.info(f"  [{i}/{len(sorted_files)}] {file_info.path.name}: {len(table):,} rows")
    
    # Fusionner
    combined = pa.concat_tables(tables)
    
    # Écrire fichier consolidé
    consolidated_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    consolidated_path = consolidated_dir / f"{table_name}_consolidated_{timestamp}.parquet"
    
    pq.write_table(combined, consolidated_path)
    
    logger.info(
        f"✅ {table_name}: Consolidated {len(sorted_files)} files → "
        f"{consolidated_path.name} ({total_rows:,} rows)"
    )
    
    # Créer metadata consolidé
    consolidated_metadata = {
        'row_count': total_rows,
        'consolidated_from': [f.path.name for f in sorted_files]
    }
    
    # Créer status consolidé
    consolidated_status = {
        'load_mode': 'INCREMENTAL',
        'row_count': total_rows,
        'source_files': len(sorted_files),
        'consolidated_at': datetime.now().isoformat()
    }
    
    # Sauvegarder metadata et status
    _save_consolidated_metadata(
        consolidated_path,
        consolidated_metadata,
        consolidated_status
    )
    
    # ✅ Retourner dict avec extraction_date
    return {
        'path': consolidated_path,
        'table_name': table_name,
        'physical_name': last_file.physical_name,
        'load_mode': 'INCREMENTAL',
        'extraction_date': last_file.extraction_date,  # ✅ Pas modified_time
        'metadata': consolidated_metadata,
        'status': consolidated_status,
        'columns': last_file.columns,  # ✅ Ajouter columns
        'original_files': sorted_files  # ✅ Pour archivage
    }


def _archive_old_files(file_list: list, superseded_dir: Path, logger):
    """Archiver des fichiers obsolètes"""
    superseded_dir.mkdir(parents=True, exist_ok=True)
    
    for file_info in file_list:
        parquet_path = file_info.path
        base_name = parquet_path.stem
        
        # ✅ Utiliser settings pour les chemins
        files_to_archive = {
            'parquet': parquet_path,
            'metadata': settings.sftp_metadata_dir / f"{base_name}_metadata.json",
            'status': settings.sftp_status_dir / f"{base_name}_status.json"
        }
        
        for file_type, src_path in files_to_archive.items():
            if not src_path.exists():
                continue
            
            dest_path = superseded_dir / src_path.name
            
            try:
                src_path.rename(dest_path)
                logger.info(f"🗑️  Archived {file_type:8s}: {src_path.name}")
            except Exception as e:
                logger.warning(f"Failed to archive {src_path}: {e}")


def _save_consolidated_metadata(parquet_path: Path, metadata: dict, status: dict):
    """Sauvegarder metadata/status pour fichier consolidé"""
    import json
    
    base_name = parquet_path.stem
    
    # ✅ Utiliser settings
    metadata_dir = settings.sftp_metadata_dir
    status_dir = settings.sftp_status_dir
    
    metadata_dir.mkdir(parents=True, exist_ok=True)
    status_dir.mkdir(parents=True, exist_ok=True)
    
    # Metadata
    metadata_path = metadata_dir / f"{base_name}_metadata.json"
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    # Status
    status_path = status_dir / f"{base_name}_status.json"
    with open(status_path, 'w') as f:
        json.dump(status, f, indent=2)