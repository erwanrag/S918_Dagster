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
from src.utils.metadata_utils import count_extent_columns
from src.utils.filename_parser import parse_and_resolve
from src.config.settings import get_settings


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
    
    Gère les fichiers consolidés et normaux.
    """
    if not sftp_parquet_inventory:
        context.log.info("No files to process")
        return {"results": [], "total_rows": 0, "run_id": "empty"}

    settings = get_settings()
    processed_root = settings.sftp_processed_dir
    
    # ✅ Générer run_id
    from datetime import datetime
    run_id = f"dagster_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    def process_single_file(file_metadata: dict) -> dict:
        """Process un fichier (normal ou consolidé)"""
        
        parquet_path = Path(file_metadata["path"])
        
        # ✅ Parser le nom
        file_name = parquet_path.name
        names = parse_and_resolve(file_name, file_metadata, strict=False)
        
        table_name = names["table_name"]
        config_name = names.get("config_name")
        physical_name = names["physical_name"]
        
        # ✅ Détecter si fichier consolidé
        is_consolidated = 'original_files' in file_metadata
        
        if is_consolidated:
            context.log.info(
                f"Processing CONSOLIDATED: {file_name} → raw_{physical_name} "
                f"(from {len(file_metadata['original_files'])} original files)"
            )
        else:
            context.log.info(
                f"Processing: {file_name} → raw_{physical_name}"
            )

        try:
            # Monitoring
            with context.resources.postgres.get_connection() as conn:
                log_id = log_sftp_file(
                    conn=conn,
                    file_path=parquet_path,
                    table_name=table_name,
                    load_mode=file_metadata["load_mode"],
                )

            # Load RAW
            rows = load_parquet_to_raw(
                parquet_path=parquet_path,
                table_name=table_name,
                config_name=config_name,
                columns_metadata=file_metadata.get("columns", []),
                log_id=log_id,
                conn=None,
            )

            # ✅ ARCHIVAGE selon type
            if is_consolidated:
                # Archiver TOUS les fichiers originaux + consolidé
                _archive_consolidated_files(
                    original_files=file_metadata['original_files'],
                    processed_root=processed_root,
                    logger=context.log, 
                    consolidated_path=parquet_path
                )
            else:
                # Archiver fichier normal
                archive_and_cleanup(
                    base_filename=parquet_path.stem,
                    archive_root=processed_root,
                    incoming_data_dir=settings.sftp_root / "Incoming" / "data",
                    logger=context.log,
                )

            return {
                "table": table_name,
                "config_name": config_name,
                "physical_name": physical_name,
                "rows": rows,
                "mode": file_metadata["load_mode"],
                "success": True,
                "error": None
            }

        except Exception as e:
            context.log.error(f"RAW load failed: {physical_name} - {e}")
            return {
                "table": table_name,
                "config_name": config_name,
                "physical_name": physical_name,
                "rows": 0,
                "mode": file_metadata.get("load_mode", "UNKNOWN"),
                "success": False,
                "error": str(e)
            }

    # ✅ Process tous les fichiers
    all_results = []
    for file_meta in sftp_parquet_inventory:
        result = process_single_file(file_meta)
        all_results.append(result)

    # ✅ Séparer success/failed
    success_results = [r for r in all_results if r["success"]]
    failed_results = [r for r in all_results if not r["success"]]

    total_rows = sum(r["rows"] for r in success_results)
    
    # ✅ Calculer extent columns
    total_extent_columns = 0
    for file_meta in sftp_parquet_inventory:
        total_extent_columns += count_extent_columns(file_meta.get("columns", []))

    # ✅ RÉSUMÉ
    context.log.info("=" * 80)
    context.log.info("RAW LOADING SUMMARY")
    context.log.info("=" * 80)
    context.log.info(f"Files processed   : {len(all_results)}")
    context.log.info(f"Success           : {len(success_results)}")
    context.log.info(f"Failed            : {len(failed_results)}")
    context.log.info(f"Total rows        : {total_rows:,}")
    context.log.info(f"Extent columns    : {total_extent_columns}")
    context.log.info("=" * 80)

    for r in success_results:
        context.log.info(
            f"✅ {r['physical_name']:30s} | {r['mode']:12s} | {r['rows']:>8,} rows"
        )
    
    for r in failed_results:
        context.log.error(
            f"❌ {r['physical_name']:30s} | {r['mode']:12s} | ERROR: {r['error']}"
        )

    context.add_output_metadata({
        "files_total": len(all_results),
        "files_success": len(success_results),
        "files_failed": len(failed_results),
        "total_rows": total_rows,
        "total_extent_columns": total_extent_columns,
        "run_id": run_id,
    })

    return {
        "results": success_results, 
        "total_rows": total_rows,
        "success_count": len(success_results),
        "failed_count": len(failed_results),
        "run_id": run_id,
    }


def _archive_consolidated_files(
    original_files: list,
    processed_root: Path,
    logger,
    consolidated_path: Path = None
):
    """
    Archiver tous les fichiers originaux d'un fichier consolidé
    + le fichier consolidé lui-même + ses métadonnées/status
    """
    from datetime import datetime
    
    settings = get_settings()
    today = datetime.now().strftime("%Y-%m-%d")
    archive_dir = processed_root / today
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    # ✅ 1. Archiver les fichiers originaux
    for file_info in original_files:
        if hasattr(file_info, 'path'):
            parquet_path = file_info.path
        else:
            parquet_path = file_info['path']
        
        base_name = parquet_path.stem
        
        files_to_archive = {
            'parquet': parquet_path,
            'metadata': settings.sftp_metadata_dir / f"{base_name}_metadata.json",
            'status': settings.sftp_status_dir / f"{base_name}_status.json"
        }
        
        for file_type, src_path in files_to_archive.items():
            if not src_path.exists():
                logger.debug(f"Skipping {file_type} (not found): {src_path.name}")
                continue
            
            dest_path = archive_dir / src_path.name
            
            try:
                src_path.rename(dest_path)
                size_mb = dest_path.stat().st_size / (1024 * 1024)
                logger.info(
                    f"📦 {file_type:8s} : {src_path.name:50s} → "
                    f"{today}/{src_path.name} ({size_mb:6.2f} MB)"
                )
            except Exception as e:
                logger.warning(f"Failed to archive {src_path}: {e}")
    
    # ✅ 2. Archiver le fichier consolidé + ses métadonnées/status
    if consolidated_path and consolidated_path.exists():
        base_name = consolidated_path.stem
        
        consolidated_files = {
            'CONSOL': consolidated_path,
            'metadata': settings.sftp_metadata_dir / f"{base_name}_metadata.json",
            'status': settings.sftp_status_dir / f"{base_name}_status.json"
        }
        
        for file_type, src_path in consolidated_files.items():
            if not src_path.exists():
                logger.debug(f"Skipping consolidated {file_type} (not found): {src_path.name}")
                continue
            
            dest_path = archive_dir / src_path.name
            
            try:
                src_path.rename(dest_path)
                size_mb = dest_path.stat().st_size / (1024 * 1024)
                logger.info(
                    f"📦 {file_type:8s} : {src_path.name:50s} → "
                    f"{today}/{src_path.name} ({size_mb:6.2f} MB)"
                )
            except Exception as e:
                logger.warning(f"Failed to archive consolidated {file_type} {src_path}: {e}")