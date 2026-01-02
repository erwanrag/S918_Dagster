"""
Assets RAW - Chargement SFTP → RAW + QUALITY + MATERIALIZATION
"""

import os
from pathlib import Path
from datetime import datetime
import time

from dagster import AssetExecutionContext, asset, Output

# ✅ IMPORT DU MODULE MATERIALIZATION
from src.core.materialization import create_raw_materialization, AssetLayer

from src.core.raw.loader import load_parquet_to_raw
from src.core.raw.quality import (
    RawDataQualityChecker, 
    generate_quality_report_html, 
    save_quality_report_json
)
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
    Charge fichiers Parquet → RAW avec AssetMaterializations riches
    """
    if not sftp_parquet_inventory:
        context.log.info("No files to process")
        yield Output({"results": [], "total_rows": 0})
        return

    settings = get_settings()
    processed_root = settings.sftp_processed_dir
    quality_reports_dir = Path("/data/dagster/data_quality_reports")
    quality_reports_dir.mkdir(parents=True, exist_ok=True)
    
    run_id = f"dagster_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    enable_quality_check = os.getenv("ENABLE_QUALITY_CHECK", "false").lower() == "true"
    
    quality_checker = RawDataQualityChecker(max_issues_per_type=100) if enable_quality_check else None

    # ===================================================================
    # TRAITER TOUS LES FICHIERS
    # ===================================================================
    all_results = []
    
    for file_meta in sftp_parquet_inventory:
        # ✅ TRACKING TEMPS
        start_time = time.time()
        
        parquet_path = Path(file_meta["path"])
        file_name = parquet_path.name
        names = parse_and_resolve(file_name, file_meta, strict=False)
        
        table_name = names["table_name"]
        config_name = names.get("config_name")
        physical_name = names["physical_name"]
        is_consolidated = 'original_files' in file_meta
        
        context.log.info(f"Processing: {file_name} → raw_{physical_name}")

        try:
            # ===========================================================
            # ✅ 1. CALCUL TAILLE FICHIER **AVANT TOUT**
            # ===========================================================
            file_size_mb = parquet_path.stat().st_size / (1024 ** 2)
            extent_count = count_extent_columns(file_meta.get("columns", []))
            
            # ===========================================================
            # 2. QUALITY CHECK (optionnel)
            # ===========================================================
            quality_passed = True
            quality_issues_count = 0
            
            if quality_checker:
                context.log.info(f"🔍 Running quality checks: {table_name}")
                
                quality_report = quality_checker.check_parquet_file(
                    parquet_path=parquet_path,
                    table_name=table_name,
                    sample_size=10000
                )
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                html_path = quality_reports_dir / f"{table_name}_quality_{timestamp}.html"
                json_path = quality_reports_dir / f"{table_name}_quality_{timestamp}.json"
                
                generate_quality_report_html(quality_report, html_path)
                save_quality_report_json(quality_report, json_path)
                
                quality_passed = quality_report.passed
                quality_issues_count = len(quality_report.issues)
                
                error_count = sum(1 for i in quality_report.issues if i.severity == 'error')
                warning_count = sum(1 for i in quality_report.issues if i.severity == 'warning')
                
                if error_count > 0:
                    context.log.warning(f"❌ Quality FAILED: {error_count} errors, {warning_count} warnings")
                elif warning_count > 0:
                    context.log.info(f"⚠️ Quality PASSED with {warning_count} warnings")
                else:
                    context.log.info(f"✅ Quality PASSED")
            
            # ===========================================================
            # 3. MONITORING
            # ===========================================================
            with context.resources.postgres.get_connection() as conn:
                log_id = log_sftp_file(
                    conn=conn,
                    file_path=parquet_path,
                    table_name=table_name,
                    load_mode=file_meta["load_mode"],
                )

            # ===========================================================
            # 4. LOAD RAW
            # ===========================================================
            rows = load_parquet_to_raw(
                parquet_path=parquet_path,
                table_name=table_name,
                config_name=config_name,
                columns_metadata=file_meta.get("columns", []),
                log_id=log_id,
                conn=None,
            )

            # ===========================================================
            # ✅ 5. CRÉER MATERIALIZATION **AVANT** ARCHIVAGE
            # ===========================================================
            duration = time.time() - start_time
            
            materialization = create_raw_materialization(
                table_name=physical_name,
                rows_loaded=rows,
                rows_failed=0,
                source_path=str(parquet_path),
                load_mode=file_meta["load_mode"],
                extent_columns=extent_count,
                quality_passed=quality_passed,
                quality_issues=quality_issues_count,
                is_consolidated=is_consolidated,
                original_files_count=len(file_meta['original_files']) if is_consolidated else 0,
                file_size_mb=file_size_mb,  # ✅ Utilise valeur calculée AVANT
                duration_seconds=duration,
            )
            
            # ✅ YIELD materialization
            yield materialization

            # ===========================================================
            # ✅ 6. ARCHIVAGE **APRÈS** MATERIALIZATION
            # ===========================================================
            if is_consolidated:
                _archive_consolidated_files(
                    original_files=file_meta['original_files'],
                    processed_root=processed_root,  
                    logger=context.log,  
                    consolidated_path=parquet_path  
                )
            else:
                from src.core.raw.archive import archive_and_cleanup
                archive_and_cleanup(
                    base_filename=parquet_path.stem,
                    archive_root=processed_root,
                    incoming_data_dir=settings.sftp_root / "Incoming" / "data",
                    logger=context.log,
                )

            # ✅ APPEND RESULT
            all_results.append({
                "table": table_name,
                "config_name": config_name,
                "physical_name": physical_name,
                "rows": rows,
                "mode": file_meta["load_mode"],
                "extent_columns": extent_count,
                "quality_passed": quality_passed,
                "quality_issues": quality_issues_count,
                "success": True,
                "error": None
            })

        except Exception as e:
            context.log.error(f"RAW load failed: {physical_name} - {e}")
            
            duration = time.time() - start_time
            
            # Materialization d'échec
            materialization = create_raw_materialization(
                table_name=physical_name,
                rows_loaded=0,
                rows_failed=0,
                load_mode=file_meta.get("load_mode", "UNKNOWN"),
                extent_columns=0,
                quality_passed=False,
                quality_issues=0,
            )
            materialization.metadata["error"] = str(e)[:500]
            materialization.metadata["status"] = "❌ FAILED"
            
            yield materialization
            
            all_results.append({
                "table": table_name,
                "config_name": config_name,
                "physical_name": physical_name,
                "rows": 0,
                "mode": file_meta.get("load_mode", "UNKNOWN"),
                "extent_columns": 0,
                "quality_passed": False,
                "quality_issues": 0,
                "success": False,
                "error": str(e)
            })

    # ===================================================================
    # RÉSUMÉ
    # ===================================================================
    success_results = [r for r in all_results if r["success"]]
    failed_results = [r for r in all_results if not r["success"]]
    
    total_rows = sum(r["rows"] for r in success_results)
    
    context.log.info("=" * 80)
    context.log.info("RAW LOADING SUMMARY")
    context.log.info("=" * 80)
    context.log.info(f"Success: {len(success_results)} | Failed: {len(failed_results)} | Total rows: {total_rows:,}")
    context.log.info("=" * 80)

    # ✅ YIELD Output final
    yield Output({
        "results": success_results,
        "total_rows": total_rows,
        "success_count": len(success_results),
        "failed_count": len(failed_results),
        "run_id": run_id,
    })


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
    from src.config.settings import get_settings
    
    settings = get_settings()
    today = datetime.now().strftime("%Y-%m-%d")
    archive_dir = processed_root / today
    archive_dir.mkdir(parents=True, exist_ok=True)
    
    # 1. Archiver les fichiers originaux
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
    
    # ✅ 2. ARCHIVER + SUPPRIMER LE FICHIER CONSOLIDÉ
    if consolidated_path and consolidated_path.exists():
        base_name = consolidated_path.stem
        
        # Fichiers du consolidé
        consolidated_files = {
            'parquet': consolidated_path,
            'metadata': settings.sftp_metadata_dir / f"{base_name}_metadata.json",
            'status': settings.sftp_status_dir / f"{base_name}_status.json"
        }
        
        for file_type, src_path in consolidated_files.items():
            if not src_path.exists():
                logger.debug(f"Skipping consolidated {file_type}: {src_path.name}")
                continue
            
            dest_path = archive_dir / src_path.name
            
            try:
                # ✅ MOVE (pas copy) pour vraiment SUPPRIMER le consolidated
                src_path.rename(dest_path)
                size_mb = dest_path.stat().st_size / (1024 * 1024)
                logger.info(
                    f"📦 {file_type:8s} : {src_path.name:50s} → "
                    f"{today}/{src_path.name} ({size_mb:6.2f} MB) [CONSOLIDATED]"
                )
            except Exception as e:
                logger.warning(f"Failed to archive consolidated {src_path}: {e}")
                # Si move échoue, au moins supprimer
                try:
                    src_path.unlink()
                    logger.info(f"🗑️  Deleted consolidated: {src_path.name}")
                except Exception as e2:
                    logger.error(f"Failed to delete consolidated {src_path}: {e2}")