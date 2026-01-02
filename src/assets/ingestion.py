"""
============================================================================
Assets Ingestion - SFTP INVENTORY avec AssetMaterializations riches
============================================================================
"""

from pathlib import Path
from datetime import datetime
from dagster import AssetExecutionContext, Config, asset, Output  # ✅ Ajouter Output
import time

# ✅ IMPORT MATERIALIZATION
from src.core.materialization import MaterializationBuilder, AssetLayer

from src.core.sftp.scanner import scan_parquet_files
from src.core.sftp.consolidator import handle_duplicate_files
from src.db.metadata import get_table_metadata
from src.config.constants import LoadMode
from src.config.settings import get_settings


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
    Scan SFTP, consolide doublons, enrichit avec métadonnées PostgreSQL.
    Génère AssetMaterializations par table découverte.
    """
    start_time = time.time()
    
    settings = get_settings()
    
    context.log.info("Starting SFTP scan...")
    all_files = scan_parquet_files(validate_files=config.validate_parquet)

    if not all_files:
        context.log.warning("No files discovered on SFTP")
        yield Output([])  # ✅ yield Output au lieu de return
        return  # ✅ Sortir après yield

    context.log.info(f"Discovered {len(all_files)} parquet file(s)")

    # =========================================================================
    # 1. GROUPER PAR TABLE
    # =========================================================================
    files_by_table = {}
    for file in all_files:
        files_by_table.setdefault(file.table_name, []).append(file)

    # =========================================================================
    # 2. GÉRER LES DOUBLONS (consolidation ou archivage)
    # =========================================================================
    consolidated_dir = settings.sftp_parquet_dir / "_consolidated"
    superseded_dir = (
        settings.sftp_processed_dir / 
        datetime.now().strftime("%Y-%m-%d") / 
        "superseded"
    )
    
    final_files = []
    
    for table_name, file_list in files_by_table.items():
        if config.tables and table_name not in config.tables:
            context.log.debug(f"Skipping {table_name} (not in filter)")
            continue
        
        if len(file_list) == 1:
            final_files.append(file_list[0])
        else:
            result_files = handle_duplicate_files(
                table_name=table_name,
                file_list=file_list,
                consolidated_dir=consolidated_dir,
                superseded_dir=superseded_dir,
                logger=context.log
            )
            final_files.extend(result_files)

    context.log.info(
        f"After consolidation: {len(final_files)} file(s) to load "
        f"(from {len(all_files)} original files)"
    )

    # =========================================================================
    # 3. ENRICHIR AVEC MÉTADONNÉES + MATERIALIZATIONS
    # =========================================================================
    enriched_files = []
    tables_stats = {}

    with context.resources.postgres.get_connection() as conn:
        for file in final_files:
            file_start = time.time()
            
            if isinstance(file, dict):
                table_name = file['table_name']
                config_name = file.get('config_name')
                file_dict = file
                file_path = Path(file['path'])
            else:
                table_name = file.table_name
                config_name = getattr(file, 'config_name', None)
                file_dict = file.to_dict()
                file_path = Path(file.path)
            
            table_meta = get_table_metadata(conn, table_name, config_name=config_name)

            if table_meta:
                file_dict["primary_keys"] = table_meta["primary_keys"]
                file_dict["has_timestamps"] = table_meta["has_timestamps"]
                file_dict["force_full"] = table_meta["force_full"]
                file_dict["table_description"] = table_meta.get("description", "")
                file_dict["columns"] = table_meta["columns"]

                if table_meta["force_full"]:
                    file_dict["load_mode"] = LoadMode.FULL.value
                    context.log.info(f"{table_name}: force_full=True → FULL mode")
            else:
                file_dict.update({
                    "primary_keys": [],
                    "has_timestamps": False,
                    "force_full": False,
                    "table_description": "",
                    "columns": []
                })
                context.log.warning(f"{table_name}: No metadata found in PostgreSQL")

            enriched_files.append(file_dict)
            
            file_size_mb = file_path.stat().st_size / (1024 ** 2) if file_path.exists() else 0
            is_consolidated = 'original_files' in file_dict
            original_count = len(file_dict['original_files']) if is_consolidated else 1
            column_count = len(file_dict.get('columns', []))
            
            file_duration = time.time() - file_start
            
            physical_name = file_dict.get('physical_name', table_name)
            
            builder = MaterializationBuilder(AssetLayer.INGESTION, physical_name)
            builder.with_source(str(file_path), file_size_mb)
            builder.with_load_mode(file_dict['load_mode'])
            builder.with_performance(file_duration)
            
            builder.metadata.update({
                "file_discovered": True,
                "column_count": column_count,
                "has_primary_keys": len(file_dict.get('primary_keys', [])) > 0,
                "has_timestamps": file_dict.get('has_timestamps', False),
            })
            
            if is_consolidated:
                builder.with_consolidation_info(
                    is_consolidated=True,
                    original_files_count=original_count
                )
            
            # ✅ YIELD MATERIALIZATION
            yield builder.build()
            
            load_mode = file_dict['load_mode']
            if load_mode not in tables_stats:
                tables_stats[load_mode] = {"count": 0, "total_size_mb": 0}
            tables_stats[load_mode]["count"] += 1
            tables_stats[load_mode]["total_size_mb"] += file_size_mb

    # =========================================================================
    # 4. RÉSUMÉ
    # =========================================================================
    total_duration = time.time() - start_time
    
    context.log.info("=" * 80)
    context.log.info("SFTP INVENTORY SUMMARY")
    context.log.info("=" * 80)
    context.log.info(f"Total discovered      : {len(all_files)}")
    context.log.info(f"After consolidation   : {len(enriched_files)}")
    context.log.info(f"Duration              : {total_duration:.2f}s")
    context.log.info("")
    
    for mode, stats in tables_stats.items():
        context.log.info(
            f"{mode:12s} : {stats['count']} files ({stats['total_size_mb']:.2f} MB)"
        )
    
    context.log.info("=" * 80)
    
    for file_dict in enriched_files:
        physical_name = file_dict.get('physical_name', file_dict.get('table_name'))
        load_mode = file_dict.get('load_mode', 'UNKNOWN')
        is_consolidated = 'original_files' in file_dict
        
        if is_consolidated:
            source_count = len(file_dict['original_files'])
            context.log.info(
                f"📦 {physical_name:30s} | {load_mode:12s} | "
                f"CONSOLIDATED from {source_count} files"
            )
        else:
            context.log.info(
                f"📄 {physical_name:30s} | {load_mode:12s}"
            )
    
    context.log.info("=" * 80)
    
    context.add_output_metadata({
        "files_discovered": len(all_files),
        "files_after_consolidation": len(enriched_files),
        "duration_seconds": round(total_duration, 2),
        "full_mode_count": tables_stats.get("FULL", {}).get("count", 0),
        "incremental_mode_count": tables_stats.get("INCREMENTAL", {}).get("count", 0),
    })

    # ✅ YIELD Output au lieu de return
    yield Output(enriched_files)