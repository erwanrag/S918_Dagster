"""
============================================================================
Assets Ingestion - SFTP INVENTORY (scan + enrichissement + consolidation)
============================================================================
FIX: Gère les listes retournées par handle_duplicate_files (tables multi-config)
============================================================================
"""

from pathlib import Path
from datetime import datetime
from dagster import AssetExecutionContext, Config, asset

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
    
    Strategy:
    - FULL/FULL_RESET : Garde dernier fichier, archive le reste
    - INCREMENTAL : Consolide tous les fichiers en un seul
    - Multi-config tables (lisval) : Retourne tous les fichiers séparément
    """
    settings = get_settings()
    
    context.log.info("Starting SFTP scan...")
    all_files = scan_parquet_files(validate_files=config.validate_parquet)

    if not all_files:
        context.log.warning("No files discovered on SFTP")
        return []

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
        # Filtrage optionnel
        if config.tables and table_name not in config.tables:
            context.log.debug(f"Skipping {table_name} (not in filter)")
            continue
        
        if len(file_list) == 1:
            final_files.append(file_list[0])
        else:
            # ✅ handle_duplicate_files retourne maintenant une LISTE
            # - Pour tables multi-config (lisval) : liste de N fichiers
            # - Pour tables normales : liste de 1 fichier (consolidé ou dernier)
            result_files = handle_duplicate_files(
                table_name=table_name,
                file_list=file_list,
                consolidated_dir=consolidated_dir,
                superseded_dir=superseded_dir,
                logger=context.log
            )
            # ✅ Ajouter TOUS les fichiers de la liste
            final_files.extend(result_files)

    context.log.info(
        f"After consolidation: {len(final_files)} file(s) to load "
        f"(from {len(all_files)} original files)"
    )

    # =========================================================================
    # 3. ENRICHIR AVEC MÉTADONNÉES POSTGRESQL
    # =========================================================================
    enriched_files = []

    with context.resources.postgres.get_connection() as conn:
        for file in final_files:
            # ✅ Gérer à la fois les objets FileInfo et les dicts (fichiers consolidés)
            if isinstance(file, dict):
                table_name = file['table_name']
                config_name = file.get('config_name')  # ✅ Récupérer config_name
                file_dict = file
            else:
                table_name = file.table_name
                config_name = getattr(file, 'config_name', None)  # ✅ Récupérer config_name
                file_dict = file.to_dict()
            
            # ✅ Récupérer métadonnées avec config_name
            table_meta = get_table_metadata(conn, table_name, config_name=config_name)

            if table_meta:
                file_dict["primary_keys"] = table_meta["primary_keys"]
                file_dict["has_timestamps"] = table_meta["has_timestamps"]
                file_dict["force_full"] = table_meta["force_full"]
                file_dict["table_description"] = table_meta.get("description", "")
                file_dict["columns"] = table_meta["columns"]  # ✅ Ajouter colonnes

                # Override load_mode si force_full
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

    # =========================================================================
    # 4. RÉSUMÉ
    # =========================================================================
    context.log.info("=" * 80)
    context.log.info("SFTP INVENTORY SUMMARY")
    context.log.info("=" * 80)
    context.log.info(f"Total discovered      : {len(all_files)}")
    context.log.info(f"After consolidation   : {len(enriched_files)}")
    context.log.info(f"Skipped (filter)      : {len(all_files) - len(files_by_table) if config.tables else 0}")
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

    return enriched_files