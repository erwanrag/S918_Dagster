"""
============================================================================
Assets Ingestion - Inventaire SFTP avec métadonnées enrichies
============================================================================
"""

from dagster import AssetExecutionContext, Config, asset
from src.core.sftp.scanner import scan_parquet_files
from src.db.metadata import get_table_metadata
from src.config.constants import LoadMode


class IngestionConfig(Config):
    """Configuration pour filtrer les tables à traiter"""
    tables: list[str] | None = None      # None = toutes les tables
    validate_parquet: bool = True         # Valider intégrité parquets


@asset(
    name="sftp_parquet_inventory",
    group_name="ingestion",
    required_resource_keys={"postgres"},
    description="""
    Inventaire ENRICHI des fichiers Parquet disponibles sur le SFTP.

    Métadonnées extraites :
    • Fichier : path, name, size, checksum MD5
    • Table : table_name, config_name, physical_name
    • Extraction : load_mode, extraction_date, row_count
    • Colonnes : types Progress + PostgreSQL, extent, width, scale
    • Config PostgreSQL : primary_keys, force_full, has_timestamps
    • Validation : status, parquet integrity, checksums
    
    Filtres appliqués :
    • Skip si pas de metadata.json
    • Skip si status != "completed" / "ready_for_phase2"
    • Skip si parquet corrompu (optionnel)
    • Skip si checksum invalide
    • Garde le plus récent si doublons
    
    Config optionnelle :
    • 'tables' : filtrer les tables à traiter (par physical_name)
    • 'validate_parquet' : valider intégrité des fichiers (default: True)
    """,
)
def sftp_parquet_inventory(
    context: AssetExecutionContext,
    config: IngestionConfig,
) -> list[dict]:
    """
    Scan SFTP et enrichit avec métadonnées PostgreSQL
    
    Returns:
        Liste de dicts avec TOUTES les infos nécessaires pour l'ETL :
        - Infos fichier (path, size, name, checksum)
        - Infos table (table_name, config_name, physical_name)
        - Infos extraction (load_mode, date, row_count)
        - Infos colonnes (types, extent, width, scale)
        - Infos config (primary_keys, force_full, has_timestamps)
        - Validation (status, checksum ok)
    """
    # ================================================================
    # 1. SCAN SFTP
    # ================================================================
    context.log.info("Starting SFTP scan...")
    files = scan_parquet_files(validate_files=config.validate_parquet)
    
    context.log.info(f"SFTP scan completed: {len(files)} file(s) discovered")
    
    if not files:
        context.log.warning("No files discovered on SFTP")
        return []
    
    # ================================================================
    # 2. FILTRER PAR TABLES (si config fournie)
    # ================================================================
    if config.tables:
        context.log.info(f"Filter enabled: {len(config.tables)} table(s) selected")
        context.log.info(f"Tables: {', '.join(config.tables)}")
        
        # Filtrer par physical_name (pas table_name)
        files = [f for f in files if f.physical_name in config.tables]
        
        context.log.info(f"After filter: {len(files)} file(s)")
    
    if not files:
        context.log.warning("No files after filter")
        return []
    
    # ================================================================
    # 3. ENRICHIR AVEC MÉTADONNÉES POSTGRESQL
    # ================================================================
    context.log.info("Enriching with PostgreSQL metadata...")
    enriched_files = []
    
    with context.resources.postgres.get_connection() as conn:
        for file in files:
            # Récupérer config PostgreSQL
            table_meta = get_table_metadata(conn, file.table_name)
            
            # Convertir en dict
            file_dict = file.to_dict()
            
            # Enrichir avec config PostgreSQL
            if table_meta:
                file_dict["primary_keys"] = table_meta["primary_keys"]
                file_dict["has_timestamps"] = table_meta["has_timestamps"]
                file_dict["force_full"] = table_meta["force_full"]
                file_dict["table_description"] = table_meta.get("description", "")
                
                # Override load_mode si force_full=True
                if table_meta["force_full"]:
                    original_mode = file_dict["load_mode"]
                    file_dict["load_mode"] = LoadMode.FULL.value
                    
                    context.log.info(
                        f"Force FULL mode enabled for {file.physical_name} "
                        f"(original: {original_mode})"
                    )
            else:
                # Table pas dans metadata.etl_tables
                context.log.warning(
                    f"No PostgreSQL metadata found for {file.table_name} "
                    f"(physical: {file.physical_name})"
                )
                file_dict["primary_keys"] = []
                file_dict["has_timestamps"] = False
                file_dict["force_full"] = False
                file_dict["table_description"] = ""
            
            # Compter colonnes EXTENT
            extent_cols = file.get_extent_columns()
            if extent_cols:
                file_dict["extent_columns_count"] = len(extent_cols)
                context.log.debug(
                    f"{file.physical_name}: {len(extent_cols)} EXTENT column(s)"
                )
            else:
                file_dict["extent_columns_count"] = 0
            
            enriched_files.append(file_dict)
    
    # ================================================================
    # 4. LOG RÉSUMÉ
    # ================================================================
    context.log.info("=" * 80)
    context.log.info("SFTP INVENTORY SUMMARY")
    context.log.info("=" * 80)
    context.log.info(f"Total files discovered : {len(enriched_files)}")
    
    # Grouper par load_mode
    by_mode = {}
    for f in enriched_files:
        mode = f["load_mode"]
        by_mode[mode] = by_mode.get(mode, 0) + 1
    
    for mode, count in by_mode.items():
        context.log.info(f"  {mode:15s} : {count} file(s)")
    
    # Total rows estimé
    total_rows = sum(f["row_count"] for f in enriched_files)
    context.log.info(f"Estimated total rows : {total_rows:,}")
    
    # Total EXTENT columns
    total_extent = sum(f.get("extent_columns_count", 0) for f in enriched_files)
    context.log.info(f"Total EXTENT columns : {total_extent}")
    
    if config.tables:
        context.log.info(f"Filtered tables      : {', '.join(config.tables)}")
    
    context.log.info("=" * 80)
    
    # Détail par fichier
    for f in enriched_files:
        extent_info = f"EXTENT: {f.get('extent_columns_count', 0)}" if f.get("extent_columns_count", 0) > 0 else ""
        pk_info = f"PK: {','.join(f['primary_keys'][:2])}" if f['primary_keys'] else "PK: none"
        
        context.log.info(
            f"📦 {f['physical_name']:30s} | "
            f"{f['load_mode']:12s} | "
            f"{f['row_count']:>8,} rows | "
            f"{f['size_bytes'] / 1024 / 1024:>6.1f} MB | "
            f"{pk_info:20s} | "
            f"{extent_info}"
        )
    
    # Ajouter métadonnées Dagster
    context.add_output_metadata({
        "total_files": len(enriched_files),
        "total_rows": total_rows,
        "total_size_mb": round(sum(f["size_bytes"] for f in enriched_files) / 1024 / 1024, 2),
        "load_modes": by_mode,
        "tables": [f["physical_name"] for f in enriched_files],
        "total_extent_columns": total_extent,
    })
    
    return enriched_files