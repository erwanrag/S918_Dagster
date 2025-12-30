"""
============================================================================
Assets RAW - Chargement SFTP → RAW (copie directe)
============================================================================
Les colonnes EXTENT sont stockées comme TEXT (string avec ";")
Elles seront éclatées en STAGING
============================================================================
"""

from pathlib import Path
from typing import Tuple, Optional

from dagster import AssetExecutionContext, asset

from src.core.raw.loader import load_parquet_to_raw
from src.db.monitoring import log_sftp_file
from src.utils.parallel_utils import process_files_by_size_strategy
from src.utils.metadata_utils import get_extent_columns, count_extent_columns


@asset(
    name="raw_sftp_tables",
    group_name="raw",
    required_resource_keys={"postgres"},
    description="""
    Tables RAW issues du SFTP (copie directe du Parquet).

    • Données brutes sans transformation
    • 1 fichier = 1 table RAW
    • Typage depuis metadata.json
    • Colonnes EXTENT → TEXT (stockées comme "val1;val2;val3...")
    • Pas d'éclatement (se fera en STAGING)
    """,
)
def raw_sftp_tables(
    context: AssetExecutionContext,
    sftp_parquet_inventory: list[dict],
) -> dict:
    """
    Charge les fichiers Parquet dans les tables RAW (copie directe)
    
    Args:
        sftp_parquet_inventory: Inventaire enrichi depuis asset ingestion
    
    Returns:
        Statistiques de chargement
    """
    context.log.info(f"Starting raw_sftp_tables with {len(sftp_parquet_inventory)} files")
    
    # Cas 0 fichiers
    if not sftp_parquet_inventory:
        context.log.info("No files to process, returning empty result")
        return {"results": [], "total_rows": 0, "total_extent_columns": 0}
    
    # Construire mappings
    file_paths = [Path(f["path"]) for f in sftp_parquet_inventory]
    file_metadata = {Path(f["path"]): f for f in sftp_parquet_inventory}
    
    context.log.info(f"Processing {len(file_paths)} file(s)")

    def process_single_file(
        parquet_path: Path,
    ) -> Tuple[str, str, bool, Optional[str], Optional[int]]:
        """Traite un seul fichier parquet"""
        
        metadata = file_metadata[parquet_path]
        table_name = metadata["table_name"]
        physical_name = metadata["physical_name"]
        load_mode = metadata["load_mode"]
        columns_metadata = metadata["columns"]
        
        # Log colonnes EXTENT
        extent_cols = get_extent_columns(columns_metadata)
        if extent_cols:
            context.log.info(
                f"[{physical_name}] {len(extent_cols)} EXTENT column(s) detected, "
                f"will be stored as TEXT (split in STAGING)"
            )

        try:
            # ÉTAPE 1: Log monitoring
            with context.resources.postgres.get_connection() as conn:
                log_id = log_sftp_file(
                    conn=conn,
                    file_path=parquet_path,
                    table_name=table_name,
                    load_mode=load_mode,
                )
                context.log.debug(f"[{physical_name}] Log ID: {log_id}")
            
            # ÉTAPE 2: Load parquet avec métadonnées
            rows = load_parquet_to_raw(
                parquet_path=parquet_path,
                table_name=table_name,
                columns_metadata=columns_metadata,
                log_id=log_id,
                conn=None,  # Loader crée sa propre connexion
            )
            
            context.log.info(
                f"RAW load completed: {physical_name} ({rows:,} rows, "
                f"{len(columns_metadata)} columns)"
            )
            
            return (physical_name, load_mode, True, None, rows)

        except Exception as e:
            context.log.error(f"RAW load failed: {physical_name} - {str(e)}")
            return (physical_name, load_mode, False, str(e), None)

    # ================================================================
    # TRAITER LES FICHIERS (parallèle intelligent)
    # ================================================================
    context.log.info("Processing files with size-based strategy...")
    results = process_files_by_size_strategy(file_paths, process_single_file)
    
    # ================================================================
    # CALCULER STATISTIQUES
    # ================================================================
    total_rows = sum(r.get("rows", 0) for r in results["success"] if r.get("rows"))
    total_extent_columns = sum(
        count_extent_columns(file_metadata[Path(r["file"])]["columns"])
        for r in results["success"]
    )
    
    # Formatter les résultats
    formatted_results = []
    for r in results["success"]:
        file_meta = file_metadata[Path(r["file"])]
        formatted_results.append({
            "table": r["table"],
            "physical_name": file_meta["physical_name"],
            "mode": r["mode"],
            "rows": r.get("rows", 0),
            "columns": len(file_meta["columns"]),
            "extent_columns": count_extent_columns(file_meta["columns"]),
        })

    # ================================================================
    # RÉSUMÉ FINAL
    # ================================================================
    context.log.info("=" * 80)
    context.log.info("RAW LOADING SUMMARY")
    context.log.info("=" * 80)
    context.log.info(f"Files processed      : {results['total']}")
    context.log.info(f"  Success            : {len(results['success'])}")
    context.log.info(f"  Failed             : {len(results['failed'])}")
    context.log.info(f"Total rows loaded    : {total_rows:,}")
    context.log.info(f"Total EXTENT columns : {total_extent_columns} (stored as TEXT)")
    
    if results['failed']:
        context.log.warning("Failed files:")
        for f in results['failed']:
            # f est un tuple: (physical_name, load_mode, False, error_msg, None)
            physical_name = f[0] if isinstance(f, tuple) else f.get('table', 'unknown')
            error_msg = f[3] if isinstance(f, tuple) else f.get('error', 'Unknown error')
            context.log.warning(f"  ❌ {physical_name}: {error_msg}")
    
    context.log.info("=" * 80)
    
    # Détail par table
    for res in formatted_results:
        extent_info = ""
        if res['extent_columns'] > 0:
            extent_info = f"EXTENT: {res['extent_columns']} (TEXT)"
        
        context.log.info(
            f"✅ {res['physical_name']:30s} | "
            f"{res['mode']:12s} | "
            f"{res['rows']:>8,} rows | "
            f"{res['columns']:>3} cols | "
            f"{extent_info}"
        )

    # Ajouter métadonnées Dagster
    context.add_output_metadata({
        "files_total": results["total"],
        "files_success": len(results["success"]),
        "files_failed": len(results["failed"]),
        "total_rows": total_rows,
        "total_extent_columns": total_extent_columns,
    })

    return {
        "results": formatted_results,
        "total_rows": total_rows,
        "total_extent_columns": total_extent_columns,
        "success_count": len(results["success"]),
        "failed_count": len(results["failed"]),
    }