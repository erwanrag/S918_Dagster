"""
============================================================================
Assets RAW - Chargement SFTP → RAW (VERSION DEBUG)
============================================================================
"""

from pathlib import Path
from typing import Tuple, Optional

from dagster import AssetExecutionContext, asset, MetadataValue

from src.core.raw.loader import load_parquet_to_raw
from src.db.monitoring import log_sftp_file
from src.utils.parallel_utils import process_files_by_size_strategy


@asset(
    name="raw_sftp_tables",
    group_name="raw",
    required_resource_keys={"postgres"},
    description="""
    Tables RAW issues du SFTP.

    • Données brutes
    • 1 fichier = 1 table RAW
    • Aucune logique métier
    """,
)
def raw_sftp_tables(
    context: AssetExecutionContext,
    sftp_parquet_inventory: list[dict],
) -> dict:
    """
    Charge les fichiers Parquet dans les tables RAW.
    """
    context.log.info(f"[DEBUG] Starting raw_sftp_tables with {len(sftp_parquet_inventory)} files")
    
    # Cas 0 fichiers
    if not sftp_parquet_inventory:
        context.log.info("No files to process, returning empty result")
        return {"results": [], "total_rows": 0}
    
    context.log.info(f"[DEBUG] Building file_paths and file_metadata")
    file_paths = [Path(f["path"]) for f in sftp_parquet_inventory]
    file_metadata = {Path(f["path"]): f for f in sftp_parquet_inventory}
    
    context.log.info(f"[DEBUG] file_paths: {[str(p) for p in file_paths]}")

    def process_single_file(
        parquet_path: Path,
    ) -> Tuple[str, str, bool, Optional[str]]:
        """Traite un seul fichier parquet"""
        context.log.info(f"[DEBUG] Processing file: {parquet_path}")
        
        metadata = file_metadata[parquet_path]
        table_name = metadata["table_name"]
        load_mode = metadata["load_mode"]
        
        context.log.info(f"[DEBUG] Table: {table_name}, Mode: {load_mode}")

        try:
            # ÉTAPE 1: Log monitoring
            context.log.info(f"[DEBUG] Logging SFTP file in monitoring")
            with context.resources.postgres.get_connection() as conn:
                log_id = log_sftp_file(
                    conn=conn,
                    file_path=parquet_path,
                    table_name=table_name,
                    load_mode=load_mode,
                )
                context.log.info(f"[DEBUG] Log ID: {log_id}")
            
            # ÉTAPE 2: Load parquet (loader crée sa propre connexion)
            context.log.info(f"[DEBUG] Loading parquet to RAW table")
            rows = load_parquet_to_raw(
                parquet_path=parquet_path,
                table_name=table_name,
                log_id=log_id,
                conn=None,  # Loader crée sa propre connexion
            )
            context.log.info(f"[DEBUG] Loaded {rows} rows")

            context.log.info(f"RAW load completed: {table_name} ({rows} rows)")
            return (table_name, load_mode, True, None)

        except Exception as e:
            context.log.error(f"RAW load failed: {table_name} - {str(e)}")
            return (table_name, load_mode, False, str(e))

    # Traiter les fichiers
    context.log.info(f"[DEBUG] Calling process_files_by_size_strategy")
    results = process_files_by_size_strategy(file_paths, process_single_file)
    context.log.info(f"[DEBUG] process_files_by_size_strategy returned")

    # Calculer total rows
    total_rows = sum(
        file_metadata[Path(r["file"])]["size_bytes"] // 1000
        for r in results["success"]
    )
    
    # Formatter les résultats
    formatted_results = [
        {
            "table": r["table"],
            "mode": r["mode"],
            "rows": file_metadata[Path(r["file"])]["size_bytes"] // 1000,
        }
        for r in results["success"]
    ]

    context.log.info(f"[DEBUG] Formatted results: {formatted_results}")

    # Ajouter métadonnées Dagster
    context.add_output_metadata({
        "files_total": results["total"],
        "files_success": len(results["success"]),
        "files_failed": len(results["failed"]),
        "total_rows": total_rows,
    })

    context.log.info(f"[DEBUG] Returning final result")
    return {
        "results": formatted_results,
        "total_rows": total_rows,
    }