
"""
============================================================================
Assets Ingestion - SFTP → RAW → STAGING
============================================================================
"""
from dagster import AssetExecutionContext, Output, asset

from src.core.raw.loader import load_parquet_to_raw
from src.core.sftp.scanner import scan_parquet_files
from src.core.staging.transformer import create_staging_table, load_raw_to_staging
from src.db.monitoring import log_sftp_file
from src.utils.logging import get_logger

logger = get_logger(__name__)


@asset(name="sftp_discovered_files", group_name="ingestion")
def sftp_discovered_files(context: AssetExecutionContext) -> list[dict]:
    """Scanner les fichiers parquet dans SFTP"""
    files = scan_parquet_files()
    
    discovered = [
        {
            "path": str(f.path),
            "table_name": f.table_name,
            "load_mode": f.load_mode,
            "size_bytes": f.size_bytes,
        }
        for f in files
    ]
    
    context.log.info(f"Discovered {len(discovered)} files")
    
    return discovered


@asset(name="raw_tables_loaded", group_name="ingestion")
def raw_tables_loaded(
    context: AssetExecutionContext,
    sftp_discovered_files: list[dict],
) -> dict:
    """Charger fichiers parquet dans RAW"""
    from pathlib import Path
    
    results = []
    total_rows = 0
    
    for file_info in sftp_discovered_files:
        parquet_path = Path(file_info["path"])
        table_name = file_info["table_name"]
        load_mode = file_info["load_mode"]
        
        try:
            # Logger dans monitoring
            log_id = log_sftp_file(parquet_path, table_name, load_mode)
            
            # Charger dans RAW
            rows = load_parquet_to_raw(parquet_path, table_name, log_id)
            total_rows += rows
            
            results.append({"table": table_name, "rows": rows, "mode": load_mode})
            context.log.info(f"[OK] {table_name}: {rows:,} rows")
            
        except Exception as e:
            context.log.error(f"[ERROR] {table_name}: {e}")
            continue
    
    context.log.info(f"RAW Loading Complete: {len(results)} tables, {total_rows:,} rows")
    
    return {"tables_loaded": len(results), "total_rows": total_rows, "results": results}


@asset(name="staging_tables_ready", group_name="ingestion")
def staging_tables_ready(
    context: AssetExecutionContext,
    raw_tables_loaded: dict,
) -> dict:
    """Transformer RAW → STAGING"""
    from datetime import datetime
    
    run_id = f"dagster_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    results = []
    total_rows = 0
    
    for table_info in raw_tables_loaded["results"]:
        table_name = table_info["table"]
        load_mode = table_info["mode"]
        
        try:
            # Créer table STAGING
            create_staging_table(table_name, load_mode)
            
            # Charger RAW → STAGING
            rows = load_raw_to_staging(table_name, run_id, load_mode)
            total_rows += rows
            
            results.append({"table": table_name, "rows": rows, "mode": load_mode})
            context.log.info(f"[OK] {table_name}: {rows:,} rows")
            
        except Exception as e:
            context.log.error(f"[ERROR] {table_name}: {e}")
            continue
    
    context.log.info(f"STAGING Complete: {len(results)} tables, {total_rows:,} rows")
    
    return {
        "tables_processed": len(results),
        "total_rows": total_rows,
        "results": results,
        "run_id": run_id,
    }
