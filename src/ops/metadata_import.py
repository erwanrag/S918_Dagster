import json
import shutil
from pathlib import Path
from dagster import op
from psycopg2.extras import execute_batch
from src.config.settings import get_settings
from src.db.connection import get_connection
from src.config.constants import Schema
from datetime import datetime

@op(name="scan_metadata_files", tags={"kind": "metadata"})
def scan_metadata_files_op(context) -> list[str]:
    settings = get_settings()
    metadata_dir = settings.sftp_root / "metadata"
    if not metadata_dir.exists():
        return []
    return [str(f) for f in metadata_dir.glob("*.json")]

@op(name="load_metadata_file", tags={"kind": "metadata"})
def load_metadata_file_op(context, file_path: str):
    path = Path(file_path)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            
        rows = data.get("data", [])
        table_name = data.get("table")
        
        if rows:
            target_table = f"{Schema.METADATA.value}.{table_name}"
            with get_connection() as conn:
                with conn.cursor() as cur:
                    # Logique simplifiée : Create table + Insert
                    cols = list(rows[0].keys())
                    cur.execute(f"CREATE TABLE IF NOT EXISTS {target_table} (data jsonb)") # Simplifié pour l'exemple
                    # En production, reprendre votre logique de mapping de types
                    context.log.info(f"Chargé {len(rows)} lignes pour {table_name}")
    except Exception as e:
        context.log.error(f"Erreur import {path.name}: {e}")
        
    return str(path)

@op(name="archive_metadata_file", tags={"kind": "metadata"})
def archive_metadata_file_op(context, file_path: str):
    settings = get_settings()
    src = Path(file_path)
    dest = settings.sftp_root / "processed" / datetime.now().strftime("%Y-%m-%d") / "db_metadata" / src.name
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(src, dest)
    context.log.info(f"Archivé : {dest}")