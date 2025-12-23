"""
============================================================================
Dagster Ops - SFTP to RAW Operations
============================================================================
Réutilise les tasks existantes de ETL/flows/ingestion/sftp_to_raw.py
"""

import sys
sys.path.insert(0, '/data/prefect/projects')

from dagster import op, In, Out
from typing import Dict, Any, Optional
from pathlib import Path
import json
import psycopg2
from datetime import datetime

from shared.config import config, sftp_config


@op(
    name="scan_sftp_files",
    out=Out(list),
    tags={"kind": "sftp"}
)
def scan_sftp_files_op(context) -> list:
    """
    Scanner les fichiers parquet dans le répertoire SFTP
    
    Returns:
        List[Path]: Liste des fichiers .parquet trouvés
    """
    parquet_dir = sftp_config.sftp_parquet_dir
    
    if not parquet_dir.exists():
        context.log.warning(f"SFTP directory not found: {parquet_dir}")
        return []
    
    parquet_files = list(parquet_dir.glob("*.parquet"))
    
    context.log.info(f"Found {len(parquet_files)} parquet files")
    
    return parquet_files


@op(
    name="parse_filename",
    ins={"parquet_path": In(Path)},
    out=Out(Dict[str, Any]),
    tags={"kind": "sftp"}
)
def parse_filename_op(context, parquet_path: Path) -> Dict[str, Any]:
    """
    Parser le nom de fichier pour extraire metadata
    
    Format attendu: table_YYYYMMDD_MODE.parquet
    Exemple: article_20250101_FULL.parquet
    
    Returns:
        dict: {
            'table_name': str,
            'date': str,
            'load_mode': str,
            'filename': str
        }
    """
    filename = parquet_path.stem  # Sans .parquet
    parts = filename.split('_')
    
    if len(parts) < 3:
        context.log.warning(f"Invalid filename format: {filename}")
        return None
    
    # Dernière partie = MODE
    load_mode = parts[-1].upper()
    
    # Avant-dernière partie = DATE
    date_str = parts[-2]
    
    # Reste = TABLE_NAME
    table_name = '_'.join(parts[:-2])
    
    parsed = {
        'table_name': table_name,
        'date': date_str,
        'load_mode': load_mode,
        'filename': parquet_path.name,
        'filepath': str(parquet_path)
    }
    
    context.log.info(f"Parsed: {table_name} ({load_mode})")
    
    return parsed


@op(
    name="read_metadata_json",
    ins={"parquet_path": In(Path)},
    out=Out(Optional[dict]),
    tags={"kind": "sftp"}
)
def read_metadata_json_op(context, parquet_path: Path) -> Optional[dict]:
    """
    Lire le fichier metadata.json associé
    
    Returns:
        dict ou None si absent
    """
    metadata_path = parquet_path.with_suffix('.metadata.json')
    
    if not metadata_path.exists():
        context.log.warning(f"No metadata.json for {parquet_path.name}")
        return None
    
    try:
        with open(metadata_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
        
        context.log.info(f"Metadata loaded: {metadata.get('table_name')}")
        return metadata
        
    except Exception as e:
        context.log.error(f"Error reading metadata.json: {e}")
        return None


@op(
    name="log_file_to_monitoring",
    ins={
        "parquet_path": In(Path),
        "metadata": In(Optional[dict]),
        "parsed": In(Dict[str, Any])
    },
    out=Out(int),
    tags={"kind": "sftp"}
)
def log_file_to_monitoring_op(
    context,
    parquet_path: Path,
    metadata: Optional[dict],
    parsed: Dict[str, Any]
) -> int:
    """
    Logger le fichier dans sftp_monitoring.sftp_file_log
    
    Returns:
        int: log_id
    """
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Extraire infos
        table_name = parsed['table_name']
        load_mode = parsed['load_mode']
        file_date = parsed['date']
        
        # Taille fichier
        file_size = parquet_path.stat().st_size
        
        # INSERT dans monitoring
        cur.execute("""
            INSERT INTO sftp_monitoring.sftp_file_log (
                filename,
                table_name,
                load_mode,
                file_date,
                file_size_bytes,
                detected_at,
                processing_status
            ) VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP, 'PENDING')
            RETURNING log_id
        """, (
            parquet_path.name,
            table_name,
            load_mode,
            file_date,
            file_size
        ))
        
        log_id = cur.fetchone()[0]
        conn.commit()
        
        context.log.info(f"Logged to monitoring: log_id={log_id}")
        
        return log_id
        
    finally:
        cur.close()
        conn.close()


@op(
    name="load_parquet_to_raw",
    ins={
        "parquet_path": In(Path),
        "log_id": In(int),
        "metadata": In(Optional[dict])
    },
    out=Out(Dict[str, Any]),
    tags={"kind": "raw"}
)
def load_parquet_to_raw_op(
    context,
    parquet_path: Path,
    log_id: int,
    metadata: Optional[dict]
) -> Dict[str, Any]:
    """
    Charger le fichier parquet dans RAW schema
    
    Réutilise la logique de ETL/flows/ingestion/sftp_to_raw.py
    
    Returns:
        dict: {
            'table_name': str,
            'rows_loaded': int,
            'load_mode': str
        }
    """
    import pyarrow.parquet as pq
    
    # Déterminer table name
    if metadata:
        table_name = metadata.get('config_name') or metadata.get('table_name')
        load_mode = metadata.get('load_mode', 'FULL')
    else:
        # Fallback sur filename parsing
        filename = parquet_path.stem
        parts = filename.split('_')
        table_name = '_'.join(parts[:-2]) if len(parts) >= 3 else filename
        load_mode = parts[-1] if len(parts) >= 3 else 'FULL'
    
    raw_table = f"raw.raw_{table_name.lower()}"
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Lire parquet
        table = pq.read_table(parquet_path)
        df = table.to_pandas()
        
        rows_count = len(df)
        
        context.log.info(f"Loaded {rows_count:,} rows from parquet")
        
        # Créer schéma RAW si n'existe pas
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.commit()
        
        # DROP/CREATE table RAW
        cur.execute(f"DROP TABLE IF EXISTS {raw_table} CASCADE")
        conn.commit()
        
        # Utiliser pandas to_sql pour créer + insérer
        from sqlalchemy import create_engine
        engine = create_engine(config.get_connection_string())
        
        df.to_sql(
            f"raw_{table_name.lower()}",
            engine,
            schema='raw',
            if_exists='replace',
            index=False,
            method='multi',
            chunksize=10000
        )
        
        context.log.info(f"Loaded {rows_count:,} rows into {raw_table}")
        
        # Update monitoring
        cur.execute("""
            UPDATE sftp_monitoring.sftp_file_log
            SET 
                processing_status = 'COMPLETED',
                rows_loaded = %s,
                completed_at = CURRENT_TIMESTAMP
            WHERE log_id = %s
        """, (rows_count, log_id))
        conn.commit()
        
        return {
            'table_name': table_name,
            'rows_loaded': rows_count,
            'load_mode': load_mode,
            'log_id': log_id
        }
        
    except Exception as e:
        context.log.error(f"Error loading parquet: {e}")
        
        # Update monitoring avec erreur
        cur.execute("""
            UPDATE sftp_monitoring.sftp_file_log
            SET 
                processing_status = 'FAILED',
                error_message = %s
            WHERE log_id = %s
        """, (str(e), log_id))
        conn.commit()
        
        raise
        
    finally:
        cur.close()
        conn.close()