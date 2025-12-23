"""
============================================================================
Dagster Ops - Metadata Operations
============================================================================
Réutilise les fonctions existantes de ETL/utils/metadata_helper.py
"""

import sys
sys.path.insert(0, '/data/prefect/projects')

from dagster import op, In, Out
from typing import List, Dict, Any
import psycopg2
from shared.config import config

# Importer uniquement ce qu'on utilise vraiment
from ETL.utils.metadata_helper import get_table_columns


@op(
    name="get_table_metadata",
    out=Out(Dict[str, Any]),
    tags={"kind": "metadata"}
)
def get_table_metadata_op(context, table_name: str) -> Dict[str, Any]:
    """
    Récupère metadata complète d'une table
    
    Returns:
        dict: {
            'table_name': str,
            'config_name': str,
            'columns': List[dict],
            'primary_keys': List[str],
            'has_timestamps': bool,
            'force_full': bool
        }
    """
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Récupérer depuis metadata.etl_tables
        cur.execute("""
            SELECT 
                "TableName",
                "ConfigName",
                "PrimaryKeyCols",
                "HasTimestamps",
                "ForceFull",
                "Description"
            FROM metadata.etl_tables
            WHERE COALESCE("ConfigName", "TableName") = %s
            LIMIT 1
        """, (table_name,))
        
        row = cur.fetchone()
        
        if not row:
            context.log.error(f"Table {table_name} not found in metadata")
            return None
        
        table_name_db = row[0]
        config_name = row[1]
        primary_keys = row[2] if row[2] else []
        has_timestamps = row[3]
        force_full = row[4]
        description = row[5]
        
        # Récupérer colonnes
        columns = get_table_columns(table_name_db)
        
        metadata = {
            'table_name': table_name_db,
            'config_name': config_name,
            'physical_name': config_name or table_name_db,
            'columns': columns,
            'primary_keys': primary_keys,
            'has_timestamps': has_timestamps,
            'force_full': force_full,
            'description': description or ''
        }
        
        context.log.info(f"Metadata loaded: {metadata['physical_name']}")
        
        return metadata
        
    finally:
        cur.close()
        conn.close()


@op(
    name="list_active_tables",
    out=Out(List[str]),
    tags={"kind": "metadata"}
)
def list_active_tables_op(context) -> List[str]:
    """
    Liste toutes les tables actives depuis metadata.etl_tables
    
    Returns:
        List[str]: Physical names (ConfigName ou TableName)
    """
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        cur.execute("""
            SELECT COALESCE("ConfigName", "TableName") as physical_name
            FROM metadata.etl_tables
            WHERE "IsActive" = TRUE
            ORDER BY "TableName"
        """)
        
        tables = [row[0] for row in cur.fetchall()]
        
        context.log.info(f"Found {len(tables)} active tables")
        
        return tables
        
    finally:
        cur.close()
        conn.close()


@op(
    name="determine_load_mode",
    ins={"table_metadata": In(Dict[str, Any])},
    out=Out(str),
    tags={"kind": "metadata"}
)
def determine_load_mode_op(context, table_metadata: Dict[str, Any]) -> str:
    """
    Détermine le load_mode optimal pour une table
    
    Priorité :
    1. Lire depuis sftp_monitoring (dernier fichier)
    2. Si force_full → FULL
    3. Si primary_keys → INCREMENTAL
    4. Sinon → FULL
    
    Returns:
        'INCREMENTAL', 'FULL', ou 'FULL_RESET'
    """
    table_name = table_metadata['physical_name']
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # 1. Checker sftp_monitoring pour le dernier fichier
        cur.execute("""
            SELECT load_mode
            FROM sftp_monitoring.sftp_file_log
            WHERE table_name = %s
              AND processing_status IN ('PENDING', 'COMPLETED')
              AND load_mode IS NOT NULL
              AND load_mode != ''
            ORDER BY detected_at DESC
            LIMIT 1
        """, (table_name,))
        
        result = cur.fetchone()
        
        if result and result[0]:
            load_mode = result[0]
            context.log.info(f"Load mode from sftp_monitoring: {load_mode}")
            return load_mode
        
        # 2. Sinon, détection automatique
        if table_metadata.get('force_full'):
            load_mode = "FULL"
        elif table_metadata.get('primary_keys') and len(table_metadata['primary_keys']) > 0:
            load_mode = "INCREMENTAL"
        else:
            load_mode = "FULL"
        
        context.log.info(f"Auto-detected load mode: {load_mode}")
        
        return load_mode
        
    finally:
        cur.close()
        conn.close()