"""
============================================================================
Dagster Partitions - Tables ETL Dynamiques
============================================================================
"""

import sys
sys.path.insert(0, '/data/prefect/projects')

import psycopg2
from dagster import DynamicPartitionsDefinition
from typing import List
from shared.config import config


def get_active_tables() -> List[str]:
    """
    Récupère les tables actives depuis metadata.etl_tables
    
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
        return tables
        
    finally:
        cur.close()
        conn.close()


def get_table_load_mode(table_name: str) -> str:
    """
    Détermine le load_mode optimal pour une table
    
    Returns:
        'INCREMENTAL', 'FULL', ou 'FULL_RESET'
    """
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Checker metadata
        cur.execute("""
            SELECT 
                "PrimaryKeyCols",
                "ForceFull"
            FROM metadata.etl_tables
            WHERE COALESCE("ConfigName", "TableName") = %s
            LIMIT 1
        """, (table_name,))
        
        row = cur.fetchone()
        
        if not row:
            return "FULL"
        
        pk_cols, force_full = row
        
        if force_full:
            return "FULL"
        elif pk_cols and len(pk_cols) > 0:
            return "INCREMENTAL"
        else:
            return "FULL"
        
    finally:
        cur.close()
        conn.close()


# Partition dynamique pour toutes les tables
tables_partition = DynamicPartitionsDefinition(name="etl_tables")