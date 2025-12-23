"""
============================================================================
Dagster Ops - RAW to STAGING Operations
============================================================================
Réutilise ETL/tasks/staging_tasks.py
"""

import sys
sys.path.insert(0, '/data/prefect/projects')

from dagster import op, In, Out
from typing import Dict, Any
import psycopg2
from datetime import datetime

from shared.config import config
from ETL.tasks.staging_tasks import (
    create_staging_table as prefect_create_staging_table,
    load_raw_to_staging as prefect_load_raw_to_staging
)


@op(
    name="create_staging_table",
    ins={"table_name": In(str), "load_mode": In(str)},
    out=Out(str),
    tags={"kind": "staging"}
)
def create_staging_table_op(context, table_name: str, load_mode: str) -> str:
    """
    Créer table STAGING avec structure metadata
    
    Réutilise la fonction Prefect existante
    
    Returns:
        str: Nom de la table staging créée
    """
    context.log.info(f"Creating staging table for {table_name} (mode: {load_mode})")
    
    # Appeler la fonction Prefect existante
    # Note: La fonction Prefect utilise @task, on appelle directement sa logique
    try:
        prefect_create_staging_table.fn(table_name, load_mode)
        
        stg_table = f"staging_etl.stg_{table_name.lower()}"
        context.log.info(f"Staging table created: {stg_table}")
        
        return stg_table
        
    except Exception as e:
        context.log.error(f"Error creating staging table: {e}")
        raise


@op(
    name="load_raw_to_staging",
    ins={
        "table_name": In(str),
        "load_mode": In(str),
        "run_id": In(str)
    },
    out=Out(Dict[str, Any]),
    tags={"kind": "staging"}
)
def load_raw_to_staging_op(
    context,
    table_name: str,
    load_mode: str,
    run_id: str
) -> Dict[str, Any]:
    """
    Charger données RAW → STAGING avec transformations
    
    Features:
    - Éclatement colonnes EXTENT
    - Calcul _etl_hashdiff
    - Support FULL/INCREMENTAL/FULL_RESET
    - Déduplication
    
    Returns:
        dict: {
            'rows_loaded': int,
            'load_mode': str,
            'table': str
        }
    """
    context.log.info(f"Loading RAW to STAGING: {table_name} ({load_mode})")
    
    try:
        # Appeler la fonction Prefect existante
        rows_loaded = prefect_load_raw_to_staging.fn(table_name, run_id, load_mode)
        
        context.log.info(f"Loaded {rows_loaded:,} rows to STAGING")
        
        return {
            'rows_loaded': rows_loaded,
            'load_mode': load_mode,
            'table': f"staging_etl.stg_{table_name.lower()}",
            'run_id': run_id
        }
        
    except Exception as e:
        context.log.error(f"Error loading to staging: {e}")
        raise


@op(
    name="verify_staging_table",
    ins={"table_name": In(str)},
    out=Out(Dict[str, Any]),
    tags={"kind": "staging"}
)
def verify_staging_table_op(context, table_name: str) -> Dict[str, Any]:
    """
    Vérifier que la table STAGING contient des données
    
    Returns:
        dict: {
            'exists': bool,
            'row_count': int,
            'has_data': bool
        }
    """
    stg_table = f"staging_etl.stg_{table_name.lower()}"
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Vérifier existence
        cur.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'staging_etl'
                AND table_name = %s
            )
        """, (f"stg_{table_name.lower()}",))
        
        exists = cur.fetchone()[0]
        
        if not exists:
            context.log.warning(f"Table {stg_table} does not exist")
            return {
                'exists': False,
                'row_count': 0,
                'has_data': False
            }
        
        # Compter lignes
        cur.execute(f"SELECT COUNT(*) FROM {stg_table}")
        row_count = cur.fetchone()[0]
        
        context.log.info(f"Staging table {stg_table}: {row_count:,} rows")
        
        return {
            'exists': True,
            'row_count': row_count,
            'has_data': row_count > 0
        }
        
    finally:
        cur.close()
        conn.close()