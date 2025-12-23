"""
============================================================================
Dagster Ops - STAGING to ODS Operations
============================================================================
Réutilise ETL/tasks/ods_tasks.py avec support FULL_RESET
"""

import sys
sys.path.insert(0, '/data/prefect/projects')

from dagster import op, In, Out
from typing import Dict, Any

from shared.config import config
from ETL.tasks.ods_tasks import (
    merge_ods_auto as prefect_merge_ods_auto,
    verify_ods_after_merge as prefect_verify_ods_after_merge
)


@op(
    name="merge_staging_to_ods",
    ins={
        "table_name": In(str),
        "load_mode": In(str),
        "run_id": In(str)
    },
    out=Out(Dict[str, Any]),
    tags={"kind": "ods"}
)
def merge_staging_to_ods_op(
    context,
    table_name: str,
    load_mode: str,
    run_id: str
) -> Dict[str, Any]:
    """
    Merge STAGING → ODS avec détection automatique du mode
    
    Modes supportés:
    - FULL: TRUNCATE + INSERT
    - INCREMENTAL: UPSERT (INSERT nouveaux + UPDATE modifiés)
    - FULL_RESET: DROP + CREATE + INSERT (réinitialisation complète)
    
    Priorité:
    1. load_mode passé en paramètre
    2. load_mode depuis sftp_monitoring
    3. Détection auto (PK → INCREMENTAL, sinon FULL)
    
    Returns:
        dict: {
            'mode': str,
            'rows_inserted': int,
            'rows_updated': int,
            'rows_affected': int,
            'table': str
        }
    """
    context.log.info(f"Merging STAGING to ODS: {table_name} (mode: {load_mode})")
    
    try:
        # Appeler la fonction Prefect existante
        result = prefect_merge_ods_auto.fn(table_name, run_id, load_mode)
        
        rows_affected = result.get('rows_affected', result.get('rows_inserted', 0))
        actual_mode = result.get('mode', load_mode)
        
        context.log.info(
            f"ODS merge completed: {rows_affected:,} rows ({actual_mode})"
        )
        
        return {
            'mode': actual_mode,
            'rows_inserted': result.get('rows_inserted', 0),
            'rows_updated': result.get('rows_updated', 0),
            'rows_affected': rows_affected,
            'table': f"ods.{table_name.lower()}",
            'run_id': run_id
        }
        
    except Exception as e:
        context.log.error(f"Error merging to ODS: {e}")
        raise


@op(
    name="verify_ods_table",
    ins={"table_name": In(str), "run_id": In(str)},
    out=Out(Dict[str, Any]),
    tags={"kind": "ods"}
)
def verify_ods_table_op(context, table_name: str, run_id: str) -> Dict[str, Any]:
    """
    Vérifier table ODS après merge
    
    Returns:
        dict: {
            'exists': bool,
            'total_rows': int
        }
    """
    context.log.info(f"Verifying ODS table: {table_name}")
    
    try:
        # Appeler la fonction Prefect existante
        result = prefect_verify_ods_after_merge.fn(table_name, run_id)
        
        context.log.info(
            f"ODS verification: {result.get('total_rows', 0):,} rows"
        )
        
        return result
        
    except Exception as e:
        context.log.error(f"Error verifying ODS: {e}")
        raise