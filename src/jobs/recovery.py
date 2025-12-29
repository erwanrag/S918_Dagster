"""Ops de recovery pour situations d'erreur"""
from dagster import op, In, Out, Failure
from datetime import datetime

@op(
    name="replay_staging_to_ods",
    ins={"table_name": In(str)},
    out=Out(dict),
    required_resource_keys={"postgres"},
    description="Recharger ODS depuis STAGING pour une table"
)
def replay_staging_to_ods_op(context, table_name: str) -> dict:
    """Recovery: Recharge ODS depuis STAGING"""
    from src.core.ods.merger import merge_staging_to_ods
    
    run_id = f"recovery_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    with context.resources.postgres.get_connection() as conn:
        # Vérifier présence données STAGING
        cur = conn.cursor()
        cur.execute(f"""
            SELECT COUNT(*) FROM staging.{table_name}
            WHERE _etl_is_current = true
        """)
        staging_count = cur.fetchone()[0]
        
        if staging_count == 0:
            raise Failure(f"No current data in staging.{table_name}")
        
        # Recharger ODS
        rows = merge_staging_to_ods(
            table_name=table_name,
            run_id=run_id,
            load_mode="FULL",  # Force FULL pour recovery
            conn=conn
        )
        
        context.log.info(
            f"Recovery completed: {table_name}",
            staging_rows=staging_count,
            ods_rows=rows
        )
        
        return {
            "table": table_name,
            "staging_rows": staging_count,
            "ods_rows": rows,
            "run_id": run_id
        }


@op(
    name="validate_ods_consistency",
    ins={"table_name": In(str)},
    out=Out(bool),
    required_resource_keys={"postgres"},
    description="Valider cohérence ODS vs STAGING"
)
def validate_ods_consistency_op(context, table_name: str) -> bool:
    """Valide que ODS est cohérent avec STAGING"""
    with context.resources.postgres.get_connection() as conn:
        cur = conn.cursor()
        
        # Compter current dans STAGING
        cur.execute(f"""
            SELECT COUNT(*)
            FROM staging.{table_name}
            WHERE _etl_is_current = true
        """)
        staging_count = cur.fetchone()[0]
        
        # Compter current dans ODS
        cur.execute(f"""
            SELECT COUNT(*)
            FROM ods.{table_name}
            WHERE _etl_is_current = true
            AND _etl_is_deleted = false
        """)
        ods_count = cur.fetchone()[0]
        
        is_consistent = (staging_count == ods_count)
        
        context.log.info(
            f"Consistency check: {table_name}",
            staging=staging_count,
            ods=ods_count,
            consistent=is_consistent
        )
        
        return is_consistent