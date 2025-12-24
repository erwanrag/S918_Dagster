from datetime import datetime, timedelta
from dagster import op
from src.db.connection import get_connection
from src.config.constants import Schema

@op(name="cleanup_etl_logs", tags={"kind": "maintenance"})
def cleanup_etl_logs_op(context, retention_days: int = 30):
    """Supprime les logs ETL obsolètes"""
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Nettoyage générique (à adapter selon vos tables réelles de logs)
            # Ici on nettoie sftp_monitoring comme demandé
            cur.execute(f"""
                DELETE FROM {Schema.SFTP_MONITORING.value}.sftp_file_log
                WHERE detected_at < %s
            """, (cutoff_date,))
            deleted = cur.rowcount
            context.log.info(f"Nettoyage SFTP monitoring : {deleted} entrées supprimées")
            
    return deleted

@op(name="get_logs_stats", tags={"kind": "maintenance"})
def get_logs_stats_op(context):
    """Récupère les stats de taille des tables"""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT schemaname || '.' || relname, pg_size_pretty(pg_total_relation_size(schemaname||'.'||relname))
                FROM pg_stat_user_tables
                WHERE schemaname IN ('{Schema.ETL_LOGS.value}', '{Schema.SFTP_MONITORING.value}')
            """)
            stats = cur.fetchall()
            for table, size in stats:
                context.log.info(f"Table {table}: {size}")
    return stats