"""
============================================================================
Ops Maintenance - Nettoyage et statistiques
============================================================================
"""

from datetime import datetime, timedelta
from dagster import op, Out
import psycopg2

from src.config.settings import get_settings


@op(
    name="cleanup_etl_logs",
    out=Out(dict),
    tags={"kind": "maintenance"},
    description="Supprime les logs ETL obsolètes"
)
def cleanup_etl_logs_op(context) -> dict:
    """
    Supprime les logs ETL de plus de X jours (configuré via settings).
    
    Utilise les tables existantes:
    - etl_logs.etl_run_log (colonne: started_at)
    - etl_logs.load_step_log (colonne: created_at)
    
    Returns:
        dict: Nombre d'enregistrements supprimés par table
    """
    settings = get_settings()
    retention_days = settings.etl_logs_retention_days
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    
    conn = psycopg2.connect(settings.postgres_url)
    
    try:
        with conn.cursor() as cur:
            # Nettoyer etl_logs.etl_run_log (anciens runs)
            cur.execute("""
                DELETE FROM etl_logs.etl_run_log
                WHERE started_at < %s
            """, (cutoff_date,))
            deleted_runs = cur.rowcount
            
            # Nettoyer etl_logs.load_step_log (anciens steps)
            cur.execute("""
                DELETE FROM etl_logs.load_step_log
                WHERE created_at < %s
            """, (cutoff_date,))
            deleted_steps = cur.rowcount
            
            conn.commit()
            
            context.log.info(
                f"Cleanup ETL logs completed: "
                f"{deleted_runs} runs, {deleted_steps} steps deleted "
                f"(cutoff: {cutoff_date.isoformat()})"
            )
            
            return {
                "deleted_runs": deleted_runs,
                "deleted_steps": deleted_steps,
                "cutoff_date": cutoff_date.isoformat(),
                "retention_days": retention_days
            }
    
    finally:
        conn.close()


@op(
    name="cleanup_sftp_monitoring",
    out=Out(dict),
    tags={"kind": "maintenance"},
    description="Supprime les anciens enregistrements SFTP monitoring"
)
def cleanup_sftp_monitoring_op(context, start_after) -> dict:
    """
    Nettoie les enregistrements SFTP monitoring traités.
    
    Utilise la table: sftp_monitoring.sftp_file_log
    
    Args:
        start_after: Dépendance pour forcer l'ordre d'exécution
        
    Returns:
        dict: Nombre d'enregistrements supprimés
    """
    settings = get_settings()
    retention_days = settings.sftp_monitoring_retention_days
    cutoff_date = datetime.now() - timedelta(days=retention_days)
    
    conn = psycopg2.connect(settings.postgres_url)
    
    try:
        with conn.cursor() as cur:
            # Vérifier si la table existe
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'sftp_monitoring' 
                    AND table_name = 'sftp_file_log'
                )
            """)
            
            table_exists = cur.fetchone()[0]
            
            if not table_exists:
                context.log.warning("Table sftp_monitoring.sftp_file_log does not exist, skipping cleanup")
                return {
                    "deleted_records": 0,
                    "cutoff_date": cutoff_date.isoformat(),
                    "retention_days": retention_days,
                    "skipped": True
                }
            
            # Cleanup (adapter selon la structure réelle de ta table)
            cur.execute("""
                DELETE FROM sftp_monitoring.sftp_file_log
                WHERE detected_at < %s
                AND processing_status = 'COMPLETED'
            """, (cutoff_date,))
            deleted = cur.rowcount

            conn.commit()

            context.log.info(
                f"Cleanup SFTP monitoring: {deleted} records deleted "
                f"(cutoff: {cutoff_date.isoformat()})"
            )
            
            return {
                "deleted_records": deleted,
                "cutoff_date": cutoff_date.isoformat(),
                "retention_days": retention_days
            }
    
    except Exception as e:
        context.log.warning(f"SFTP cleanup error: {e}")
        return {
            "deleted_records": 0,
            "error": str(e)
        }
    
    finally:
        conn.close()


@op(
    name="vacuum_analyze_all",
    out=Out(dict),
    tags={"kind": "maintenance"},
    description="VACUUM ANALYZE sur tous les schémas ETL"
)
def vacuum_analyze_all_op(context) -> dict:
    """
    Optimise toutes les tables avec VACUUM ANALYZE.
    
    Returns:
        dict: Liste des schémas traités
    """
    settings = get_settings()
    schemas = ["raw", "staging", "ods", "prep", "services", "etl_logs"]
    
    conn = psycopg2.connect(settings.postgres_url)
    
    try:
        conn.autocommit = True  # VACUUM ne peut pas être dans transaction
        
        with conn.cursor() as cur:
            for schema in schemas:
                context.log.info(f"Running VACUUM ANALYZE on schema: {schema}")
                
                # VACUUM ANALYZE toutes les tables du schéma
                cur.execute(f"""
                    SELECT tablename 
                    FROM pg_tables 
                    WHERE schemaname = %s
                """, (schema,))
                
                tables = [row[0] for row in cur.fetchall()]
                
                for table in tables:
                    try:
                        cur.execute(f'VACUUM ANALYZE {schema}."{table}"')
                        context.log.debug(f"  ✓ {schema}.{table}")
                    except Exception as e:
                        context.log.warning(f"  ✗ {schema}.{table}: {e}")
                
                context.log.info(f"Completed schema {schema}: {len(tables)} tables")
        
        return {
            "schemas_vacuumed": schemas,
            "timestamp": datetime.now().isoformat()
        }
    
    finally:
        conn.close()


@op(
    name="get_logs_stats",
    out=Out(dict),
    tags={"kind": "maintenance"},
    description="Récupère statistiques volumétrie logs"
)
def get_logs_stats_op(context) -> dict:
    """
    Récupère les statistiques de volumétrie des logs.
    
    Utilise les tables existantes:
    - etl_logs.etl_run_log
    - etl_logs.load_step_log
    
    Returns:
        dict: Statistiques par table
    """
    settings = get_settings()
    conn = psycopg2.connect(settings.postgres_url)
    
    try:
        with conn.cursor() as cur:
            # Total runs
            cur.execute("SELECT COUNT(*) FROM etl_logs.etl_run_log")
            total_runs = cur.fetchone()[0]
            
            # Runs récents (30j)
            cur.execute("""
                SELECT COUNT(*)
                FROM etl_logs.etl_run_log
                WHERE started_at > CURRENT_DATE - INTERVAL '30 days'
            """)
            recent_runs = cur.fetchone()[0]
            
            # Total steps
            cur.execute("SELECT COUNT(*) FROM etl_logs.load_step_log")
            total_steps = cur.fetchone()[0]
            
            # Steps récents (30j)
            cur.execute("""
                SELECT COUNT(*)
                FROM etl_logs.load_step_log
                WHERE created_at > CURRENT_DATE - INTERVAL '30 days'
            """)
            recent_steps = cur.fetchone()[0]
            
            # Taille tables logs
            cur.execute("""
                SELECT 
                    schemaname || '.' || tablename as table_name,
                    pg_size_pretty(pg_total_relation_size(
                        schemaname || '.' || tablename
                    )) as size
                FROM pg_tables
                WHERE schemaname IN ('etl_logs', 'sftp_monitoring')
                ORDER BY pg_total_relation_size(
                    schemaname || '.' || tablename
                ) DESC
            """)
            table_sizes = {row[0]: row[1] for row in cur.fetchall()}
            
            stats = {
                "total_runs": total_runs,
                "recent_runs_30d": recent_runs,
                "total_steps": total_steps,
                "recent_steps_30d": recent_steps,
                "table_sizes": table_sizes,
                "timestamp": datetime.now().isoformat()
            }
            
            context.log.info(
                f"Logs statistics: "
                f"total_runs={total_runs}, recent_runs={recent_runs}, "
                f"total_steps={total_steps}, recent_steps={recent_steps}"
            )
            
            return stats
    
    finally:
        conn.close()


@op(
    name="reindex_all_tables",
    out=Out(dict),
    tags={"kind": "maintenance"},
    description="REINDEX toutes les tables pour optimiser les index"
)
def reindex_all_tables_op(context, start_after) -> dict:
    """
    Reconstruit tous les index pour optimisation.
    
    ⚠️ Opération lourde - à exécuter hors heures de production
    
    Args:
        start_after: Dépendance pour forcer l'ordre d'exécution
    
    Returns:
        dict: Schémas réindexés
    """
    settings = get_settings()
    schemas = ["raw", "staging", "ods", "prep", "services", "etl_logs"]
    
    conn = psycopg2.connect(settings.postgres_url)
    
    try:
        conn.autocommit = True
        
        with conn.cursor() as cur:
            for schema in schemas:
                context.log.info(f"Reindexing schema: {schema}")
                
                try:
                    cur.execute(f"REINDEX SCHEMA {schema}")
                    context.log.info(f"  ✓ {schema} reindexed")
                except Exception as e:
                    context.log.warning(f"  ✗ {schema} failed: {e}")
        
        return {
            "schemas_reindexed": schemas,
            "timestamp": datetime.now().isoformat()
        }
    
    finally:
        conn.close()