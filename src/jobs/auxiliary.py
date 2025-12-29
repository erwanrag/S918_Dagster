"""
============================================================================
Auxiliary Jobs - Maintenance et imports
============================================================================
"""

from dagster import (
    job,
    ScheduleDefinition,
    DefaultScheduleStatus,
)

from src.ops.maintenance import (
    cleanup_etl_logs_op,
    cleanup_sftp_monitoring_op,
    vacuum_analyze_all_op,
    get_logs_stats_op,
    reindex_all_tables_op,
)
from src.ops.metadata_import import (
    scan_metadata_files_op,
    load_metadata_file_op,
    archive_metadata_file_op,
)


# =============================================================================
# Maintenance Cleanup Job (mensuel)
# =============================================================================

@job(name="maintenance_cleanup_job")
def maintenance_cleanup_job():
    """
    Job de maintenance léger : nettoyage logs ETL et SFTP.
    Exécuté mensuellement (1er du mois à 4h).
    """
    # Stats avant cleanup
    get_logs_stats_op()
    
    # Nettoyage séquentiel (enchaîné automatiquement)
    cleanup_etl = cleanup_etl_logs_op()
    cleanup_sftp_monitoring_op(cleanup_etl)


maintenance_schedule = ScheduleDefinition(
    job=maintenance_cleanup_job,
    cron_schedule="0 4 1 * *",  # 1er du mois à 4h
    default_status=DefaultScheduleStatus.RUNNING,
    execution_timezone="Europe/Paris",
    description="Maintenance mensuelle : cleanup logs",
)


# =============================================================================
# Heavy Maintenance Job (trimestriel)
# =============================================================================

@job(name="maintenance_heavy_job")
def maintenance_heavy_job():
    """
    Job de maintenance lourd : VACUUM, ANALYZE, REINDEX.
    Exécuté trimestriellement (1er du trimestre à 3h).
    """
    # Les 2 ops s'exécutent de manière indépendante (pas de dépendance)
    vacuum_analyze_all_op()
    reindex_all_tables_op()


heavy_maintenance_schedule = ScheduleDefinition(
    job=maintenance_heavy_job,
    cron_schedule="0 3 1 */3 *",  # 1er janv/avr/juil/oct à 3h
    default_status=DefaultScheduleStatus.STOPPED,
    execution_timezone="Europe/Paris",
    description="Maintenance trimestrielle : VACUUM, REINDEX",
)


# =============================================================================
# Metadata Import Job (quotidien à 8h)
# =============================================================================

@job(name="metadata_import_job")
def metadata_import_job():
    """
    Job d'import des fichiers metadata SFTP.
    Traite tous les fichiers trouvés séquentiellement.
    Exécuté tous les jours à 8h.
    """
    files_info = scan_metadata_files_op()
    loaded = load_metadata_file_op(files_info)
    archive_metadata_file_op(loaded)


metadata_import_schedule = ScheduleDefinition(
    job=metadata_import_job,
    cron_schedule="0 8 * * *",  # Tous les jours à 8h
    default_status=DefaultScheduleStatus.RUNNING,
    execution_timezone="Europe/Paris",
    description="Import metadata Progress quotidien à 8h",
)


# =============================================================================
# Export
# =============================================================================

__all__ = [
    "maintenance_cleanup_job",
    "maintenance_heavy_job",
    "metadata_import_job",
    "maintenance_schedule",
    "heavy_maintenance_schedule",
    "metadata_import_schedule",  # ← Ajouté
]