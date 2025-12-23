"""
============================================================================
ETL Schedules - Dagster
============================================================================
"""

from dagster import ScheduleDefinition, DefaultScheduleStatus


etl_hourly_schedule = ScheduleDefinition(
    name="etl_hourly",
    cron_schedule="0 * * * *",
    # Pour l'instant pas de job défini, on le fera plus tard
    # job_name="etl_pipeline_job",
    execution_timezone="Europe/Paris",
    default_status=DefaultScheduleStatus.STOPPED,
    description="Pipeline ETL complet - Exécution horaire"
)
