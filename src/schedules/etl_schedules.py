"""Schedules ETL"""
from dagster import ScheduleDefinition, DefaultScheduleStatus
from src.jobs.pipelines import (
    full_etl_pipeline,
    services_pipeline,
)

# =============================================================================
# Pipeline ETL Production
# =============================================================================

production_schedule = ScheduleDefinition(
    name="production_etl",
    job=full_etl_pipeline,
    cron_schedule="30 * * * *",  # Toutes les heures à :30
    execution_timezone="Europe/Paris",
    description="Pipeline ETL complet toutes les heures à :30",
    default_status=DefaultScheduleStatus.RUNNING,
)


# =============================================================================
# Services (devises + dimension temps)
# =============================================================================

services_schedule = ScheduleDefinition(
    name="services_refresh",
    job=services_pipeline,
    cron_schedule="0 3 * * *",  # 3h du matin
    execution_timezone="Europe/Paris",
    description="Refresh devises + dimension temps quotidien",
    default_status=DefaultScheduleStatus.RUNNING,
)