"""Schedules ETL"""
from dagster import ScheduleDefinition
from src.jobs.pipelines import (
    full_etl_pipeline,
    ingestion_pipeline,
    services_pipeline,
)

hourly_schedule = ScheduleDefinition(
    name="hourly_etl_schedule",
    job=full_etl_pipeline,
    cron_schedule="0 * * * *",
    execution_timezone="Europe/Paris",
    description="Pipeline complet toutes les heures",
)

daily_schedule = ScheduleDefinition(
    name="daily_etl_schedule",
    job=full_etl_pipeline,
    cron_schedule="0 2 * * *",
    execution_timezone="Europe/Paris",
    description="Pipeline quotidien 2h",
)

frequent_schedule = ScheduleDefinition(
    name="frequent_etl_schedule",
    job=ingestion_pipeline,
    cron_schedule="0 */4 * * *",
    execution_timezone="Europe/Paris",
    description="Ingestion toutes les 4h",
)

services_schedule = ScheduleDefinition(
    name="services_schedule",
    job=services_pipeline,
    cron_schedule="0 3 * * *",
    execution_timezone="Europe/Paris",
    description="Services quotidien 3h",
)
