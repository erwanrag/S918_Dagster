"""
============================================================================
Dagster Workspace - Definitions
============================================================================
Point d'entrée principal exposant tous les assets, jobs, schedules, sensors
"""
import sys
# Setup paths AVANT tout import
sys.path.insert(0, '/data/prefect/projects/ETL')
sys.path.insert(0, '/data/prefect/projects')

from dagster import (
    Definitions,
    ScheduleDefinition,
    load_assets_from_modules,
    define_asset_job,
    AssetSelection,
)
from dagster_dbt import DbtCliResource  # ← ICI avec les imports Dagster

# Import des modules
from .assets import ingestion, ods, dbt_prep, services
from .jobs.full_pipeline import (
    full_pipeline_job,
    ingestion_only_job,
    ods_only_job,
    prep_only_job,
    recovery_from_staging_job,
    recovery_from_ods_job
)
from .sensors.sftp_sensor import sftp_file_sensor, sftp_hourly_sensor
from .resources.postgres import postgres_io_manager
from .resources.config import etl_config_resource

# ============================================================================
# CHARGEMENT ASSETS
# ============================================================================

all_assets = load_assets_from_modules(
    modules=[ingestion, ods, dbt_prep, services],
)


# ============================================================================
# JOBS
# ============================================================================

# Job Services (à ajouter à la liste des jobs existants)
services_job = define_asset_job(
    name="services_pipeline",
    selection=AssetSelection.groups("services"),
    description="Pipeline Services: devises + dimension temporelle",
    tags={"pipeline": "services"}
)


# ============================================================================
# SCHEDULES
# ============================================================================

# Schedule horaire pour pipeline complet
hourly_schedule = ScheduleDefinition(
    name="hourly_etl_schedule",
    job=full_pipeline_job,
    cron_schedule="0 * * * *",  # Toutes les heures
    execution_timezone="Europe/Paris",
    description="Exécution horaire du pipeline ETL complet",
    tags={"schedule": "production"}
)

# Schedule quotidien (backup)
daily_schedule = ScheduleDefinition(
    name="daily_etl_schedule",
    job=full_pipeline_job,
    cron_schedule="0 2 * * *",  # 2h du matin
    execution_timezone="Europe/Paris",
    description="Exécution quotidienne du pipeline ETL complet",
    tags={"schedule": "backup"}
)

# Schedule toutes les 4h (fréquent)
frequent_schedule = ScheduleDefinition(
    name="frequent_etl_schedule",
    job=full_pipeline_job,
    cron_schedule="0 */4 * * *",  # Toutes les 4h
    execution_timezone="Europe/Paris",
    description="Exécution fréquente du pipeline (4h)",
    tags={"schedule": "frequent"}
)

# Schedule dbt only (PREP) - Plus fréquent car rapide
dbt_schedule = ScheduleDefinition(
    name="dbt_prep_schedule",
    job=prep_only_job,
    cron_schedule="30 * * * *",  # Toutes les heures à :30
    execution_timezone="Europe/Paris",
    description="Exécution dbt PREP uniquement",
    tags={"schedule": "dbt"}
)

# Schedule Services (Devises + Dimension Temporelle)
services_schedule = ScheduleDefinition(
    name="services_schedule",
    job=services_job,
    cron_schedule="0 3 * * *",  # Quotidien à 3h du matin
    execution_timezone="Europe/Paris",
    description="Chargement quotidien devises et dimension temporelle",
    tags={"schedule": "services"}
)


# ============================================================================
# DEFINITIONS
# ============================================================================

definitions = Definitions(
    assets=all_assets,
    
    jobs=[
        full_pipeline_job,
        ingestion_only_job,
        ods_only_job,
        prep_only_job,
        recovery_from_staging_job,
        recovery_from_ods_job,
        services_job,
    ],
    
    schedules=[
        hourly_schedule,
        daily_schedule,
        frequent_schedule,
        dbt_schedule,
        services_schedule,
    ],
    
    sensors=[
        sftp_file_sensor,
        sftp_hourly_sensor,
    ],
    
    resources={
        "postgres_io": postgres_io_manager,
        "etl_config": etl_config_resource,
        "dbt": DbtCliResource(project_dir="/data/prefect/projects/ETL/dbt/etl_db"),
    },
)