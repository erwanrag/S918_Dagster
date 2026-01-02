"""
Definitions Dagster - Version avec sensors
"""

from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from src.config.settings import get_settings
from src.resources.postgres import PostgresResource
from src.assets import ingestion, raw, staging, ods, services, dbt_prep

from src.jobs.pipelines import (
    full_etl_pipeline,
    ingestion_pipeline,
    raw_pipeline,
    ods_pipeline,
    prep_pipeline,
    services_pipeline,
    recovery_from_staging,
)
from src.jobs.auxiliary import (
    maintenance_cleanup_job,
    maintenance_heavy_job,
    metadata_import_job
)
from src.schedules.etl_schedules import (
    production_schedule,
    services_schedule,
)

# ✅ IMPORT DES SENSORS
from src.hooks.alerting_hooks import (
    success_notification_sensor,
    failure_notification_sensor,
)

from src.utils.logging import setup_logging


# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
setup_logging()
settings = get_settings()


# ---------------------------------------------------------------------
# Assets
# ---------------------------------------------------------------------
all_assets = load_assets_from_modules([
    ingestion,   # sftp_parquet_inventory
    raw,         # raw_sftp_tables
    staging,     # staging_tables
    ods,         # ods_tables
    services,    # currency_codes, exchange_rates, time_dimension
    dbt_prep,    # dbt_prep_models (27 modèles)
])


# ---------------------------------------------------------------------
# Definitions
# ---------------------------------------------------------------------
definitions = Definitions(
    assets=all_assets,
    
    jobs=[
        # Pipeline principal
        full_etl_pipeline,
        
        # Pipelines partiels
        ingestion_pipeline,
        raw_pipeline,
        ods_pipeline,
        prep_pipeline,
        services_pipeline,
        
        # Recovery
        recovery_from_staging,
        
        # Maintenance & imports
        maintenance_cleanup_job,
        maintenance_heavy_job,
        metadata_import_job,
    ],
    
    schedules=[
        production_schedule,
        services_schedule,
    ],
    
    # ✅ AJOUTER SENSORS
    sensors=[
        success_notification_sensor,
        failure_notification_sensor,
    ],
    
    resources={
        "postgres": PostgresResource(dsn=settings.postgres_url),
        "dbt": DbtCliResource(project_dir=str(settings.dbt_project_dir)),
    },
)