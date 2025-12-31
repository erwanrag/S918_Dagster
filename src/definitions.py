"""
Definitions Dagster - Version nettoyée
"""

from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from src.config.settings import get_settings
from src.resources.postgres import PostgresResource
from src.assets import ingestion, raw, staging, ods, services, dbt_prep

from src.jobs.pipelines import (
    full_etl_pipeline,
    ods_pipeline,
    services_pipeline,
)
from src.jobs.auxiliary import (
    maintenance_cleanup_job,
    maintenance_heavy_job,
    metadata_import_job
    # maintenance_schedule,
    # heavy_maintenance_schedule,
    # metadata_import_schedule,
)
from src.schedules.etl_schedules import (
    production_schedule,
    services_schedule,
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
        ods_pipeline,
        services_pipeline,
        
        # Maintenance & imports
        maintenance_cleanup_job,
        maintenance_heavy_job,
        metadata_import_job,
    ],
    
    schedules=[
        # ETL
        production_schedule,           # 30 * * * * (toutes les heures à :30)
        services_schedule,             # 0 3 * * * (3h du matin)
        
        # Maintenance
        # maintenance_schedule,          # 0 4 1 * * (1er du mois à 4h)
        # heavy_maintenance_schedule,    # 0 3 1 */3 * (trimestriel)
        
        # # Metadata
        # metadata_import_schedule,      # 0 8 * * * (8h du matin)
    ],
    
    sensors=[],  # Pas de sensors, 100% schedules
    
    resources={
        "postgres": PostgresResource(dsn=settings.postgres_url),
        "dbt": DbtCliResource(project_dir=str(settings.dbt_project_dir)),
    },
)