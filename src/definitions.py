"""
Definitions Dagster
"""

import os
from dagster import Definitions, load_assets_from_modules
from dagster_dbt import DbtCliResource

from src.resources.postgres import PostgresResource
from src.assets import ingestion, raw, staging, ods, services, dbt_prep
from src.jobs.pipelines import (
    full_etl_pipeline,
    ingestion_pipeline,
    ods_pipeline,
    services_pipeline,
    recovery_from_staging,
)
from src.schedules.etl_schedules import (
    daily_schedule,
    frequent_schedule,
    hourly_schedule,
    services_schedule,
)
from src.sensors.sftp_sensor import (
    sftp_file_sensor,
    sftp_hourly_sensor,
)
from src.utils.logging import setup_logging


# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
setup_logging()


# ---------------------------------------------------------------------
# Assets
# ---------------------------------------------------------------------
all_assets = load_assets_from_modules([
    ingestion,
    raw,
    staging,
    ods,
    services,
    dbt_prep,
])


# ---------------------------------------------------------------------
# Definitions
# ---------------------------------------------------------------------
definitions = Definitions(
    assets=all_assets,
    jobs=[
        full_etl_pipeline,
        ingestion_pipeline,
        ods_pipeline,
        services_pipeline,
        recovery_from_staging,
    ],
    schedules=[
        hourly_schedule,
        daily_schedule,
        frequent_schedule,
        services_schedule,
    ],
    sensors=[
        sftp_file_sensor,
        sftp_hourly_sensor,
    ],
    resources={
        "postgres": PostgresResource(
            dsn=os.environ["POSTGRES_URL"]
        ),
        "dbt": DbtCliResource(
            project_dir="/data/prefect/projects/ETL/dbt/etl_db"
        ),
    },
)
