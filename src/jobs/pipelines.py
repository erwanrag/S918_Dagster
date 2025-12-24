"""Jobs"""
from dagster import AssetSelection, define_asset_job
from src.hooks.alerting_hooks import alert_on_failure, alert_on_success

full_etl_pipeline = define_asset_job(
    name="full_etl_pipeline",
    selection=AssetSelection.all(),
    description="Pipeline ETL complet",
    hooks={alert_on_failure, alert_on_success},
)

ingestion_pipeline = define_asset_job(
    name="ingestion_pipeline",
    selection=AssetSelection.groups("ingestion"),
    description="Ingestion: SFTP → RAW → STAGING",
    hooks={alert_on_failure}
)

ods_pipeline = define_asset_job(
    name="ods_pipeline",
    selection=AssetSelection.groups("ods"),
    description="ODS: STAGING → ODS",
    hooks={alert_on_failure}
)

services_pipeline = define_asset_job(
    name="services_pipeline",
    selection=AssetSelection.groups("services"),
    description="Services: Devises + Dimension",
    hooks={alert_on_failure}
)

recovery_from_staging = define_asset_job(
    name="recovery_from_staging",
    selection=AssetSelection.groups("ods"),
    description="Recovery: Recharger ODS depuis STAGING",
    hooks={alert_on_failure}
)

prep_pipeline = define_asset_job(
    name="prep_pipeline",
    selection=AssetSelection.groups("prep"),
    description="PREP: ODS → PREP (dbt)",
)