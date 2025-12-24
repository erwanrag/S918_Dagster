"""Jobs"""
from dagster import AssetSelection, define_asset_job

full_etl_pipeline = define_asset_job(
    name="full_etl_pipeline",
    selection=AssetSelection.all(),
    description="Pipeline ETL complet",
)

ingestion_pipeline = define_asset_job(
    name="ingestion_pipeline",
    selection=AssetSelection.groups("ingestion"),
    description="Ingestion: SFTP → RAW → STAGING",
)

ods_pipeline = define_asset_job(
    name="ods_pipeline",
    selection=AssetSelection.groups("ods"),
    description="ODS: STAGING → ODS",
)

services_pipeline = define_asset_job(
    name="services_pipeline",
    selection=AssetSelection.groups("services"),
    description="Services: Devises + Dimension",
)

recovery_from_staging = define_asset_job(
    name="recovery_from_staging",
    selection=AssetSelection.groups("ods"),
    description="Recovery: Recharger ODS depuis STAGING",
)
