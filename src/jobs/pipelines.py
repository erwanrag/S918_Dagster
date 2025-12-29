"""
Jobs - Pipelines ETL
"""

from dagster import AssetSelection, define_asset_job
from src.hooks.alerting_hooks import alert_on_failure, alert_on_success


# =============================================================================
# Pipeline principal (sans services)
# =============================================================================

full_etl_pipeline = define_asset_job(
    name="full_etl_pipeline",
    selection=AssetSelection.groups("ingestion", "raw", "staging", "ods", "prep"),
    description="Pipeline ETL complet: SFTP → RAW → STAGING → ODS → PREP",
    hooks={alert_on_failure, alert_on_success},
)


# =============================================================================
# Pipeline Ingestion (SFTP → RAW → STAGING)
# =============================================================================

ingestion_pipeline = define_asset_job(
    name="ingestion_pipeline",
    selection=AssetSelection.groups("ingestion", "raw", "staging"),
    description="Ingestion: SFTP → RAW → STAGING",
    hooks={alert_on_failure},
)


# =============================================================================
# Pipeline ODS uniquement
# =============================================================================

ods_pipeline = define_asset_job(
    name="ods_pipeline",
    selection=AssetSelection.groups("ods"),
    description="ODS: STAGING → ODS",
    hooks={alert_on_failure},
)


# =============================================================================
# Pipeline PREP uniquement
# =============================================================================

prep_pipeline = define_asset_job(
    name="prep_pipeline",
    selection=AssetSelection.groups("prep"),
    description="PREP: ODS → PREP (dbt)",
    hooks={alert_on_failure},
)


# =============================================================================
# Pipeline Services uniquement
# =============================================================================

services_pipeline = define_asset_job(
    name="services_pipeline",
    selection=AssetSelection.groups("services"),
    description="Services: Devises + Dimension temps",
    hooks={alert_on_failure},
)


# =============================================================================
# Recovery: Recharger ODS depuis STAGING
# =============================================================================

recovery_from_staging = define_asset_job(
    name="recovery_from_staging",
    selection=AssetSelection.groups("ods"),
    description="Recovery: Recharger ODS depuis STAGING",
    hooks={alert_on_failure},
)