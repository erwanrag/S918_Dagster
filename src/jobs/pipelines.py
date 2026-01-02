"""
Jobs - Pipelines ETL
"""

from dagster import AssetSelection, define_asset_job


# =============================================================================
# Pipeline principal (SFTP → RAW → STAGING → ODS → dbt PREP)
# =============================================================================

full_etl_pipeline = define_asset_job(
    name="full_etl_pipeline",
    selection=AssetSelection.groups(
        "ingestion", 
        "raw", 
        "staging", 
        "ods", 
        "dbt_prep"
    ),
    description="Pipeline ETL complet: SFTP → RAW → STAGING → ODS → dbt PREP",
)


# =============================================================================
# Pipeline Ingestion (SFTP → RAW → STAGING → ODS)
# =============================================================================

ingestion_pipeline = define_asset_job(
    name="ingestion_pipeline",
    selection=AssetSelection.groups("ingestion", "raw", "staging", "ods"),
    description="Ingestion: SFTP → RAW → STAGING → ODS (sans dbt)",
)


# =============================================================================
# Pipeline RAW uniquement (SFTP → RAW)
# =============================================================================

raw_pipeline = define_asset_job(
    name="raw_pipeline",
    selection=AssetSelection.groups("ingestion", "raw"),
    description="RAW: SFTP → RAW uniquement",
)


# =============================================================================
# Pipeline ODS uniquement (STAGING → ODS)
# =============================================================================

ods_pipeline = define_asset_job(
    name="ods_pipeline",
    selection=AssetSelection.groups("ods"),
    description="ODS: STAGING → ODS",
)


# =============================================================================
# Pipeline PREP uniquement (ODS → dbt)
# =============================================================================

prep_pipeline = define_asset_job(
    name="prep_pipeline",
    selection=AssetSelection.groups("dbt_prep"),
    description="PREP: ODS → PREP (dbt)",
)


# =============================================================================
# Pipeline Services uniquement
# =============================================================================

services_pipeline = define_asset_job(
    name="services_pipeline",
    selection=AssetSelection.groups("services"),
    description="Services: Devises + Dimension temps",
)


# =============================================================================
# Recovery: Recharger ODS depuis STAGING
# =============================================================================

recovery_from_staging = define_asset_job(
    name="recovery_from_staging",
    selection=AssetSelection.groups("ods"),
    description="Recovery: Recharger ODS depuis STAGING",

)