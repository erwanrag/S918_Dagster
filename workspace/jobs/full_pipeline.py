"""
============================================================================
Dagster Job - Full ETL Pipeline
============================================================================
Équivalent Prefect: full_etl_pipeline() dans orchestration/full_pipeline.py
Orchestre: SFTP → RAW → STAGING → ODS → PREP
"""

from dagster import (
    job,
    op,
    In,
    Out,
    DynamicOut,
    DynamicOutput,
    AssetSelection,
    define_asset_job,
)
from typing import List


# ============================================================================
# JOB PRINCIPAL: Full ETL Pipeline
# ============================================================================

full_pipeline_job = define_asset_job(
    name="full_etl_pipeline",
    description="Pipeline ETL complet: SFTP → RAW → STAGING → ODS → PREP",
    selection=AssetSelection.all(),
    tags={
        "pipeline": "etl",
        "env": "production"
    }
)


# ============================================================================
# JOB PARTIEL: Ingestion Only (SFTP → RAW → STAGING)
# ============================================================================

ingestion_only_job = define_asset_job(
    name="ingestion_pipeline",
    description="Pipeline ingestion uniquement: SFTP → RAW → STAGING",
    selection=AssetSelection.groups("ingestion"),
    tags={
        "pipeline": "ingestion",
        "env": "production"
    }
)


# ============================================================================
# JOB PARTIEL: ODS Only (STAGING → ODS)
# ============================================================================

ods_only_job = define_asset_job(
    name="ods_pipeline",
    description="Pipeline ODS uniquement: STAGING → ODS",
    selection=AssetSelection.groups("ods"),
    tags={
        "pipeline": "ods",
        "env": "production"
    }
)


# ============================================================================
# JOB PARTIEL: PREP Only (ODS → PREP via dbt)
# ============================================================================

prep_only_job = define_asset_job(
    name="prep_pipeline",
    description="Pipeline PREP uniquement: ODS → PREP (dbt)",
    selection=AssetSelection.groups("prep"),
    tags={
        "pipeline": "prep",
        "env": "production"
    }
)


# ============================================================================
# JOB RECOVERY: Rejouer depuis STAGING
# ============================================================================

recovery_from_staging_job = define_asset_job(
    name="recovery_from_staging",
    description="Recovery: Rejouer depuis STAGING → ODS → PREP",
    selection=AssetSelection.groups("ods", "prep"),
    tags={
        "pipeline": "recovery",
        "env": "production"
    }
)


# ============================================================================
# JOB RECOVERY: Rejouer depuis ODS
# ============================================================================

recovery_from_ods_job = define_asset_job(
    name="recovery_from_ods",
    description="Recovery: Rejouer depuis ODS → PREP (dbt uniquement)",
    selection=AssetSelection.groups("prep"),
    tags={
        "pipeline": "recovery",
        "env": "production"
    }
)