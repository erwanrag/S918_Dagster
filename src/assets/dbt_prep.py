"""
============================================================================
Assets dbt PREP
============================================================================
"""

from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

DBT_PROJECT_DIR = Path("/data/prefect/projects/ETL/dbt/etl_db")
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    select="prep.*",
)
def dbt_prep_models(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
):
    """
    Transformations dbt PREP.

    • Source : ODS
    • Usage : préparation analytique
    • Porté par dbt
    """
    yield from dbt.cli(["run"], context=context).stream()
