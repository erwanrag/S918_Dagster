"""
============================================================================
dbt PREP Assets - Modèles de préparation pour analytics
============================================================================
"""

from pathlib import Path
from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, dbt_assets

from src.config.settings import get_settings

settings = get_settings()

DBT_PROJECT_DIR = Path(settings.dbt_project_dir)
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"

@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
)
def dbt_prep_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    Modèles dbt PREP - Transformations business pour analytics.
    
    Génère automatiquement les assets depuis manifest.json.
    """
    yield from dbt.cli(["build"], context=context).stream()