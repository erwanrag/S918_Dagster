# src/assets/dbt_prep.py
"""
============================================================================
dbt PREP Assets - Modèles de préparation pour analytics
============================================================================
"""

from pathlib import Path
from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator

from src.config.settings import get_settings

settings = get_settings()

DBT_PROJECT_DIR = Path(settings.dbt_project_dir)
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"


class CustomDbtTranslator(DagsterDbtTranslator):
    """Translator pour assigner le groupe dbt_prep et déclarer dépendances ODS"""
    
    def get_group_name(self, dbt_resource_props):
        return "dbt_prep"
    
    def get_upstream_asset_keys(self, dbt_resource_props):
        """Déclarer que dbt dépend de ods_tables"""
        upstream = super().get_upstream_asset_keys(dbt_resource_props)
        return upstream | {AssetKey("ods_tables")}


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    dagster_dbt_translator=CustomDbtTranslator(),
    # ❌ RETIRER partitions_def pour l'instant
)
def dbt_prep_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    Modèles dbt PREP - Transformations business pour analytics.
    
    Dépend de : ODS (tables ods.*)
    Génère automatiquement les assets depuis manifest.json.
    """
    context.log.info("Starting dbt build (after ODS completion)")
    yield from dbt.cli(["build"], context=context).stream()