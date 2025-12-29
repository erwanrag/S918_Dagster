"""
Assets dbt PREP - Solution wrapper simple
"""

from pathlib import Path
from dagster import AssetExecutionContext, asset
from dagster_dbt import DbtCliResource

from src.config.settings import get_settings

settings = get_settings()


@asset(
    name="dbt_prep_models",
    group_name="prep",
    required_resource_keys={"dbt"},
    description="Modèles dbt PREP - Dépend de ODS",
)
def dbt_prep_models(
    context: AssetExecutionContext,
    ods_tables: dict,  # ← DÉPENDANCE EXPLICITE VERS ODS
) -> dict:
    """
    Exécute dbt build pour tous les modèles PREP après que ODS soit prêt.
    
    Cette approche wrapper garantit que dbt ne lance PAS avant ODS.
    """
    context.log.info(f"ODS ready: {ods_tables.get('tables_merged', 0)} tables merged")
    context.log.info("Starting dbt build for PREP models")
    
    # Obtenir la resource dbt
    dbt: DbtCliResource = context.resources.dbt
    
    # Exécuter dbt build
    result = dbt.cli(["build"], context=context).wait()
    
    if result.success:
        context.log.info("dbt build completed successfully")
        return {"success": True, "ods_tables_merged": ods_tables.get('tables_merged', 0)}
    else:
        raise Exception(f"dbt build failed")