"""
============================================================================
Dagster Assets - dbt PREP Layer (ODS → PREP)
============================================================================
Intégration native dagster-dbt pour transformer ODS → PREP
"""

import sys
sys.path.insert(0, '/data/prefect/projects')

from dagster import (
    asset,
    AssetExecutionContext,
    Output,
    MetadataValue,
)
from dagster_dbt import DbtCliResource, dbt_assets
from pathlib import Path
from typing import Dict, Any

# Chemin dbt en dur (ou depuis variable d'environnement)
import os
DBT_PROJECT_DIR = Path(os.getenv('ETL_DBT_PROJECT', '/data/prefect/projects/ETL/dbt/etl_db'))
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"


# ============================================================================
# ASSET 6: dbt PREP Models (via dagster-dbt)
# ============================================================================

@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    select="prep.*",
    name="dbt_prep_models"
)
def dbt_prep_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    Asset 6: Exécuter tous les modèles dbt PREP
    
    Équivalent Prefect: ods_to_prep_flow() avec dbt run
    
    dagster-dbt génère automatiquement:
    - 1 asset par modèle dbt
    - Lineage basé sur ref()
    - Metadata (rows, execution time)
    
    Returns:
        dbt run results
    """
    # Exécuter dbt run --models prep.*
    yield from dbt.cli(["run", "--models", "prep.*"], context=context).stream()


# ============================================================================
# ASSET 7: dbt PREP Tests
# ============================================================================

@asset(
    name="dbt_prep_tests",
    group_name="prep",
    description="Exécuter tests dbt sur modèles PREP",
    compute_kind="dbt",
    deps=[dbt_prep_models]
)
def dbt_prep_tests(
    context: AssetExecutionContext,
    dbt: DbtCliResource
) -> Dict[str, Any]:
    """
    Asset 7: Exécuter tests dbt
    
    Équivalent Prefect: run_dbt_tests() dans ods_to_prep_flow()
    
    Returns:
        dict: Résultats des tests
    """
    context.log.info("=" * 70)
    context.log.info("ASSET: dbt PREP Tests")
    context.log.info("=" * 70)
    
    # Exécuter dbt test --models prep.*
    test_results = dbt.cli(["test", "--models", "prep.*"], context=context).wait()
    
    # Parser résultats (simplifié)
    tests_passed = 0
    tests_failed = 0
    
    if test_results.success:
        context.log.info("[OK] All dbt tests passed")
        tests_passed = 1  # Placeholder
    else:
        context.log.warning("[WARN] Some dbt tests failed")
        tests_failed = 1  # Placeholder
    
    context.log.info("=" * 70)
    
    return {
        'success': test_results.success,
        'tests_passed': tests_passed,
        'tests_failed': tests_failed
    }


# ============================================================================
# ASSET 8: PREP Summary
# ============================================================================

@asset(
    name="prep_pipeline_summary",
    group_name="prep",
    description="Résumé du pipeline PREP",
    compute_kind="metadata",
    deps=["dbt_prep_tests"]
)
def prep_pipeline_summary(
    context: AssetExecutionContext,
    dbt_prep_tests: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Asset 8: Résumé complet du pipeline PREP
    
    Returns:
        dict: Statistiques PREP
    """
    context.log.info("=" * 70)
    context.log.info("PIPELINE PREP SUMMARY")
    context.log.info("=" * 70)
    
    import psycopg2
    from shared.config import config
    
    conn = psycopg2.connect(config.get_connection_string())
    cur = conn.cursor()
    
    try:
        # Compter tables PREP
        cur.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'prep'
        """)
        total_prep_tables = cur.fetchone()[0]
        
        # Compter lignes totales PREP
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables 
            WHERE table_schema = 'prep'
        """)
        
        total_prep_rows = 0
        for (table,) in cur.fetchall():
            try:
                cur.execute(f"SELECT COUNT(*) FROM prep.{table}")
                total_prep_rows += cur.fetchone()[0]
            except:
                pass
        
        summary = {
            'total_prep_tables': total_prep_tables,
            'total_prep_rows': total_prep_rows,
            'tests_success': dbt_prep_tests['success'],
            'tests_passed': dbt_prep_tests['tests_passed'],
            'tests_failed': dbt_prep_tests['tests_failed']
        }
        
        context.log.info(f"Total PREP tables: {total_prep_tables}")
        context.log.info(f"Total PREP rows: {total_prep_rows:,}")
        context.log.info(f"Tests passed: {dbt_prep_tests['tests_passed']}")
        context.log.info("=" * 70)
        
        yield Output(
            summary,
            metadata={
                "prep_tables": total_prep_tables,
                "prep_rows": total_prep_rows,
                "tests_passed": dbt_prep_tests['tests_passed']
            }
        )
        
    finally:
        cur.close()
        conn.close()