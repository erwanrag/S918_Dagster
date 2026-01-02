from pathlib import Path
from dagster import AssetExecutionContext, AssetKey, AssetMaterialization, MetadataValue
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator
import time
import json

from src.config.settings import get_settings

settings = get_settings()

DBT_PROJECT_DIR = Path(settings.dbt_project_dir)
DBT_MANIFEST_PATH = DBT_PROJECT_DIR / "target" / "manifest.json"


class CustomDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props):
        return "dbt_prep"

    def get_asset_key(self, dbt_resource_props):
        resource_type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]
        schema = dbt_resource_props.get("schema", "prep")

        if resource_type == "model":
            return AssetKey([schema, name])

        return super().get_asset_key(dbt_resource_props)


@dbt_assets(
    manifest=DBT_MANIFEST_PATH,
    dagster_dbt_translator=CustomDbtTranslator(),
)
def dbt_prep_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    dbt PREP models
    + résumé global matérialisé proprement
    """
    start_time = time.time()

    context.log.info("=" * 80)
    context.log.info("🚀 Starting dbt build (PREP)")
    context.log.info("=" * 80)

    # ▶️ DBT BUILD
    yield from dbt.cli(["build"], context=context).stream()

    total_duration = time.time() - start_time

    run_results_path = DBT_PROJECT_DIR / "target" / "run_results.json"
    if not run_results_path.exists():
        context.log.warning("run_results.json not found")
        return

    with open(run_results_path) as f:
        run_results = json.load(f)

    # ===== STATS =====
    total_models = models_success = models_failed = 0
    total_tests = tests_passed = tests_failed = 0

    for result in run_results.get("results", []):
        node = result.get("unique_id", "")
        status = result.get("status", "unknown")
        exec_time = result.get("execution_time", 0.0)
        rows = result.get("adapter_response", {}).get("rows_affected", 0)

        if node.startswith("model."):
            total_models += 1
            model = node.split(".")[-1]

            if status == "success":
                models_success += 1
                emoji = "✅"
            else:
                models_failed += 1
                emoji = "❌"

            context.log.info(
                f"{emoji} {model:40s} | {rows:>8,} rows | {exec_time:>6.2f}s"
            )

        elif node.startswith("test."):
            total_tests += 1
            if status == "pass":
                tests_passed += 1
            else:
                tests_failed += 1

    # ===== RÉSUMÉ =====
    context.log.info("=" * 80)
    context.log.info("📊 DBT BUILD SUMMARY")
    context.log.info(f"Models : {models_success}/{total_models} success")
    context.log.info(f"Tests  : {tests_passed}/{total_tests} passed")
    context.log.info(f"Time   : {total_duration:.2f}s")
    context.log.info("=" * 80)

    # ✅ SEULE SORTIE MÉTA (PROPRE)
    yield AssetMaterialization(
        asset_key=AssetKey(["dbt_prep", "dbt_build_summary"]),
        description="Global dbt PREP build summary",
        metadata={
            "models_total": MetadataValue.int(total_models),
            "models_success": MetadataValue.int(models_success),
            "models_failed": MetadataValue.int(models_failed),
            "tests_total": MetadataValue.int(total_tests),
            "tests_passed": MetadataValue.int(tests_passed),
            "tests_failed": MetadataValue.int(tests_failed),
            "duration_seconds": MetadataValue.float(round(total_duration, 2)),
            "dbt_command": MetadataValue.text("dbt build"),
            "summary": MetadataValue.md(f"""
### 📊 dbt PREP Build Summary

| Metric | Value |
|--------|-------|
| ✅ Models success | {models_success}/{total_models} |
| ❌ Models failed | {models_failed} |
| ✅ Tests passed | {tests_passed}/{total_tests} |
| ⏱️ Duration | {total_duration:.2f}s |
            """),
        },
    )
