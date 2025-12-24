import subprocess
import sys
from pathlib import Path
from dagster import op
from dagster_dbt import DbtCliResource
from src.config.settings import get_settings

@op(name="generate_prep_models", tags={"kind": "dbt_gen"})
def generate_prep_models_op(context):
    settings = get_settings()
    # On cherche le script dans le dossier scripts à la racine
    script_path = settings.dagster_home.parent / "scripts" / "generators" / "generate_prep_models.py"
    
    if not script_path.exists():
        context.log.warning(f"Script de génération introuvable : {script_path}")
        return

    result = subprocess.run([sys.executable, str(script_path)], capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Erreur génération : {result.stderr}")
    context.log.info(f"Génération OK : {result.stdout}")

@op(name="compile_generated_models", tags={"kind": "dbt_gen"})
def compile_generated_models_op(context, dbt: DbtCliResource):
    dbt.cli(["compile", "--models", "prep.*"], context=context).wait()