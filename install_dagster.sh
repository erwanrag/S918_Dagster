#!/bin/bash
# ============================================================================
# Installation Dagster sur S918-ETL-01-FR (Linux Debian)
# ============================================================================

set -e

echo "========================================="
echo "Installation Dagster v1.9.x"
echo "========================================="

DAGSTER_HOME="/data/dagster"
VENV_PATH="$DAGSTER_HOME/venv"

# ============================================================================
# 1. Création de la structure
# ============================================================================

echo "[1/6] Création de la structure /data/dagster..."

mkdir -p $DAGSTER_HOME/{dagster_home,workspace,logs,storage}
mkdir -p $DAGSTER_HOME/workspace/{assets,resources,schedules,sensors,jobs}

echo "✓ Structure créée"

# ============================================================================
# 2. Python Virtual Environment
# ============================================================================

echo "[2/6] Création du virtual environment Python..."

cd $DAGSTER_HOME
python3 -m venv $VENV_PATH

source $VENV_PATH/bin/activate

pip install --upgrade pip

echo "✓ Virtual environment créé"

# ============================================================================
# 3. Installation des packages Dagster
# ============================================================================

echo "[3/6] Installation packages Dagster..."

pip install \
    dagster==1.9.9 \
    dagster-webserver==1.9.9 \
    dagster-postgres==0.25.9 \
    dagster-dbt==0.25.9 \
    psycopg2-binary \
    pandas \
    pyarrow \
    requests \
    pyyaml \
    sqlalchemy

echo "✓ Packages installés"

# ============================================================================
# 4. Configuration dagster.yaml
# ============================================================================

echo "[4/6] Configuration dagster.yaml..."

cat > $DAGSTER_HOME/dagster_home/dagster.yaml <<'YAML'
# ============================================================================
# Dagster Configuration - S918-ETL-01-FR
# ============================================================================

storage:
  postgres:
    postgres_db:
      username:
        env: ETL_PG_USER
      password:
        env: ETL_PG_PASSWORD
      hostname:
        env: ETL_PG_HOST
      db_name:
        env: ETL_PG_DATABASE
      port:
        env: ETL_PG_PORT

run_launcher:
  module: dagster.core.launcher
  class: DefaultRunLauncher

run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 10

compute_logs:
  module: dagster.core.storage.local_compute_log_manager
  class: LocalComputeLogManager
  config:
    base_dir: /data/dagster/logs

scheduler:
  module: dagster.core.scheduler
  class: DagsterDaemonScheduler

telemetry:
  enabled: false
YAML

echo "✓ dagster.yaml créé"

# ============================================================================
# 5. Variables d'environnement
# ============================================================================

echo "[5/6] Configuration des variables d'environnement..."

cat > $DAGSTER_HOME/.env <<'ENV'
# ============================================================================
# Dagster Environment Variables
# ============================================================================

export DAGSTER_HOME=/data/dagster/dagster_home

# PostgreSQL (CHANGE ME!)
export ETL_PG_HOST=localhost
export ETL_PG_PORT=5432
export ETL_PG_DATABASE=etl_db
export ETL_PG_USER=postgres
export ETL_PG_PASSWORD=CHANGE_ME_PASSWORD

# SFTP
export ETL_SFTP_ROOT=/data/sftp_cbmdata01

# dbt
export ETL_DBT_PROJECT=/data/dagster/workspace/dbt/etl_db

# Python
export PYTHONIOENCODING=utf-8
export PYTHONUTF8=1
ENV

echo "⚠️  IMPORTANT : Éditer $DAGSTER_HOME/.env avec les vrais credentials"

# ============================================================================
# 6. Workspace.yaml
# ============================================================================

echo "[6/6] Configuration workspace.yaml..."

cat > $DAGSTER_HOME/dagster_home/workspace.yaml <<'WORKSPACE'
load_from:
  - python_file:
      relative_path: ../workspace/definitions.py
      working_directory: /data/dagster/dagster_home
WORKSPACE

echo "✓ workspace.yaml créé"

# ============================================================================
# Permissions
# ============================================================================

chown -R eragueneau:dagster $DAGSTER_HOME
chmod -R 775 $DAGSTER_HOME
find $DAGSTER_HOME -type d -exec chmod g+s {} \;

# ============================================================================
# Résumé
# ============================================================================

echo ""
echo "========================================="
echo "✅ Installation Dagster terminée !"
echo "========================================="
echo ""
echo "Prochaines étapes :"
echo "  1. Éditer /data/dagster/.env avec les credentials"
echo "  2. Copier les fichiers Python dans workspace/"
echo "  3. Démarrer : ./start_dagster.sh"
echo ""

