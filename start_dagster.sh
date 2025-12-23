#!/bin/bash
# ============================================================================
# Démarrage Dagster Dev Server
# ============================================================================

set -e

DAGSTER_HOME="/data/dagster"
VENV_PATH="$DAGSTER_HOME/venv"

echo "========================================="
echo "Starting Dagster Dev Server"
echo "========================================="

# Activation venv
source $VENV_PATH/bin/activate

# Chargement env vars
set -a
source $DAGSTER_HOME/.env
set +a

export DAGSTER_HOME="/data/dagster/dagster_home"

echo "✓ DAGSTER_HOME: $DAGSTER_HOME"
echo "✓ PostgreSQL: $ETL_PG_HOST:$ETL_PG_PORT/$ETL_PG_DATABASE"
echo ""
echo "========================================="
echo "Dagster UI: http://172.30.27.14:3000"
echo "========================================="
echo ""

cd /data/dagster/dagster_home
dagster dev --host 0.0.0.0 --port 3000

