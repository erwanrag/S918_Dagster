
"""
============================================================================
Definitions - Point d'entrée Dagster
============================================================================
"""
from dagster import Definitions, load_assets_from_modules

from src.assets import ingestion, ods, services
from src.utils.logging import setup_logging

# Setup logging
setup_logging()

# Charger tous les assets
all_assets = load_assets_from_modules([ingestion, ods, services])

# Definitions Dagster (version simple v1.0)
definitions = Definitions(
    assets=all_assets,
)
