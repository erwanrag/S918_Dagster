"""
============================================================================
Dagster Definitions - ETL CBM Analytics
============================================================================
"""

from dagster import Definitions, load_assets_from_modules

# Import des modules d'assets
from assets import sftp_assets, raw_assets

# Import des resources
from resources.postgres_resource import postgres_resource

# Import des schedules (commenté pour l'instant)
# from schedules.etl_schedules import etl_hourly_schedule

# ============================================================================
# Chargement de tous les assets
# ============================================================================

all_assets = [
    *load_assets_from_modules([sftp_assets]),
    *load_assets_from_modules([raw_assets])
]

# ============================================================================
# Définition du workspace
# ============================================================================

defs = Definitions(
    assets=all_assets,
    resources={
        "postgres": postgres_resource
    },
    # Pas de schedules pour l'instant
    # schedules=[
    #     etl_hourly_schedule
    # ]
)