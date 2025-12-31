"""
============================================================================
Constants - Constantes du projet
============================================================================
"""

from enum import Enum


class LoadMode(str, Enum):
    """Modes de chargement des données"""

    FULL = "FULL"
    INCREMENTAL = "INCREMENTAL"
    FULL_RESET = "FULL_RESET"


class ProcessingStatus(str, Enum):
    """Statuts de traitement"""

    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class Schema(str, Enum):
    """Schémas PostgreSQL"""

    RAW = "raw"
    STAGING = "staging"
    ODS = "ods"
    PREP = "prep"
    METADATA = "metadata"
    REFERENCE = "reference"
    SFTP_MONITORING = "sftp_monitoring"
    ETL_LOGS = "etl_logs"


# Colonnes ETL standards
ETL_COLUMNS = {
    "hashdiff": "_etl_hashdiff",
    "run_id": "_etl_run_id",
    "valid_from": "_etl_valid_from",
    "valid_to": "_etl_valid_to",
    "is_current": "_etl_is_current",
}

# Mapping types Progress → PostgreSQL
PROGRESS_TO_POSTGRES_TYPE_MAP = {
    "character": "VARCHAR",
    "integer": "INTEGER",
    "int64": "BIGINT",
    "decimal": "NUMERIC",
    "date": "DATE",
    "datetime": "TIMESTAMP",
    "logical": "BOOLEAN",
}
