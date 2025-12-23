"""
Fixtures pytest communes
"""

import pytest
from pathlib import Path


@pytest.fixture
def mock_settings():
    """Mock des settings"""
    from unittest.mock import MagicMock

    settings = MagicMock()
    settings.pg_host = "localhost"
    settings.pg_port = 5432
    settings.pg_database = "etl_db_test"
    settings.pg_user = "postgres"
    settings.pg_password = "test"
    settings.postgres_url = "postgresql://postgres:test@localhost:5432/etl_db_test"
    settings.sftp_root = Path("/tmp/sftp_test")
    settings.sftp_parquet_dir = Path("/tmp/sftp_test/parquet")
    return settings


@pytest.fixture
def sample_columns():
    """Colonnes de test"""
    return [
        {"column_name": "id", "data_type": "integer", "extent": 0},
        {"column_name": "name", "data_type": "character", "extent": 0},
        {"column_name": "code", "data_type": "character", "extent": 3},  # EXTENT
        {"column_name": "usr_crt", "data_type": "character", "extent": 0},
        {"column_name": "dat_crt", "data_type": "datetime", "extent": 0},
    ]


@pytest.fixture
def sample_metadata():
    """Metadata complète de test"""
    return {
        "table_name": "test_table",
        "config_name": "test_table",
        "physical_name": "test_table",
        "primary_keys": ["id"],
        "has_timestamps": True,
        "force_full": False,
        "columns": [
            {"column_name": "id", "data_type": "integer", "extent": 0},
            {"column_name": "name", "data_type": "character", "extent": 0},
            {"column_name": "code", "data_type": "character", "extent": 3},
        ],
    }
