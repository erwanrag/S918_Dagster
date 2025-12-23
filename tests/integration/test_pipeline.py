"""
Tests d'intégration - Pipeline complet
"""
import pytest
from unittest.mock import patch, MagicMock
from dagster import build_op_context


@pytest.mark.integration
@patch("src.core.sftp.scanner.get_settings")
def test_full_pipeline_no_files(mock_get_settings, tmp_path):
    """Test pipeline complet sans fichiers"""
    from src.assets.ingestion import (
        sftp_discovered_files,
        raw_tables_loaded,
    )
    
    # Setup
    mock_settings = MagicMock()
    mock_settings.sftp_parquet_dir = tmp_path
    mock_get_settings.return_value = mock_settings
    
    # Créer un contexte Dagster réel
    context = build_op_context()
    
    # Execute asset 1
    files = sftp_discovered_files(context)
    
    # Assert
    assert files == []
    
    # Execute asset 2
    raw_result = raw_tables_loaded(context, files)
    
    # Assert
    assert raw_result["tables_loaded"] == 0
    assert raw_result["total_rows"] == 0


@pytest.mark.integration
def test_services_assets():
    """Test assets services (imports)"""
    from src.assets.services import (
        currency_codes_loaded,
        exchange_rates_loaded,
        time_dimension_built,
    )
    
    assert currency_codes_loaded is not None
    assert exchange_rates_loaded is not None
    assert time_dimension_built is not None
