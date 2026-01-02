"""
Tests unitaires - Scanner SFTP
"""

import pytest
from unittest.mock import patch, MagicMock
from src.core.sftp.scanner import scan_parquet_files


@pytest.mark.unit
@patch("src.core.sftp.scanner.get_settings")
def test_scan_parquet_files_empty_directory(mock_get_settings, tmp_path):
    """Test scan d'un répertoire vide"""
    mock_settings = MagicMock()
    mock_settings.sftp_parquet_dir = tmp_path
    mock_get_settings.return_value = mock_settings

    result = scan_parquet_files()
    assert result == []


@pytest.mark.unit
@patch("src.core.sftp.scanner.get_settings")
def test_scan_parquet_files_invalid_filename(mock_get_settings, tmp_path):
    """Test scan avec nom de fichier invalide"""
    mock_settings = MagicMock()
    mock_settings.sftp_parquet_dir = tmp_path
    mock_get_settings.return_value = mock_settings

    invalid_file = tmp_path / "invalid.parquet"
    invalid_file.write_bytes(b"test")

    result = scan_parquet_files()
    assert result == []


@pytest.mark.unit
@patch("src.core.sftp.scanner.get_settings")
def test_scan_parquet_files_directory_not_found(mock_get_settings, tmp_path):
    """Test scan quand le répertoire n'existe pas"""
    mock_settings = MagicMock()
    mock_settings.sftp_parquet_dir = tmp_path / "nonexistent"
    mock_get_settings.return_value = mock_settings

    result = scan_parquet_files()
    assert result == []
