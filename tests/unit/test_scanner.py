"""
Tests unitaires - Scanner SFTP
"""

import pytest
from unittest.mock import patch, MagicMock
from src.core.sftp.scanner import scan_parquet_files, SftpFile


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
def test_scan_parquet_files_with_valid_files(mock_get_settings, tmp_path):
    """Test scan avec fichiers valides"""
    mock_settings = MagicMock()
    mock_settings.sftp_parquet_dir = tmp_path
    mock_get_settings.return_value = mock_settings

    file1 = tmp_path / "client_20251223_100000.parquet"
    file2 = tmp_path / "produit_20251223_110000.parquet"
    file1.write_bytes(b"test data 1")
    file2.write_bytes(b"test data 2")

    result = scan_parquet_files()

    assert len(result) == 2
    assert all(isinstance(f, SftpFile) for f in result)
    # Vérifier sans ordre spécifique
    table_names = {f.table_name for f in result}
    assert "client" in table_names
    assert "produit" in table_names


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
