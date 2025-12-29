# tests/unit/test_sftp_scanner.py
from dagster import build_asset_context
from src.assets.ingestion import sftp_parquet_inventory

def test_sftp_inventory_no_files(tmp_path):
    context = build_asset_context(
        resources={"postgres": MockPostgresResource()}
    )
    result = sftp_parquet_inventory(context)
    assert result == []

def test_sftp_inventory_with_files(tmp_path, sample_parquet_files):
    # sample_parquet_files = fixture qui crée fichiers test
    context = build_asset_context()
    result = sftp_parquet_inventory(context)
    assert len(result) == 3
    assert result[0]["table_name"] == "article"