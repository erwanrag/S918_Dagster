"""
Tests unitaires - Calcul _etl_hashdiff ODS
"""

import pytest
from src.core.ods.hashdiff import build_ods_hashdiff


@pytest.mark.unit
def test_build_ods_hashdiff_basic():
    """Test génération expression MD5 basique"""
    columns = [
        {"column_name": "id"},
        {"column_name": "name"},
        {"column_name": "code"},
        {"column_name": "usr_crt"},
        {"column_name": "dat_crt"},
    ]

    result = build_ods_hashdiff(columns, source_alias="stg")

    # Doit contenir MD5
    assert result.startswith("MD5(")

    # Colonnes business incluses
    assert 'stg."id"' in result
    assert 'stg."name"' in result
    assert 'stg."code"' in result

    # Colonnes exclues absentes
    assert "usr_crt" not in result
    assert "dat_crt" not in result

    # Gestion des NULL
    assert "COALESCE" in result

@pytest.mark.unit
def test_ods_hashdiff_with_custom_exclude():
    """Test exclusion personnalisée"""
    columns = [
        {"column_name": "id"},
        {"column_name": "secret"},
    ]

    result = build_ods_hashdiff(
        columns,
        exclude_columns={"secret"},
        source_alias="src",
    )

    assert 'src."id"' in result
    assert "secret" not in result

@pytest.mark.unit
def test_ods_hashdiff_with_no_columns():
    """Edge case: aucune colonne utile"""
    columns = [
        {"column_name": "_etl_run_id"},
        {"column_name": "_etl_valid_from"},
    ]

    result = build_ods_hashdiff(columns)

    assert result == "'no_hash'"

@pytest.mark.unit
def test_ods_hashdiff_with_extent_columns():
    """Test EXTENT éclaté"""
    columns = [
        {"column_name": "znu", "extent": 3},
        {"column_name": "id"},
    ]

    result = build_ods_hashdiff(columns, source_alias="stg")

    assert 'stg."znu_1"' in result
    assert 'stg."znu_2"' in result
    assert 'stg."znu_3"' in result
    assert 'stg."id"' in result

@pytest.mark.unit
def test_ods_hashdiff_concat_format():
    """Test format COALESCE + concat"""
    columns = [
        {"column_name": "col1"},
        {"column_name": "col2"},
    ]

    result = build_ods_hashdiff(columns)

    assert "COALESCE" in result
    assert "::text" in result
    assert "||" in result