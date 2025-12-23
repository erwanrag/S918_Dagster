"""
Tests unitaires - Calcul _etl_hashdiff
"""

import pytest
from src.core.staging.hashdiff import build_hashdiff_expression


@pytest.mark.unit
def test_build_hashdiff_expression(sample_columns):
    """Test génération expression MD5"""
    result = build_hashdiff_expression(sample_columns)

    # Doit contenir MD5
    assert result.startswith("MD5(")

    # Doit contenir les colonnes business (pas usr_crt, dat_crt)
    assert '"id"' in result
    assert '"name"' in result
    assert '"code"' in result

    # Ne doit PAS contenir les colonnes exclues
    assert '"usr_crt"' not in result
    assert '"dat_crt"' not in result

    # Doit contenir COALESCE pour gérer les NULL
    assert "COALESCE" in result


@pytest.mark.unit
def test_hashdiff_with_custom_exclude():
    """Test avec colonnes exclues personnalisées"""
    columns = [
        {"column_name": "id", "data_type": "integer"},
        {"column_name": "secret", "data_type": "character"},
    ]

    result = build_hashdiff_expression(columns, exclude_columns={"secret"})

    assert '"id"' in result
    assert '"secret"' not in result


@pytest.mark.unit
def test_hashdiff_with_no_columns():
    """Test avec aucune colonne (edge case)"""
    result = build_hashdiff_expression([])

    # Doit retourner une constante
    assert result == "'no_hash'"


@pytest.mark.unit
def test_hashdiff_concat_format():
    """Test format CONCAT avec ::text et ''"""
    columns = [
        {"column_name": "col1", "data_type": "integer"},
        {"column_name": "col2", "data_type": "character"},
    ]

    result = build_hashdiff_expression(columns, exclude_columns=set())

    # Doit avoir le format COALESCE(col::text, '')
    assert "COALESCE(\"col1\"::text, '')" in result
    assert "COALESCE(\"col2\"::text, '')" in result
    assert "||" in result  # Concaténation
