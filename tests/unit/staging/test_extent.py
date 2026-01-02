"""
Tests unitaires - Éclatement colonnes EXTENT
"""

import pytest
from src.core.staging.extent import (
    get_extent_columns,
    generate_extent_select,
    build_select_with_extent,
)


@pytest.mark.unit
def test_get_extent_columns(sample_columns):
    """Test identification colonnes EXTENT"""
    extent_cols = get_extent_columns(sample_columns)

    assert len(extent_cols) == 1
    assert "code" in extent_cols
    assert extent_cols["code"] == 3


@pytest.mark.unit
def test_generate_extent_select():
    """Test génération SELECT pour colonne EXTENT"""
    result = generate_extent_select("code", 3, "src")

    assert len(result) == 3
    assert result[0] == 'split_part(src."code", \';\', 1) AS "code_1"'
    assert result[1] == 'split_part(src."code", \';\', 2) AS "code_2"'
    assert result[2] == 'split_part(src."code", \';\', 3) AS "code_3"'


@pytest.mark.unit
def test_build_select_with_extent(sample_columns):
    """Test construction SELECT complet avec EXTENT"""
    result = build_select_with_extent(sample_columns, "src")

    # Colonnes normales
    assert 'src."id"' in result
    assert 'src."name"' in result

    # Colonnes EXTENT éclatées
    assert 'split_part(src."code", \';\', 1) AS "code_1"' in result
    assert 'split_part(src."code", \';\', 2) AS "code_2"' in result
    assert 'split_part(src."code", \';\', 3) AS "code_3"' in result

    # La colonne brute "code" ne doit jamais être sélectionnée seule
    assert 'src."code",' not in result.split("AS")


@pytest.mark.unit
def test_extent_with_no_extent_columns():
    """Test avec aucune colonne EXTENT"""
    columns = [
        {"column_name": "id", "data_type": "integer", "extent": 0},
        {"column_name": "name", "data_type": "character", "extent": 0},
    ]

    result = build_select_with_extent(columns, "t")

    assert 't."id"' in result
    assert 't."name"' in result
    assert "split_part" not in result
