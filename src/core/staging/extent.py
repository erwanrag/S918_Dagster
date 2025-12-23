"""
============================================================================
Extent Handler - Éclatement des colonnes EXTENT Progress
============================================================================
"""

from typing import Any

from src.utils.logging import get_logger

logger = get_logger(__name__)


def get_extent_columns(columns: list[dict[str, Any]]) -> dict[str, int]:
    """
    Identifier les colonnes avec extent > 0

    Returns:
        dict: {column_name: extent_count}
    """
    extent_cols = {}
    for col in columns:
        if col.get("extent", 0) > 0:
            extent_cols[col["column_name"]] = col["extent"]

    return extent_cols


def generate_extent_select(
    column_name: str, extent: int, source_alias: str = "src"
) -> list[str]:
    """
    Générer les colonnes éclatées pour une colonne EXTENT

    Example:
        column_name="cod_art", extent=5
        Returns: [
            "src.cod_art[1] AS cod_art_1",
            "src.cod_art[2] AS cod_art_2",
            ...
        ]
    """
    return [
        f'{source_alias}."{column_name}"[{i}] AS "{column_name}_{i}"'
        for i in range(1, extent + 1)
    ]


def build_select_with_extent(
    columns: list[dict[str, Any]], source_alias: str = "src"
) -> str:
    """
    Construire la clause SELECT avec éclatement des EXTENT

    Returns:
        String SQL: "col1, col2, col3[1] AS col3_1, col3[2] AS col3_2, ..."
    """
    extent_cols = get_extent_columns(columns)
    select_parts = []

    for col in columns:
        col_name = col["column_name"]

        if col_name in extent_cols:
            # Colonne avec extent: éclater
            extent = extent_cols[col_name]
            select_parts.extend(generate_extent_select(col_name, extent, source_alias))
        else:
            # Colonne normale
            select_parts.append(f'{source_alias}."{col_name}"')

    return ",\n    ".join(select_parts)
