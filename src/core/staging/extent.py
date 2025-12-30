"""
============================================================================
Extent Handler - Éclatement des colonnes EXTENT (RAW → STAGING)
============================================================================
Les colonnes EXTENT en RAW sont stockées comme TEXT avec séparateur ";"
Ex: znu = "123.45;0;0;0;0"

En STAGING, on éclate avec split_part() :
- znu_1 = split_part(znu, ';', 1) = "123.45"
- znu_2 = split_part(znu, ';', 2) = "0"
- etc.
============================================================================
"""

from typing import Any

from src.utils.logging import get_logger

logger = get_logger(__name__)


def get_extent_columns(columns: list[dict[str, Any]]) -> dict[str, int]:
    """
    Identifier les colonnes avec extent > 0
    
    Args:
        columns: Liste des métadonnées colonnes depuis metadata.json
    
    Returns:
        dict: {column_name: extent_count}
    
    Example:
        Input: [
            {"column_name": "zal", "extent": 5},
            {"column_name": "cod_cli", "extent": 0}
        ]
        Output: {"zal": 5}
    """
    extent_cols = {}
    for col in columns:
        if col.get("extent", 0) > 0:
            extent_cols[col["column_name"]] = col["extent"]

    return extent_cols


def generate_extent_select(
    column_name: str, 
    extent: int, 
    source_alias: str = "src"
) -> list[str]:
    """
    Générer les colonnes éclatées pour une colonne EXTENT avec split_part()
    
    IMPORTANT: Progress utilise le POINT-VIRGULE (;) comme séparateur
    
    Args:
        column_name: Nom de la colonne EXTENT (ex: "zal")
        extent: Nombre d'éléments (ex: 5)
        source_alias: Alias de la table source (ex: "src")
    
    Returns:
        Liste des expressions SQL
    
    Example:
        column_name="zal", extent=5
        Returns: [
            "split_part(src.\"zal\", ';', 1) AS \"zal_1\"",
            "split_part(src.\"zal\", ';', 2) AS \"zal_2\"",
            "split_part(src.\"zal\", ';', 3) AS \"zal_3\"",
            "split_part(src.\"zal\", ';', 4) AS \"zal_4\"",
            "split_part(src.\"zal\", ';', 5) AS \"zal_5\""
        ]
    
    Progress data example in RAW:
        zal = "AAA;;;;"
    
    After split_part in STAGING:
        zal_1 = "AAA"
        zal_2 = ""
        zal_3 = ""
        zal_4 = ""
        zal_5 = ""
    """
    return [
        f'split_part({source_alias}."{column_name}", \';\', {i}) AS "{column_name}_{i}"'
        for i in range(1, extent + 1)
    ]


def build_select_with_extent(
    columns: list[dict[str, Any]], 
    source_alias: str = "src"
) -> str:
    """
    Construire la clause SELECT avec éclatement des colonnes EXTENT
    
    Args:
        columns: Liste des métadonnées colonnes depuis metadata.json
        source_alias: Alias de la table source (ex: "src")
    
    Returns:
        String SQL pour SELECT
    
    Example:
        Input columns: [
            {"column_name": "cod_cli", "extent": 0},
            {"column_name": "zal", "extent": 5}
        ]
        
        Output:
            src."cod_cli",
            split_part(src."zal", ';', 1) AS "zal_1",
            split_part(src."zal", ';', 2) AS "zal_2",
            split_part(src."zal", ';', 3) AS "zal_3",
            split_part(src."zal", ';', 4) AS "zal_4",
            split_part(src."zal", ';', 5) AS "zal_5"
    """
    extent_cols = get_extent_columns(columns)
    select_parts = []

    for col in columns:
        col_name = col["column_name"]

        if col_name in extent_cols:
            # Colonne avec extent: éclater avec split_part()
            extent = extent_cols[col_name]
            select_parts.extend(generate_extent_select(col_name, extent, source_alias))
            
            logger.debug(
                f"Exploding EXTENT column {col_name} into {extent} columns"
            )
        else:
            # Colonne normale: copie directe
            select_parts.append(f'{source_alias}."{col_name}"')

    return ",\n    ".join(select_parts)


def count_expanded_columns(columns: list[dict[str, Any]]) -> int:
    """
    Compter le nombre total de colonnes après éclatement EXTENT
    
    Args:
        columns: Liste des métadonnées colonnes
    
    Returns:
        Nombre total de colonnes après éclatement
    
    Example:
        Input: 220 colonnes dont 35 avec extent (total 160 éléments)
        Calcul: (220 - 35) colonnes normales + 160 colonnes éclatées
        Output: 345 colonnes
    """
    normal_cols = sum(1 for col in columns if col.get("extent", 0) == 0)
    extent_elements = sum(col.get("extent", 0) for col in columns)
    
    return normal_cols + extent_elements