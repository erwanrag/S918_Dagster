"""
============================================================================
ODS Typing - Typage strict des colonnes éclatées EXTENT
============================================================================
Conversion STAGING (déjà typé) → ODS (mêmes types)
============================================================================
"""

from typing import Any
from src.utils.logging import get_logger

logger = get_logger(__name__)


def get_ods_type_for_extent_element(
    col_metadata: dict[str, Any],
    element_index: int
) -> str:
    """
    Déterminer le type ODS pour un élément éclaté d'une colonne EXTENT
    
    Args:
        col_metadata: Métadonnées de la colonne EXTENT originale
        element_index: Index de l'élément (1-based, non utilisé actuellement)
    
    Returns:
        Type PostgreSQL pour l'élément éclaté
    
    Example:
        col_metadata = {
            "column_name": "znu",
            "progress_type": "decimal",
            "extent": 5,
            "width": 160,
            "scale": 4
        }
        → "NUMERIC(32,4)" (160 / 5 = 32 par élément)
    """
    progress_type = col_metadata.get("progress_type", "character").lower()
    extent = col_metadata.get("extent", 1)
    width = col_metadata.get("width")
    scale = col_metadata.get("scale")
    
    # Calculer width par élément (approximation)
    element_width = None
    if width and extent > 0:
        element_width = width // extent
    
    # Mapping type Progress → PostgreSQL
    if progress_type == "character" or progress_type == "clob":
        if element_width and element_width < 10000:
            return f"VARCHAR({element_width})"
        return "TEXT"
    
    elif progress_type == "integer":
        return "INTEGER"
    
    elif progress_type == "int64":
        return "BIGINT"
    
    elif progress_type == "decimal":
        if element_width and scale is not None:
            return f"NUMERIC({element_width},{scale})"
        elif scale is not None:
            return f"NUMERIC(17,{scale})"
        return "NUMERIC"
    
    elif progress_type == "logical":
        return "BOOLEAN"
    
    elif progress_type == "date":
        return "DATE"
    
    elif progress_type == "datetime" or progress_type == "datetime-tz":
        return "TIMESTAMP"
    
    else:
        logger.warning(
            f"Unknown progress_type '{progress_type}' for EXTENT column, using TEXT"
        )
        return "TEXT"


def get_ods_column_type(col_metadata: dict[str, Any]) -> str:
    """
    Obtenir le type ODS pour une colonne normale (non-EXTENT)
    
    Args:
        col_metadata: Métadonnées de la colonne
    
    Returns:
        Type PostgreSQL strict
    """
    # Utiliser data_type depuis métadonnées (déjà mappé)
    return col_metadata.get("data_type", "TEXT")


def build_ods_columns_definition(
    columns_metadata: list[dict[str, Any]]
) -> list[tuple[str, str]]:
    """
    Construire la liste des colonnes ODS avec types stricts
    
    Args:
        columns_metadata: Métadonnées colonnes (depuis metadata.json)
    
    Returns:
        Liste de tuples (column_name, data_type)
    
    Example:
        Input: [
            {"column_name": "cod_cli", "data_type": "INTEGER", "extent": 0},
            {"column_name": "znu", "progress_type": "decimal", "extent": 5, 
             "width": 160, "scale": 4}
        ]
        
        Output: [
            ("cod_cli", "INTEGER"),
            ("znu_1", "NUMERIC(32,4)"),
            ("znu_2", "NUMERIC(32,4)"),
            ("znu_3", "NUMERIC(32,4)"),
            ("znu_4", "NUMERIC(32,4)"),
            ("znu_5", "NUMERIC(32,4)")
        ]
    """
    ods_columns = []
    
    for col in columns_metadata:
        col_name = col["column_name"]
        extent = col.get("extent", 0)
        
        if extent > 0:
            # Colonne EXTENT : créer colonnes éclatées avec typage
            for i in range(1, extent + 1):
                ods_type = get_ods_type_for_extent_element(col, i)
                ods_columns.append((f"{col_name}_{i}", ods_type))
        else:
            # Colonne normale : utiliser data_type depuis métadonnées
            ods_type = get_ods_column_type(col)
            ods_columns.append((col_name, ods_type))
    
    return ods_columns


def build_ods_select_with_casting(
    columns_metadata: list[dict[str, Any]],
    source_alias: str = "src"
) -> str:
    """
    Construire SELECT ODS depuis STAGING
    
    ⚠️ STAGING a déjà les types stricts (colonnes typées dès la création),
    donc pas besoin de cast ! On sélectionne simplement les colonnes.
    
    Args:
        columns_metadata: Métadonnées colonnes
        source_alias: Alias de la table source (STAGING)
    
    Returns:
        SQL SELECT simple sans cast
    
    Example:
        SELECT 
            src."cod_cli",
            src."znu_1",
            src."znu_2",
            ...
        FROM staging_etl.stg_client src
    """
    ods_columns = build_ods_columns_definition(columns_metadata)
    select_parts = []
    
    for col_name, data_type in ods_columns:
        # STAGING a déjà le bon type, sélection simple sans cast
        select_parts.append(
            f'{source_alias}."{col_name}"'
        )
    
    return ",\n    ".join(select_parts)