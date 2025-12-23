
"""
============================================================================
Hashdiff - Calcul du hash MD5 pour déduplication
============================================================================
"""
from typing import Any

from src.utils.logging import get_logger

logger = get_logger(__name__)


def build_hashdiff_expression(
    columns: list[dict[str, Any]],
    exclude_columns: set[str] | None = None
) -> str:
    """
    Construire l'expression SQL pour _etl_hashdiff
    
    Utilise MD5(CONCAT(...)) sur toutes les colonnes business
    
    Args:
        columns: Liste des colonnes
        exclude_columns: Colonnes à exclure du hash (timestamps, etc.)
        
    Returns:
        Expression SQL: "MD5(CONCAT(COALESCE(col1::text,''), ...))"
    """
    if exclude_columns is None:
        exclude_columns = {"usr_crt", "dat_crt", "usr_mod", "dat_mod"}
    
    # Colonnes à inclure dans le hash
    hash_columns = [
        col["column_name"] 
        for col in columns 
        if col["column_name"] not in exclude_columns
    ]
    
    if not hash_columns:
        logger.warning("No columns for hashdiff, using constant")
        return "'no_hash'"
    
    # Construire CONCAT(COALESCE(col1::text,''), COALESCE(col2::text,''), ...)
    concat_parts = [
        f"COALESCE(\"{col}\"::text, '')"
        for col in hash_columns
    ]
    
    concat_expr = " || ".join(concat_parts)
    
    return f"MD5({concat_expr})"
