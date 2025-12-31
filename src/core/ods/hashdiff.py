"""
============================================================================
Hashdiff ODS - Calcul du hash MD5 pour SCD2
============================================================================
Calcul du hashdiff depuis les colonnes STAGING (déjà éclatées et typées)
============================================================================
"""

from typing import Any
from src.utils.logging import get_logger

logger = get_logger(__name__)


def build_ods_hashdiff(
    columns_metadata: list[dict[str, Any]],
    source_alias: str = "stg",
    exclude_columns: set[str] | None = None
) -> str:
    """
    Construire hashdiff pour ODS depuis colonnes STAGING
    
    Args:
        columns_metadata: Métadonnées colonnes
        source_alias: Alias de la table source (ex: "stg", "src")
        exclude_columns: Colonnes à exclure
    
    Returns:
        Expression SQL MD5
    """
    if exclude_columns is None:
        exclude_columns = {
            "usr_crt", "dat_crt", "usr_mod", "dat_mod",
            "_loaded_at", "_source_file", "_sftp_log_id"
        }
    
    hash_columns = []
    
    for col in columns_metadata:
        col_name = col["column_name"]
        extent = col.get("extent", 0)
        
        if col_name in exclude_columns or col_name.startswith("_etl_"):
            continue
        
        if extent > 0:
            # Colonnes EXTENT éclatées
            for i in range(1, extent + 1):
                hash_columns.append(f"{col_name}_{i}")
        else:
            # Colonne normale
            hash_columns.append(col_name)
    
    if not hash_columns:
        logger.warning("No columns for hashdiff, using constant")
        return "'no_hash'"
    
    # Construire concat avec COALESCE
    concat_parts = [
        f"COALESCE({source_alias}.\"{col}\"::text, '')" 
        for col in hash_columns
    ]
    
    concat_expr = " || ".join(concat_parts)
    
    return f"MD5({concat_expr})"