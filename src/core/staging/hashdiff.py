# """
# ============================================================================
# Hashdiff - Calcul du hash MD5 pour déduplication
# ============================================================================
# Le hashdiff est calculé sur les colonnes ÉCLATÉES en STAGING
# (pas sur les colonnes EXTENT originales de RAW)
# ============================================================================
# """

# from typing import Any

# from src.utils.logging import get_logger

# logger = get_logger(__name__)


# def build_hashdiff_with_exploded_extent(
#     columns: list[dict[str, Any]],
#     exclude_columns: set[str] | None = None
# ) -> str:
#     """
#     Construire hashdiff en incluant les colonnes EXTENT éclatées
    
#     Cette version inclut zal_1, zal_2, ... au lieu de zal
#     Car en STAGING les colonnes EXTENT sont déjà éclatées

#     Args:
#         columns: Liste des métadonnées colonnes (depuis metadata.json)
#         exclude_columns: Colonnes additionnelles à exclure

#     Returns:
#         Expression SQL MD5 avec colonnes éclatées

#     Example:
#         Input columns: [
#             {"column_name": "cod_cli", "extent": 0},
#             {"column_name": "zal", "extent": 5}
#         ]
        
#         Output:
#             MD5(
#                 COALESCE("cod_cli"::text, '') ||
#                 COALESCE("zal_1"::text, '') ||
#                 COALESCE("zal_2"::text, '') ||
#                 COALESCE("zal_3"::text, '') ||
#                 COALESCE("zal_4"::text, '') ||
#                 COALESCE("zal_5"::text, '')
#             )
#     """
#     if exclude_columns is None:
#         # Colonnes standard à exclure du hash
#         exclude_columns = {
#             "usr_crt", "dat_crt", "usr_mod", "dat_mod",
#             "_loaded_at", "_source_file", "_sftp_log_id"
#         }
    
#     hash_columns = []
    
#     for col in columns:
#         col_name = col["column_name"]
#         extent = col.get("extent", 0)
        
#         # Skip si dans exclude list
#         if col_name in exclude_columns:
#             continue
        
#         # Skip si colonne ETL
#         if col_name.startswith("_etl_"):
#             continue
        
#         if extent > 0:
#             # Colonne EXTENT: inclure les colonnes éclatées
#             # En STAGING, zal devient zal_1, zal_2, ..., zal_5
#             for i in range(1, extent + 1):
#                 hash_columns.append(f"{col_name}_{i}")
#         else:
#             # Colonne normale
#             hash_columns.append(col_name)
    
#     if not hash_columns:
#         logger.warning("No columns for hashdiff, using constant")
#         return "'no_hash'"
    
#     # Construire CONCAT avec COALESCE pour gérer les NULL
#     concat_parts = [
#         f"COALESCE(\"{col}\"::text, '')" 
#         for col in hash_columns
#     ]
    
#     # Utiliser || pour concatenation PostgreSQL
#     concat_expr = " || ".join(concat_parts)
    
#     return f"MD5({concat_expr})"