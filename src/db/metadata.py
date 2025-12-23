
"""
============================================================================
Metadata Helper - Accès aux métadonnées des tables
============================================================================
"""
from typing import Any

from src.config.constants import Schema
from src.db.connection import get_connection
from src.utils.logging import get_logger

logger = get_logger(__name__)


def get_table_metadata(table_name: str) -> dict[str, Any] | None:
    """
    Récupérer les métadonnées d'une table
    
    Returns:
        dict avec:
            - table_name: Nom physique
            - config_name: Nom logique
            - primary_keys: Liste clés primaires
            - has_timestamps: Si la table a des timestamps
            - force_full: Si on force le mode FULL
            - columns: Liste des colonnes
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Table principale
            cur.execute(f"""
                SELECT 
                    "TableName",
                    "ConfigName",
                    "PrimaryKeyCols",
                    "HasTimestamps",
                    "ForceFull",
                    "Description"
                FROM {Schema.METADATA.value}.etl_tables
                WHERE COALESCE("ConfigName", "TableName") = %s
                  AND "IsActive" = TRUE
                LIMIT 1
            """, (table_name,))
            
            row = cur.fetchone()
            if not row:
                logger.warning("Table metadata not found", table=table_name)
                return None
            
            table_name_db, config_name, primary_keys, has_timestamps, force_full, description = row
            
            # Colonnes
            cur.execute(f"""
                SELECT 
                    "ColumnName",
                    "DataType",
                    "IsMandatory",
                    "Width",
                    "Scale",
                    "Extent"
                FROM {Schema.METADATA.value}.proginovcolumns
                WHERE "TableName" = %s
                ORDER BY "ProgressOrder"
            """, (table_name_db,))
            
            columns = [
                {
                    "column_name": row[0],
                    "data_type": row[1],
                    "is_mandatory": row[2],
                    "width": row[3],
                    "scale": row[4],
                    "extent": row[5],
                }
                for row in cur.fetchall()
            ]
            
            return {
                "table_name": table_name_db,
                "config_name": config_name,
                "physical_name": config_name or table_name_db,
                "primary_keys": primary_keys or [],
                "has_timestamps": has_timestamps,
                "force_full": force_full,
                "description": description or "",
                "columns": columns,
            }


def get_active_tables() -> list[str]:
    """Récupérer la liste des tables actives"""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT COALESCE("ConfigName", "TableName")
                FROM {Schema.METADATA.value}.etl_tables
                WHERE "IsActive" = TRUE
                ORDER BY "TableName"
            """)
            
            tables = [row[0] for row in cur.fetchall()]
            logger.info("Active tables retrieved", count=len(tables))
            return tables
