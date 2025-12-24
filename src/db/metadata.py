"""
============================================================================
Metadata Helper - Accès aux métadonnées des tables
============================================================================
"""

from typing import Any
from psycopg2 import sql

from src.config.constants import Schema
from src.utils.logging import get_logger

logger = get_logger(__name__)


def get_table_metadata(conn, table_name: str) -> dict[str, Any] | None:
    """
    Récupérer les métadonnées d'une table
    """
    with conn.cursor() as cur:
        # ------------------------------------------------------------------
        # Table principale
        # ------------------------------------------------------------------
        cur.execute(
            sql.SQL("""
                SELECT 
                    "TableName",
                    "ConfigName",
                    "PrimaryKeyCols",
                    "HasTimestamps",
                    "ForceFull",
                    "Description"
                FROM {}.etl_tables
                WHERE COALESCE("ConfigName", "TableName") = %s
                  AND "IsActive" = TRUE
                LIMIT 1
            """).format(sql.Identifier(Schema.METADATA.value)),
            (table_name,),
        )

        row = cur.fetchone()
        if not row:
            logger.warning("Table metadata not found", table=table_name)
            return None

        (
            table_name_db,
            config_name,
            primary_keys,
            has_timestamps,
            force_full,
            description,
        ) = row

        # ------------------------------------------------------------------
        # Colonnes
        # ------------------------------------------------------------------
        cur.execute(
            sql.SQL("""
                SELECT 
                    "ColumnName",
                    "DataType",
                    "IsMandatory",
                    "Width",
                    "Scale",
                    "Extent"
                FROM {}.proginovcolumns
                WHERE "TableName" = %s
                ORDER BY "ProgressOrder"
            """).format(sql.Identifier(Schema.METADATA.value)),
            (table_name_db,),
        )

        columns = [
            {
                "column_name": r[0],
                "data_type": r[1],
                "is_mandatory": r[2],
                "width": r[3],
                "scale": r[4],
                "extent": r[5],
            }
            for r in cur.fetchall()
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


def get_active_tables(conn) -> list[str]:
    """Récupérer la liste des tables actives"""
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("""
                SELECT COALESCE("ConfigName", "TableName")
                FROM {}.etl_tables
                WHERE "IsActive" = TRUE
                ORDER BY "TableName"
            """).format(sql.Identifier(Schema.METADATA.value))
        )

        tables = [row[0] for row in cur.fetchall()]
        logger.info("Active tables retrieved", count=len(tables))
        return tables
