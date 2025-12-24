"""
============================================================================
Staging Transformer - Transformation RAW → STAGING
============================================================================
"""

from psycopg2 import sql

from src.config.constants import LoadMode, Schema
from src.core.staging.extent import build_select_with_extent, get_extent_columns
from src.core.staging.hashdiff import build_hashdiff_expression
from src.db.metadata import get_table_metadata
from src.utils.logging import get_logger

logger = get_logger(__name__)


def create_staging_table(
    table_name: str,
    load_mode: str,
    conn,
) -> None:
    """
    Créer ou recréer la table STAGING

    - FULL_RESET: DROP + CREATE
    - FULL / INCREMENTAL: CREATE IF NOT EXISTS
    """
    metadata = get_table_metadata(table_name)
    if not metadata:
        raise ValueError(f"Table metadata not found: {table_name}")

    schema = Schema.STAGING.value
    table = table_name.lower()


    with conn.cursor() as cur:
            # ------------------------------------------------------------------
            # Create schema
            # ------------------------------------------------------------------
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}")
                .format(sql.Identifier(schema))
            )

            # ------------------------------------------------------------------
            # Drop table if FULL_RESET
            # ------------------------------------------------------------------
            if load_mode == LoadMode.FULL_RESET.value:
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE")
                    .format(
                        sql.Identifier(schema),
                        sql.Identifier(table),
                    )
                )
                logger.info("Staging table dropped (FULL_RESET)", table=table_name)

            # ------------------------------------------------------------------
            # Build columns
            # ------------------------------------------------------------------
            extent_cols = get_extent_columns(metadata["columns"])
            columns_sql = []

            for col in metadata["columns"]:
                col_name = col["column_name"]

                if col_name in extent_cols:
                    for i in range(1, extent_cols[col_name] + 1):
                        columns_sql.append(
                            sql.SQL("{} TEXT").format(
                                sql.Identifier(f"{col_name}_{i}")
                            )
                        )
                else:
                    columns_sql.append(
                        sql.SQL("{} TEXT").format(
                            sql.Identifier(col_name)
                        )
                    )

            # ETL columns
            columns_sql.extend(
                [
                    sql.SQL('"{}_hashdiff" VARCHAR(32)').format(sql.Identifier("_etl")),
                    sql.SQL('"{}_run_id" VARCHAR(100)').format(sql.Identifier("_etl")),
                    sql.SQL('"{}_valid_from" TIMESTAMP DEFAULT CURRENT_TIMESTAMP').format(
                        sql.Identifier("_etl")
                    ),
                ]
            )

            # ------------------------------------------------------------------
            # Create table
            # ------------------------------------------------------------------
            cur.execute(
                sql.SQL("""
                    CREATE TABLE IF NOT EXISTS {}.{} (
                        {}
                    )
                """).format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.SQL(", ").join(columns_sql),
                )
            )

            logger.info("Staging table ready", table=table_name, mode=load_mode)


def load_raw_to_staging(
    table_name: str,
    run_id: str,
    load_mode: str,
    conn,
) -> int:
    """
    Charger RAW → STAGING avec extent + hashdiff + déduplication

    Returns:
        Nombre de lignes chargées
    """
    metadata = get_table_metadata(table_name)
    if not metadata:
        raise ValueError(f"Table metadata not found: {table_name}")

    staging_schema = Schema.STAGING.value
    raw_schema = Schema.RAW.value
    table = table_name.lower()

    # Construire SELECT avec extent
    select_expr = build_select_with_extent(metadata["columns"], "src")

    # Construire hashdiff
    hashdiff_expr = build_hashdiff_expression(metadata["columns"])


    with conn.cursor() as cur:
            # ------------------------------------------------------------------
            # Truncate if FULL_RESET
            # ------------------------------------------------------------------
            if load_mode == LoadMode.FULL_RESET.value:
                cur.execute(
                    sql.SQL("TRUNCATE TABLE {}.{}")
                    .format(
                        sql.Identifier(staging_schema),
                        sql.Identifier(table),
                    )
                )

            # ------------------------------------------------------------------
            # Insert data
            # ------------------------------------------------------------------
            query = sql.SQL("""
                INSERT INTO {}.{}
                SELECT DISTINCT ON (_etl_hashdiff)
                    {},
                    {} AS "_etl_hashdiff",
                    %s AS "_etl_run_id",
                    CURRENT_TIMESTAMP AS "_etl_valid_from"
                FROM {}.{} src
                ORDER BY _etl_hashdiff
            """).format(
                sql.Identifier(staging_schema),
                sql.Identifier(table),
                sql.SQL(select_expr),
                sql.SQL(hashdiff_expr),
                sql.Identifier(raw_schema),
                sql.Identifier(f"raw_{table}"),
            )

            cur.execute(query, (run_id,))
            rows_inserted = cur.rowcount

            logger.info(
                "RAW to STAGING loaded",
                table=table_name,
                rows=rows_inserted,
                mode=load_mode,
            )

            return rows_inserted
