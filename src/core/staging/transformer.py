"""
============================================================================
Staging Transformer - Transformation RAW → STAGING
============================================================================
Éclatement des colonnes EXTENT depuis RAW (TEXT) vers STAGING (colonnes typées)
Exemple: znu (TEXT "123.45;0;0;0;0") → znu_1, znu_2, znu_3, znu_4, znu_5 (NUMERIC)
============================================================================
"""

from psycopg2 import sql

from src.config.constants import LoadMode, Schema
from src.core.staging.extent import (
    build_select_with_extent, 
    get_extent_columns,
    count_expanded_columns
)
from src.core.staging.hashdiff import build_hashdiff_with_exploded_extent
from src.core.ods.typing import get_ods_type_for_extent_element
from src.db.metadata import get_table_metadata
from src.utils.logging import get_logger

logger = get_logger(__name__)


def create_staging_table(
    table_name: str,
    load_mode: str,
    conn,
) -> None:
    """
    Créer ou recréer la table STAGING avec colonnes EXTENT éclatées et typées

    - FULL_RESET: DROP + CREATE
    - FULL / INCREMENTAL: CREATE IF NOT EXISTS
    
    En STAGING, les colonnes EXTENT sont éclatées avec typage strict :
    - RAW : zal TEXT ("AAA;;;;")
    - STAGING : zal_1 VARCHAR, zal_2 VARCHAR, ...
    - RAW : znu TEXT ("123.45;0;0;0;0")
    - STAGING : znu_1 NUMERIC(32,4), znu_2 NUMERIC(32,4), ...
    """
    metadata = get_table_metadata(conn, table_name)
    if not metadata:
        raise ValueError(f"Table metadata not found: {table_name}")

    schema = Schema.STAGING.value
    table = metadata["physical_name"].lower()
    
    columns_metadata = metadata["columns"]
    extent_cols = get_extent_columns(columns_metadata)
    
    # Compter colonnes totales après éclatement
    total_cols_expanded = count_expanded_columns(columns_metadata)
    
    logger.info(
        "Creating STAGING table",
        table=table_name,
        physical=table,
        mode=load_mode,
        base_columns=len(columns_metadata),
        extent_columns=len(extent_cols),
        total_expanded=total_cols_expanded
    )

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
        # Build columns (avec éclatement EXTENT + typage)
        # ------------------------------------------------------------------
        columns_sql = []

        for col in columns_metadata:
            col_name = col["column_name"]
            extent = col.get("extent", 0)

            if extent > 0:
                # Colonne EXTENT: créer colonnes éclatées avec typage strict
                for i in range(1, extent + 1):
                    element_type = get_ods_type_for_extent_element(col, i)
                    columns_sql.append(
                        sql.SQL("{} {}").format(
                            sql.Identifier(f"{col_name}_{i}"),
                            sql.SQL(element_type)
                        )
                    )
            else:
                # Colonne normale: utiliser data_type depuis métadonnées
                data_type = col.get("data_type", "TEXT")
                columns_sql.append(
                    sql.SQL("{} {}").format(
                        sql.Identifier(col_name),
                        sql.SQL(data_type)
                    )
                )

        # ETL columns
        columns_sql.extend([
            sql.SQL('"_etl_hashdiff" VARCHAR(32)'),
            sql.SQL('"_etl_run_id" VARCHAR(100)'),
            sql.SQL('"_etl_valid_from" TIMESTAMP DEFAULT CURRENT_TIMESTAMP'),
        ])

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

        logger.info(
            "Staging table ready", 
            table=table_name, 
            mode=load_mode,
            columns_created=len(columns_sql)
        )


def load_raw_to_staging(
    table_name: str,
    run_id: str,
    load_mode: str,
    conn,
) -> int:
    """
    Charger RAW → STAGING avec éclatement EXTENT + cast + hashdiff + déduplication
    
    Transformations:
    - Colonnes EXTENT éclatées avec split_part(colonne, ';', N)
    - Cast vers types stricts (NUMERIC, INTEGER, BOOLEAN, etc.)
    - Hashdiff calculé sur colonnes éclatées
    - DISTINCT ON (hashdiff) pour déduplication

    Returns:
        Nombre de lignes chargées
    """
    metadata = get_table_metadata(conn, table_name)
    if not metadata:
        raise ValueError(f"Table metadata not found: {table_name}")

    staging_schema = Schema.STAGING.value
    raw_schema = Schema.RAW.value
    table = metadata["physical_name"].lower()
    
    columns_metadata = metadata["columns"]

    # Construire SELECT avec éclatement extent (split_part) + cast
    select_expr = build_select_with_extent_and_cast(columns_metadata, "src")

    # Construire hashdiff sur colonnes éclatées
    hashdiff_expr = build_hashdiff_with_exploded_extent(columns_metadata)
    
    logger.info(
        "Loading RAW to STAGING",
        table=table_name,
        physical=table,
        mode=load_mode
    )

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
            logger.debug("Staging table truncated", table=table_name)

        # ------------------------------------------------------------------
        # Insert data avec CTE pour éclatement puis hashdiff
        # ------------------------------------------------------------------
        query = sql.SQL("""
            WITH exploded AS (
                SELECT 
                    {select_expr}
                FROM {raw_schema}.{raw_table} src
            ),
            hashed AS (
                SELECT 
                    *,
                    {hashdiff_expr} AS "_etl_hashdiff",
                    %s AS "_etl_run_id",
                    CURRENT_TIMESTAMP AS "_etl_valid_from"
                FROM exploded
            )
            INSERT INTO {staging_schema}.{table}
            SELECT DISTINCT ON ("_etl_hashdiff") *
            FROM hashed
            ORDER BY "_etl_hashdiff"
        """).format(
            staging_schema=sql.Identifier(staging_schema),
            table=sql.Identifier(table),
            select_expr=sql.SQL(select_expr),
            hashdiff_expr=sql.SQL(hashdiff_expr),
            raw_schema=sql.Identifier(raw_schema),
            raw_table=sql.Identifier(f"raw_{table}"),
        )

        cur.execute(query, (run_id,))
        rows_inserted = cur.rowcount

        logger.info(
            "RAW to STAGING loaded",
            table=table_name,
            physical=table,
            rows=rows_inserted,
            mode=load_mode,
        )

        return rows_inserted


def build_select_with_extent_and_cast(
    columns_metadata: list[dict],
    source_alias: str = "src"
) -> str:
    """
    Construire SELECT RAW → STAGING
    
    RAW a déjà les types stricts SAUF pour EXTENT (qui sont TEXT avec ';')
    
    Logique:
    - EXTENT: split_part() + cast
    - Normal: copie directe (RAW a déjà le bon type)
    """
    select_parts = []
    
    for col in columns_metadata:
        col_name = col["column_name"]
        extent = col.get("extent", 0)
        
        if extent > 0:
            # EXTENT: RAW est TEXT, split + cast
            for i in range(1, extent + 1):
                element_type = get_ods_type_for_extent_element(col, i)
                target_col = f"{col_name}_{i}"
                
                # Gestion cast selon type
                if element_type.startswith("NUMERIC") or element_type in ["INTEGER", "BIGINT"]:
                    select_parts.append(
                        f'NULLIF(TRIM(split_part({source_alias}."{col_name}", \';\', {i})), \'\')::{element_type} '
                        f'AS "{target_col}"'
                    )
                elif element_type == "BOOLEAN":
                    select_parts.append(
                        f'CASE WHEN LOWER(TRIM(split_part({source_alias}."{col_name}", \';\', {i}))) '
                        f'IN (\'true\', \'1\', \'yes\', \'t\') THEN TRUE '
                        f'WHEN LOWER(TRIM(split_part({source_alias}."{col_name}", \';\', {i}))) '
                        f'IN (\'false\', \'0\', \'no\', \'f\', \'\') THEN FALSE '
                        f'ELSE NULL END AS "{target_col}"'
                    )
                elif element_type in ["DATE", "TIMESTAMP"]:
                    select_parts.append(
                        f'CASE WHEN TRIM(split_part({source_alias}."{col_name}", \';\', {i})) = \'\' '
                        f'THEN NULL '
                        f'ELSE TRIM(split_part({source_alias}."{col_name}", \';\', {i}))::{element_type} END '
                        f'AS "{target_col}"'
                    )
                else:  # VARCHAR, TEXT
                    select_parts.append(
                        f'TRIM(split_part({source_alias}."{col_name}", \';\', {i})) AS "{target_col}"'
                    )
        else:
            # Normal: RAW a déjà le bon type, copie directe
            select_parts.append(
                f'{source_alias}."{col_name}"'
            )
    
    return ",\n    ".join(select_parts)