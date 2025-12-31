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
                    logger.debug(f"Column {col_name}_{i} has type: {element_type}")
                    # Défensif : accepter BIT, LOGICAL, BOOLEAN
                    if element_type.upper() in ["BIT", "LOGICAL"]:
                        element_type = "BOOLEAN"
                        logger.debug(f"Normalized to BOOLEAN for EXTENT column {col_name}_{i}")
                    columns_sql.append(
                        sql.SQL("{} {}").format(
                            sql.Identifier(f"{col_name}_{i}"),
                            sql.SQL(element_type)
                        )
                    )
            else:
                # Colonne normale: utiliser data_type depuis métadonnées
                data_type = col.get("data_type", "TEXT")
                logger.debug(f"Column {col_name} has type: {data_type}")
                # Défensif : accepter BIT, LOGICAL, BOOLEAN
                if data_type.upper() in ["BIT", "LOGICAL"]:
                    data_type = "BOOLEAN"
                    logger.debug(f"Normalized to BOOLEAN for column {col_name}")
                columns_sql.append(
                    sql.SQL("{} {}").format(
                        sql.Identifier(col_name),
                        sql.SQL(data_type)
                    )
                )

        # ETL columns
        columns_sql.extend([
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
    Charger RAW → STAGING avec éclatement EXTENT + cast
    PAS de hashdiff ici (calculé en ODS)
    """
    metadata = get_table_metadata(conn, table_name)
    if not metadata:
        raise ValueError(f"Table metadata not found: {table_name}")

    staging_schema = Schema.STAGING.value
    raw_schema = Schema.RAW.value
    table = metadata["physical_name"].lower()
    columns_metadata = metadata["columns"]

    select_expr = build_select_with_extent_and_cast(columns_metadata, "src")
    
    logger.info("Loading RAW to STAGING", table=table_name, physical=table, mode=load_mode)

    with conn.cursor() as cur:
        if load_mode == LoadMode.FULL_RESET.value:
            cur.execute(
                sql.SQL("TRUNCATE TABLE {}.{}")
                .format(sql.Identifier(staging_schema), sql.Identifier(table))
            )
            logger.debug("Staging table truncated", table=table_name)

        query = sql.SQL("""
            INSERT INTO {staging_schema}.{table}
            SELECT 
                {select_expr},
                %s AS "_etl_run_id",
                CURRENT_TIMESTAMP AS "_etl_valid_from"
            FROM {raw_schema}.{raw_table} src
        """).format(
            staging_schema=sql.Identifier(staging_schema),
            table=sql.Identifier(table),
            select_expr=sql.SQL(select_expr),
            raw_schema=sql.Identifier(raw_schema),
            raw_table=sql.Identifier(f"raw_{table}"),
        )
        logger.error(f"========== SQL Query for {table_name} ==========")
        logger.error(query.as_string(conn))
        cur.execute(query, (run_id,))
        rows_inserted = cur.rowcount

        logger.info("RAW to STAGING loaded", table=table_name, rows=rows_inserted, mode=load_mode)
        return rows_inserted


def build_select_with_extent_and_cast(
    columns_metadata: list[dict],
    source_alias: str = "src"
) -> str:
    """
    Construire SELECT RAW → STAGING
    
    RAW est maintenant TOUT EN TEXT (données brutes)
    
    Logique:
    - EXTENT: split_part() + cast
    - Normal: cast depuis TEXT vers type strict
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
                elif element_type.upper() in ["BOOLEAN", "BIT", "LOGICAL"]:
                    select_parts.append(
                        f'CASE '
                        f'WHEN LOWER(TRIM(split_part({source_alias}."{col_name}", \';\', {i}))) IN (\'true\', \'t\', \'1\', \'yes\') THEN TRUE '
                        f'WHEN LOWER(TRIM(split_part({source_alias}."{col_name}", \';\', {i}))) IN (\'false\', \'f\', \'0\', \'no\', \'\') THEN FALSE '
                        f'ELSE NULL END AS "{target_col}"'
                    )
                elif element_type in ["DATE", "TIMESTAMP"]:
                    select_parts.append(
                        f'''CASE 
                            WHEN TRIM(split_part({source_alias}."{col_name}", ';', {i})) = '' THEN NULL
                            WHEN TRIM(split_part({source_alias}."{col_name}", ';', {i})) = '?' THEN NULL
                            WHEN TRIM(split_part({source_alias}."{col_name}", ';', {i})) ~ '^\d{{2}}/\d{{2}}/\d{{4}}$' THEN
                                TO_DATE(TRIM(split_part({source_alias}."{col_name}", ';', {i})), 'MM/DD/YYYY')::{element_type}
                            ELSE 
                                TRIM(split_part({source_alias}."{col_name}", ';', {i}))::{element_type}
                        END AS "{target_col}"'''
                    )
                else:  # VARCHAR, TEXT
                    select_parts.append(
                        f'TRIM(split_part({source_alias}."{col_name}", \';\', {i})) AS "{target_col}"'
                    )
        else:
            # Normal: RAW est TEXT, il faut TOUJOURS caster
            data_type = col.get("data_type", "TEXT")
            
            if data_type.startswith("NUMERIC") or data_type in ["INTEGER", "BIGINT"]:
                select_parts.append(
                    f'NULLIF(TRIM({source_alias}."{col_name}"), \'\')::{data_type} AS "{col_name}"'
                )
            elif data_type.upper() in ["BOOLEAN", "BIT", "LOGICAL"]:  # ← ✅ CORRIGÉ ICI
                select_parts.append(
                    f'CASE '
                    f'WHEN LOWER(TRIM({source_alias}."{col_name}")) IN (\'true\', \'t\', \'1\', \'yes\') THEN TRUE '
                    f'WHEN LOWER(TRIM({source_alias}."{col_name}")) IN (\'false\', \'f\', \'0\', \'no\', \'\') THEN FALSE '
                    f'ELSE NULL END AS "{col_name}"'
                )
            elif data_type in ["DATE", "TIMESTAMP"]:
                select_parts.append(
                    f'CASE WHEN TRIM({source_alias}."{col_name}") IN (\'\', \'?\') THEN NULL '
                    f'ELSE TRIM({source_alias}."{col_name}")::{data_type} END AS "{col_name}"'
                )
            else:  # TEXT, VARCHAR ou autre type non prévu
                # CAST QUAND MÊME pour garantir le bon type
                select_parts.append(
                    f'NULLIF(TRIM({source_alias}."{col_name}"), \'\')::{data_type} AS "{col_name}"'
                )
    
    return ",\n    ".join(select_parts)