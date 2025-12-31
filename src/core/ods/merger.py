"""
============================================================================
ODS Merger - STAGING → ODS avec SCD2 (historisation + flags)
============================================================================
Logique SCD2 complète :
- _etl_valid_from / _etl_valid_to : Période de validité
- _etl_is_current : TRUE pour ligne active
- _etl_is_deleted : TRUE pour ligne supprimée (soft delete)

Modes :
- INCREMENTAL : INSERT nouvelles + UPDATE modifiées (historisation)
- FULL : + Détection suppressions (soft delete avec _etl_is_deleted)
- FULL_RESET : DROP + CREATE + INSERT (initialisation)
============================================================================
"""

from __future__ import annotations
from typing import List, Sequence
from psycopg2 import sql
from datetime import datetime

from src.config.constants import LoadMode, Schema
from src.db.metadata import get_table_metadata
from src.core.ods.typing import build_ods_columns_definition, build_ods_select_with_casting
from src.core.ods.hashdiff import build_ods_hashdiff
from src.utils.logging import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_columns(cur, schema: str, table: str) -> List[str]:
    """Récupérer liste des colonnes d'une table"""
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    return [r[0] for r in cur.fetchall()]


def _ensure_ods_schema(cur, ods_schema: str) -> None:
    """Créer schéma ODS si nécessaire"""
    cur.execute(
        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}")
        .format(sql.Identifier(ods_schema))
    )


def _create_ods_table_with_scd2(
    cur, 
    ods_schema: str, 
    ods_table: str, 
    columns_metadata: list[dict]
) -> None:
    """
    Créer table ODS avec typage strict + colonnes SCD2
    """
    # Construire définitions colonnes business avec types stricts
    ods_columns = build_ods_columns_definition(columns_metadata)
    
    columns_sql = []
    for col_name, data_type in ods_columns:
        columns_sql.append(
            sql.SQL("{} {}").format(
                sql.Identifier(col_name),
                sql.SQL(data_type)
            )
        )
    
    # Ajouter colonnes SCD2
    columns_sql.extend([
        sql.SQL('"_etl_valid_from" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP'),
        sql.SQL('"_etl_valid_to" TIMESTAMP'),  # NULL = ligne courante
        sql.SQL('"_etl_is_current" BOOLEAN NOT NULL DEFAULT TRUE'),
        sql.SQL('"_etl_is_deleted" BOOLEAN NOT NULL DEFAULT FALSE'),
        sql.SQL('"_etl_hashdiff" VARCHAR(32)'),
        sql.SQL('"_etl_run_id" VARCHAR(100)'),
    ])
    
    # CREATE TABLE
    cur.execute(
        sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.{} (
                {}
            )
        """).format(
            sql.Identifier(ods_schema),
            sql.Identifier(ods_table),
            sql.SQL(", ").join(columns_sql),
        )
    )
    
    logger.info(
        f"ODS table created with SCD2: {ods_schema}.{ods_table}, "
        f"{len(ods_columns)} business columns + 6 SCD2 columns"
    )


def _create_pk_and_indexes(
    cur, 
    ods_schema: str, 
    ods_table: str, 
    primary_keys: Sequence[str]
) -> None:
    """Créer indexes sur table ODS SCD2"""
    
    if primary_keys:
        pk_cols = sql.SQL(", ").join(sql.Identifier(c) for c in primary_keys)
        
        # Index composite PK + _etl_is_current pour requêtes courantes
        try:
            cur.execute(
                sql.SQL("""
                    CREATE INDEX IF NOT EXISTS {} 
                    ON {}.{} ({}, "_etl_is_current")
                """).format(
                    sql.Identifier(f"idx_{ods_table}_pk_current"),
                    sql.Identifier(ods_schema),
                    sql.Identifier(ods_table),
                    pk_cols,
                )
            )
        except Exception as e:
            logger.warning(f"Index creation failed: {e}")
        
        # Index PK + hashdiff pour UPSERT
        try:
            cur.execute(
                sql.SQL("""
                    CREATE INDEX IF NOT EXISTS {} 
                    ON {}.{} ({}, "_etl_hashdiff")
                """).format(
                    sql.Identifier(f"idx_{ods_table}_pk_hashdiff"),
                    sql.Identifier(ods_schema),
                    sql.Identifier(ods_table),
                    pk_cols,
                )
            )
        except Exception as e:
            logger.warning(f"Index creation failed: {e}")
    
    # Index sur dates de validité
    try:
        cur.execute(
            sql.SQL("""
                CREATE INDEX IF NOT EXISTS {}
                ON {}.{} ("_etl_valid_from", "_etl_valid_to")
            """).format(
                sql.Identifier(f"idx_{ods_table}_validity"),
                sql.Identifier(ods_schema),
                sql.Identifier(ods_table),
            )
        )
    except Exception as e:
        logger.warning(f"Index creation failed: {e}")
    
    # Index sur _etl_is_current pour requêtes courantes
    try:
        cur.execute(
            sql.SQL("""
                CREATE INDEX IF NOT EXISTS {}
                ON {}.{} ("_etl_is_current")
                WHERE "_etl_is_current" = TRUE
            """).format(
                sql.Identifier(f"idx_{ods_table}_current"),
                sql.Identifier(ods_schema),
                sql.Identifier(ods_table),
            )
        )
    except Exception as e:
        logger.warning(f"Index creation failed: {e}")


def _dedup_distinct_on(primary_keys: Sequence[str]) -> sql.SQL:
    return sql.SQL(", ").join(sql.Identifier(pk) for pk in primary_keys)


def _dedup_order_by(primary_keys: Sequence[str]) -> sql.SQL:
    parts = [sql.Identifier(pk) for pk in primary_keys]
    parts.append(sql.SQL('"_etl_valid_from" DESC'))
    return sql.SQL(", ").join(parts)


def _pk_join(target_alias: str, source_alias: str, primary_keys: Sequence[str]) -> sql.SQL:
    return sql.SQL(" AND ").join(
        sql.SQL("{}.{} = {}.{}").format(
            sql.SQL(target_alias),
            sql.Identifier(pk),
            sql.SQL(source_alias),
            sql.Identifier(pk),
        )
        for pk in primary_keys
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def merge_staging_to_ods(
    table_name: str,
    run_id: str,
    load_mode: str,
    conn,
) -> int:
    """
    Merger STAGING → ODS avec SCD2 complet
    
    Logique SCD2 :
    - INCREMENTAL : Historise les modifications, insère les nouvelles
    - FULL : + Détecte les suppressions (soft delete)
    - FULL_RESET : Réinitialisation complète
    """
    metadata = get_table_metadata(conn, table_name)
    if not metadata:
        raise ValueError(f"Table metadata not found: {table_name}")

    stg_schema = Schema.STAGING.value
    ods_schema = Schema.ODS.value
    ods_table = metadata["physical_name"].lower()
    stg_table = ods_table
    
    columns_metadata = metadata["columns"]
    primary_keys: List[str] = list(metadata.get("primary_keys") or [])
    
    if not primary_keys:
        raise ValueError(f"No primary keys for SCD2: {table_name}")

    # Calculer hashdiff depuis colonnes STAGING
    hashdiff_expr = build_ods_hashdiff(columns_metadata, source_alias="src")

    with conn.cursor() as cur:
        _ensure_ods_schema(cur, ods_schema)
        
        current_timestamp = datetime.now()

        # ------------------------------------------------------------------
        # FULL_RESET: DROP + CREATE + INSERT initial
        # ------------------------------------------------------------------
        if load_mode == LoadMode.FULL_RESET.value:
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE")
                .format(sql.Identifier(ods_schema), sql.Identifier(ods_table))
            )
            
            # CREATE TABLE avec SCD2
            _create_ods_table_with_scd2(cur, ods_schema, ods_table, columns_metadata)
            
            # INSERT initial avec flags SCD2
            select_with_cast = build_ods_select_with_casting(columns_metadata, "src")
            
            insert_sql = sql.SQL("""
                INSERT INTO {ods_schema}.{ods_table} (
                    {business_cols},
                    "_etl_valid_from",
                    "_etl_valid_to",
                    "_etl_is_current",
                    "_etl_is_deleted",
                    "_etl_hashdiff",
                    "_etl_run_id"
                )
                SELECT 
                    {select_expr},
                    %s AS "_etl_valid_from",
                    NULL AS "_etl_valid_to",
                    TRUE AS "_etl_is_current",
                    FALSE AS "_etl_is_deleted",
                    {hashdiff_expr} AS "_etl_hashdiff",
                    %s AS "_etl_run_id"
                FROM {stg_schema}.{stg_table} src
            """).format(
                ods_schema=sql.Identifier(ods_schema),
                ods_table=sql.Identifier(ods_table),
                business_cols=sql.SQL(", ").join(
                    sql.Identifier(col[0]) 
                    for col in build_ods_columns_definition(columns_metadata)
                ),
                select_expr=sql.SQL(select_with_cast),
                hashdiff_expr=sql.SQL(hashdiff_expr),
                stg_schema=sql.Identifier(stg_schema),
                stg_table=sql.Identifier(stg_table),
            )
            
            cur.execute(insert_sql, (current_timestamp, run_id))
            rows = cur.rowcount
            
            # Créer indexes
            _create_pk_and_indexes(cur, ods_schema, ods_table, primary_keys)
            
            logger.info("ODS FULL_RESET done", table=table_name, rows=rows)
            return rows

        # ------------------------------------------------------------------
        # Vérifier si table existe
        # ------------------------------------------------------------------
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            )
        """, (ods_schema, ods_table))
        
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
            # Créer table avec SCD2
            _create_ods_table_with_scd2(cur, ods_schema, ods_table, columns_metadata)
            
            # INSERT initial
            select_with_cast = build_ods_select_with_casting(columns_metadata, "src")
            
            insert_sql = sql.SQL("""
                INSERT INTO {ods_schema}.{ods_table} (
                    {business_cols},
                    "_etl_valid_from",
                    "_etl_valid_to",
                    "_etl_is_current",
                    "_etl_is_deleted",
                    "_etl_hashdiff",
                    "_etl_run_id"
                )
                SELECT 
                    {select_expr},
                    %s AS "_etl_valid_from",
                    NULL AS "_etl_valid_to",
                    TRUE AS "_etl_is_current",
                    FALSE AS "_etl_is_deleted",
                    {hashdiff_expr} AS "_etl_hashdiff",
                    %s AS "_etl_run_id"
                FROM {stg_schema}.{stg_table} src
            """).format(
                ods_schema=sql.Identifier(ods_schema),
                ods_table=sql.Identifier(ods_table),
                business_cols=sql.SQL(", ").join(
                    sql.Identifier(col[0]) 
                    for col in build_ods_columns_definition(columns_metadata)
                ),
                select_expr=sql.SQL(select_with_cast),
                hashdiff_expr=sql.SQL(hashdiff_expr),
                stg_schema=sql.Identifier(stg_schema),
                stg_table=sql.Identifier(stg_table),
            )
            
            cur.execute(insert_sql, (current_timestamp, run_id))
            rows = cur.rowcount
            
            # Créer indexes
            _create_pk_and_indexes(cur, ods_schema, ods_table, primary_keys)
            
            logger.info("ODS initial load done", table=table_name, rows=rows)
            return rows

        # ------------------------------------------------------------------
        # Table existe : MERGE SCD2 (INCREMENTAL ou FULL)
        # ------------------------------------------------------------------
        
        # Calculer hashdiff avec alias "stg" pour les requêtes de merge
        hashdiff_expr_stg = build_ods_hashdiff(columns_metadata, source_alias="stg")
        
        distinct_on = _dedup_distinct_on(primary_keys)
        order_by = _dedup_order_by(primary_keys)
        pk_join = _pk_join("target", "source", primary_keys)
        select_with_cast = build_ods_select_with_casting(columns_metadata, "stg")
        business_cols = sql.SQL(", ").join(
            sql.Identifier(col[0]) 
            for col in build_ods_columns_definition(columns_metadata)
        )
        
        rows_closed = rows_inserted = rows_deleted = 0

        # ------------------------------------------------------------------
        # 1. CLOSE lignes modifiées (UPDATE valid_to + is_current)
        # ------------------------------------------------------------------
        close_sql = sql.SQL("""
            WITH source AS (
                SELECT DISTINCT ON ({distinct_on})
                    {pk_cols},
                    {hashdiff_expr} AS "_etl_hashdiff"
                FROM {stg_schema}.{stg_table} stg
                ORDER BY {order_by}
            )
            UPDATE {ods_schema}.{ods_table} AS target
            SET 
                "_etl_valid_to" = %s,
                "_etl_is_current" = FALSE
            FROM source
            WHERE {pk_join}
              AND target."_etl_is_current" = TRUE
              AND target."_etl_is_deleted" = FALSE
              AND target."_etl_hashdiff" IS DISTINCT FROM source."_etl_hashdiff"
        """).format(
            distinct_on=distinct_on,
            pk_cols=sql.SQL(", ").join(sql.Identifier(pk) for pk in primary_keys),
            hashdiff_expr=sql.SQL(hashdiff_expr_stg),
            stg_schema=sql.Identifier(stg_schema),
            stg_table=sql.Identifier(stg_table),
            order_by=order_by,
            ods_schema=sql.Identifier(ods_schema),
            ods_table=sql.Identifier(ods_table),
            pk_join=pk_join,
        )
        
        cur.execute(close_sql, (current_timestamp,))
        rows_closed = cur.rowcount

        # ------------------------------------------------------------------
        # 2. INSERT nouvelles versions (modifiées + nouvelles)
        # ------------------------------------------------------------------
        insert_sql = sql.SQL("""
            WITH source AS (
                SELECT DISTINCT ON ({distinct_on})
                    {select_expr},
                    {hashdiff_expr} AS "_etl_hashdiff"
                FROM {stg_schema}.{stg_table} stg
                ORDER BY {order_by}
            )
            INSERT INTO {ods_schema}.{ods_table} (
                {business_cols},
                "_etl_valid_from",
                "_etl_valid_to",
                "_etl_is_current",
                "_etl_is_deleted",
                "_etl_hashdiff",
                "_etl_run_id"
            )
            SELECT 
                {business_cols_src},
                %s AS "_etl_valid_from",
                NULL AS "_etl_valid_to",
                TRUE AS "_etl_is_current",
                FALSE AS "_etl_is_deleted",
                source."_etl_hashdiff",
                %s AS "_etl_run_id"
            FROM source
            WHERE NOT EXISTS (
                SELECT 1
                FROM {ods_schema}.{ods_table} AS target
                WHERE {pk_join}
                  AND target."_etl_is_current" = TRUE
                  AND target."_etl_is_deleted" = FALSE
                  AND target."_etl_hashdiff" = source."_etl_hashdiff"
            )
        """).format(
            distinct_on=distinct_on,
            select_expr=sql.SQL(select_with_cast),
            hashdiff_expr=sql.SQL(hashdiff_expr_stg),
            stg_schema=sql.Identifier(stg_schema),
            stg_table=sql.Identifier(stg_table),
            order_by=order_by,
            ods_schema=sql.Identifier(ods_schema),
            ods_table=sql.Identifier(ods_table),
            business_cols=business_cols,
            business_cols_src=sql.SQL(", ").join(
                sql.SQL("source.{}").format(sql.Identifier(col[0]))
                for col in build_ods_columns_definition(columns_metadata)
            ),
            pk_join=pk_join,
        )
        
        cur.execute(insert_sql, (current_timestamp, run_id))
        rows_inserted = cur.rowcount

        # ------------------------------------------------------------------
        # 3. SOFT DELETE absentes (FULL mode only)
        # ------------------------------------------------------------------
        if load_mode == LoadMode.FULL.value:
            delete_sql = sql.SQL("""
                WITH source AS (
                    SELECT DISTINCT ON ({distinct_on})
                        {pk_cols}
                    FROM {stg_schema}.{stg_table}
                    ORDER BY {order_by}
                )
                UPDATE {ods_schema}.{ods_table} AS target
                SET 
                    "_etl_valid_to" = %s,
                    "_etl_is_current" = FALSE,
                    "_etl_is_deleted" = TRUE
                WHERE target."_etl_is_current" = TRUE
                  AND target."_etl_is_deleted" = FALSE
                  AND NOT EXISTS (
                    SELECT 1 FROM source
                    WHERE {pk_join}
                )
            """).format(
                distinct_on=distinct_on,
                pk_cols=sql.SQL(", ").join(sql.Identifier(pk) for pk in primary_keys),
                stg_schema=sql.Identifier(stg_schema),
                stg_table=sql.Identifier(stg_table),
                order_by=order_by,
                ods_schema=sql.Identifier(ods_schema),
                ods_table=sql.Identifier(ods_table),
                pk_join=pk_join,
            )
            
            cur.execute(delete_sql, (current_timestamp,))
            rows_deleted = cur.rowcount

        total = rows_closed + rows_inserted + rows_deleted

        logger.info(
            "ODS SCD2 merge completed",
            table=table_name,
            mode=load_mode,
            closed=rows_closed,
            inserted=rows_inserted,
            deleted=rows_deleted,
            total=total,
            run_id=run_id,
        )

        return total