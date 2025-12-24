"""
============================================================================
ODS Merger - STAGING → ODS (current-state)
============================================================================

Semantics:
- INCREMENTAL: UPDATE modified + INSERT new (no delete)
- FULL: UPDATE + INSERT + DELETE missing
- FULL_RESET: DROP + CREATE (LIKE STAGING) + INSERT ALL
"""

from __future__ import annotations

from typing import List, Sequence

from psycopg2 import sql

from src.config.constants import LoadMode, Schema
from src.db.connection import get_connection
from src.db.metadata import get_table_metadata
from src.utils.logging import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_columns(cur, schema: str, table: str) -> List[str]:
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


def _common_columns(cur, stg_schema: str, stg_table: str, ods_schema: str, ods_table: str) -> List[str]:
    stg_cols = set(_get_columns(cur, stg_schema, stg_table))
    ods_cols = set(_get_columns(cur, ods_schema, ods_table))
    return [c for c in _get_columns(cur, stg_schema, stg_table) if c in ods_cols]


def _ensure_ods_schema_and_table(cur, stg_schema: str, stg_table: str, ods_schema: str, ods_table: str) -> None:
    cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(ods_schema)))
    cur.execute(
        sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.{} (
                LIKE {}.{} INCLUDING ALL
            )
        """).format(
            sql.Identifier(ods_schema),
            sql.Identifier(ods_table),
            sql.Identifier(stg_schema),
            sql.Identifier(stg_table),
        )
    )


def _create_pk_and_indexes(cur, ods_schema: str, ods_table: str, primary_keys: Sequence[str], cols: Sequence[str]) -> None:
    if primary_keys:
        pk_cols = sql.SQL(", ").join(sql.Identifier(c) for c in primary_keys)
        cur.execute(
            sql.SQL("ALTER TABLE {}.{} ADD PRIMARY KEY ({})").format(
                sql.Identifier(ods_schema),
                sql.Identifier(ods_table),
                pk_cols,
            )
        )

        if "_etl_hashdiff" in cols:
            cur.execute(
                sql.SQL("""
                    CREATE INDEX IF NOT EXISTS {} 
                    ON {}.{} ({}, {})
                """).format(
                    sql.Identifier(f"idx_{ods_table}_upsert"),
                    sql.Identifier(ods_schema),
                    sql.Identifier(ods_table),
                    pk_cols,
                    sql.Identifier("_etl_hashdiff"),
                )
            )

    if "_etl_valid_from" in cols:
        cur.execute(
            sql.SQL("""
                CREATE INDEX IF NOT EXISTS {}
                ON {}.{} ({})
            """).format(
                sql.Identifier(f"idx_{ods_table}_etl_valid_from"),
                sql.Identifier(ods_schema),
                sql.Identifier(ods_table),
                sql.Identifier("_etl_valid_from"),
            )
        )


def _dedup_distinct_on(primary_keys: Sequence[str]) -> sql.SQL:
    return sql.SQL(", ").join(sql.Identifier(pk) for pk in primary_keys)


def _dedup_order_by(primary_keys: Sequence[str], has_valid_from: bool) -> sql.SQL:
    parts = [sql.Identifier(pk) for pk in primary_keys]
    if has_valid_from:
        parts.append(sql.SQL("{} DESC").format(sql.Identifier("_etl_valid_from")))
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


def _update_set_clause(primary_keys: Sequence[str], columns: Sequence[str]) -> sql.SQL:
    updatable = [c for c in columns if c not in primary_keys]
    return sql.SQL(", ").join(
        sql.SQL("{} = source.{}").format(
            sql.Identifier(c),
            sql.Identifier(c),
        )
        for c in updatable
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
    metadata = get_table_metadata(table_name)
    if not metadata:
        raise ValueError(f"Table metadata not found: {table_name}")

    stg_schema = Schema.STAGING.value
    ods_schema = Schema.ODS.value
    table = table_name.lower()

    primary_keys: List[str] = list(metadata.get("primary_keys") or [])


    with conn.cursor() as cur:
            _ensure_ods_schema_and_table(cur, stg_schema, table, ods_schema, table)

            # ------------------------------------------------------------------
            # FULL_RESET
            # ------------------------------------------------------------------
            if load_mode == LoadMode.FULL_RESET.value:
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {}.{} CASCADE")
                    .format(sql.Identifier(ods_schema), sql.Identifier(table))
                )

                cur.execute(
                    sql.SQL("""
                        CREATE TABLE {}.{} (
                            LIKE {}.{} INCLUDING ALL
                        )
                    """).format(
                        sql.Identifier(ods_schema),
                        sql.Identifier(table),
                        sql.Identifier(stg_schema),
                        sql.Identifier(table),
                    )
                )

                cur.execute(
                    sql.SQL("INSERT INTO {}.{} SELECT * FROM {}.{}")
                    .format(
                        sql.Identifier(ods_schema),
                        sql.Identifier(table),
                        sql.Identifier(stg_schema),
                        sql.Identifier(table),
                    )
                )

                rows = cur.rowcount
                cols = _get_columns(cur, ods_schema, table)

                try:
                    _create_pk_and_indexes(cur, ods_schema, table, primary_keys, cols)
                except Exception as e:
                    logger.warning("Index/PK creation failed", error=str(e))

                logger.info("ODS FULL_RESET done", table=table_name, rows=rows)
                return rows

            # ------------------------------------------------------------------
            # FULL / INCREMENTAL
            # ------------------------------------------------------------------
            if not primary_keys:
                raise ValueError(f"No primary keys for merge: {table_name}")

            common_cols = _common_columns(cur, stg_schema, table, ods_schema, table)
            if not common_cols:
                raise ValueError(f"No common columns for merge: {table_name}")

            has_valid_from = "_etl_valid_from" in common_cols
            has_hashdiff = "_etl_hashdiff" in common_cols

            distinct_on = _dedup_distinct_on(primary_keys)
            order_by = _dedup_order_by(primary_keys, has_valid_from)
            set_clause = _update_set_clause(primary_keys, common_cols)
            join_cond = _pk_join("target", "source", primary_keys)

            rows_updated = rows_inserted = rows_deleted = 0

            # UPDATE modified
            if has_hashdiff:
                cur.execute(
                    sql.SQL("""
                        WITH source AS (
                            SELECT DISTINCT ON ({distinct_on}) {cols}
                            FROM {stg_schema}.{table}
                            ORDER BY {order_by}
                        )
                        UPDATE {ods_schema}.{table} AS target
                        SET {set_clause}
                        FROM source
                        WHERE {join_cond}
                          AND target."_etl_hashdiff"
                              IS DISTINCT FROM source."_etl_hashdiff"
                    """).format(
                        distinct_on=distinct_on,
                        cols=sql.SQL(", ").join(sql.Identifier(c) for c in common_cols),
                        stg_schema=sql.Identifier(stg_schema),
                        table=sql.Identifier(table),
                        order_by=order_by,
                        ods_schema=sql.Identifier(ods_schema),
                        set_clause=set_clause,
                        join_cond=join_cond,
                    )
                )
                rows_updated = cur.rowcount

            # INSERT new
            cur.execute(
                sql.SQL("""
                    WITH source AS (
                        SELECT DISTINCT ON ({distinct_on}) {cols}
                        FROM {stg_schema}.{table}
                        ORDER BY {order_by}
                    )
                    INSERT INTO {ods_schema}.{table} ({cols})
                    SELECT {cols}
                    FROM source
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM {ods_schema}.{table} AS target
                        WHERE {join_cond}
                    )
                """).format(
                    distinct_on=distinct_on,
                    cols=sql.SQL(", ").join(sql.Identifier(c) for c in common_cols),
                    stg_schema=sql.Identifier(stg_schema),
                    table=sql.Identifier(table),
                    order_by=order_by,
                    ods_schema=sql.Identifier(ods_schema),
                    join_cond=_pk_join("target", "source", primary_keys),
                )
            )
            rows_inserted = cur.rowcount

            # DELETE missing (FULL only)
            if load_mode == LoadMode.FULL.value:
                cur.execute(
                    sql.SQL("""
                        WITH source AS (
                            SELECT DISTINCT ON ({distinct_on}) {pk_cols}
                            FROM {stg_schema}.{table}
                            ORDER BY {order_by}
                        )
                        DELETE FROM {ods_schema}.{table} AS target
                        WHERE NOT EXISTS (
                            SELECT 1 FROM source
                            WHERE {join_cond}
                        )
                    """).format(
                        distinct_on=distinct_on,
                        pk_cols=sql.SQL(", ").join(sql.Identifier(pk) for pk in primary_keys),
                        stg_schema=sql.Identifier(stg_schema),
                        table=sql.Identifier(table),
                        order_by=order_by,
                        ods_schema=sql.Identifier(ods_schema),
                        join_cond=_pk_join("target", "source", primary_keys),
                    )
                )
                rows_deleted = cur.rowcount

            total = rows_updated + rows_inserted + rows_deleted

            logger.info(
                "ODS merge completed",
                table=table_name,
                mode=load_mode,
                updated=rows_updated,
                inserted=rows_inserted,
                deleted=rows_deleted,
                total=total,
                run_id=run_id,
            )

            return total
