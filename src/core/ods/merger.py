"""
============================================================================
ODS Merger - Merge STAGING → ODS avec SCD Type 2
============================================================================
"""

from src.config.constants import LoadMode, Schema
from src.db.connection import get_connection
from src.db.metadata import get_table_metadata
from src.utils.logging import get_logger

logger = get_logger(__name__)


def merge_staging_to_ods(
    table_name: str,
    run_id: str,
    load_mode: str,
) -> int:
    """
    Merge STAGING → ODS avec gestion SCD Type 2

    Logic:
    - FULL_RESET: TRUNCATE + INSERT
    - FULL: Fermer anciennes versions + INSERT nouvelles
    - INCREMENTAL: Merge sur clés primaires

    Returns:
        Nombre de lignes affectées
    """
    metadata = get_table_metadata(table_name)
    if not metadata:
        raise ValueError(f"Table metadata not found: {table_name}")

    staging_table = f"{Schema.STAGING.value}.{table_name.lower()}"
    ods_table = f"{Schema.ODS.value}.{table_name.lower()}"
    primary_keys = metadata["primary_keys"]

    with get_connection() as conn:
        with conn.cursor() as cur:
            # Créer schéma ODS
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {Schema.ODS.value}")

            # Créer table ODS si n'existe pas (même structure que STAGING)
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {ods_table} (
                    LIKE {staging_table} INCLUDING ALL
                )
            """
            )

            if load_mode == LoadMode.FULL_RESET.value:
                # FULL_RESET: Tout effacer et recharger
                cur.execute(f"TRUNCATE TABLE {ods_table}")
                cur.execute(
                    f"""
                    INSERT INTO {ods_table}
                    SELECT * FROM {staging_table}
                """
                )
                rows_affected = cur.rowcount

            elif load_mode == LoadMode.FULL.value:
                # FULL: Fermer anciennes versions + INSERT nouvelles
                cur.execute(
                    f"""
                    UPDATE {ods_table}
                    SET "_etl_valid_to" = CURRENT_TIMESTAMP
                    WHERE "_etl_valid_to" IS NULL
                """
                )

                cur.execute(
                    f"""
                    INSERT INTO {ods_table}
                    SELECT * FROM {staging_table}
                """
                )
                rows_affected = cur.rowcount

            else:  # INCREMENTAL
                # Merge sur clés primaires
                if not primary_keys:
                    raise ValueError(
                        f"No primary keys defined for INCREMENTAL: {table_name}"
                    )

                pk_join = " AND ".join(
                    [f'ods."{pk}" = stg."{pk}"' for pk in primary_keys]
                )

                # Fermer les versions modifiées
                cur.execute(
                    f"""
                    UPDATE {ods_table} ods
                    SET "_etl_valid_to" = CURRENT_TIMESTAMP
                    FROM {staging_table} stg
                    WHERE {pk_join}
                      AND ods."_etl_hashdiff" != stg."_etl_hashdiff"
                      AND ods."_etl_valid_to" IS NULL
                """
                )

                # Insérer nouvelles versions
                cur.execute(
                    f"""
                    INSERT INTO {ods_table}
                    SELECT stg.*
                    FROM {staging_table} stg
                    LEFT JOIN {ods_table} ods ON {pk_join}
                        AND ods."_etl_valid_to" IS NULL
                    WHERE ods."{primary_keys[0]}" IS NULL
                       OR ods."_etl_hashdiff" != stg."_etl_hashdiff"
                """
                )
                rows_affected = cur.rowcount

            logger.info(
                "STAGING to ODS merged",
                table=table_name,
                rows=rows_affected,
                mode=load_mode,
            )

            return rows_affected
