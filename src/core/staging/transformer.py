
"""
============================================================================
Staging Transformer - Transformation RAW → STAGING
============================================================================
"""
from src.config.constants import LoadMode, Schema
from src.core.staging.extent import build_select_with_extent, get_extent_columns
from src.core.staging.hashdiff import build_hashdiff_expression
from src.db.connection import get_connection
from src.db.metadata import get_table_metadata
from src.utils.logging import get_logger

logger = get_logger(__name__)


def create_staging_table(table_name: str, load_mode: str) -> None:
    """
    Créer ou recréer la table STAGING
    
    - FULL_RESET: DROP + CREATE
    - FULL/INCREMENTAL: CREATE IF NOT EXISTS
    """
    metadata = get_table_metadata(table_name)
    if not metadata:
        raise ValueError(f"Table metadata not found: {table_name}")
    
    staging_table = f"{Schema.STAGING.value}.{table_name.lower()}"
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Créer le schéma STAGING
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {Schema.STAGING.value}")
            
            if load_mode == LoadMode.FULL_RESET.value:
                # DROP CASCADE pour FULL_RESET
                cur.execute(f"DROP TABLE IF EXISTS {staging_table} CASCADE")
                logger.info("Staging table dropped (FULL_RESET)", table=table_name)
            
            # CREATE TABLE avec colonnes éclatées
            extent_cols = get_extent_columns(metadata["columns"])
            
            create_cols = []
            for col in metadata["columns"]:
                col_name = col["column_name"]
                
                if col_name in extent_cols:
                    # Colonnes éclatées
                    extent = extent_cols[col_name]
                    for i in range(1, extent + 1):
                        create_cols.append(f'"{col_name}_{i}" TEXT')
                else:
                    # Colonne normale (tout en TEXT au départ)
                    create_cols.append(f'"{col_name}" TEXT')
            
            # Colonnes ETL
            create_cols.extend([
                '"_etl_hashdiff" VARCHAR(32)',
                '"_etl_run_id" VARCHAR(100)',
                '"_etl_valid_from" TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
            ])
            
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {staging_table} (
                    {', '.join(create_cols)}
                )
            """
            
            cur.execute(create_sql)
            logger.info("Staging table ready", table=table_name, mode=load_mode)


def load_raw_to_staging(
    table_name: str,
    run_id: str,
    load_mode: str,
) -> int:
    """
    Charger RAW → STAGING avec extent + hashdiff + déduplication
    
    Returns:
        Nombre de lignes chargées
    """
    metadata = get_table_metadata(table_name)
    if not metadata:
        raise ValueError(f"Table metadata not found: {table_name}")
    
    raw_table = f"{Schema.RAW.value}.raw_{table_name.lower()}"
    staging_table = f"{Schema.STAGING.value}.{table_name.lower()}"
    
    # Construire SELECT avec extent
    select_expr = build_select_with_extent(metadata["columns"], "src")
    
    # Construire hashdiff
    hashdiff_expr = build_hashdiff_expression(metadata["columns"])
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            if load_mode == LoadMode.FULL_RESET.value:
                # FULL_RESET: Vider la table
                cur.execute(f"TRUNCATE TABLE {staging_table}")
            
            # INSERT avec déduplication par hashdiff
            insert_sql = f"""
                INSERT INTO {staging_table}
                SELECT DISTINCT ON (_etl_hashdiff)
                    {select_expr},
                    {hashdiff_expr} AS "_etl_hashdiff",
                    %s AS "_etl_run_id",
                    CURRENT_TIMESTAMP AS "_etl_valid_from"
                FROM {raw_table} src
                ORDER BY _etl_hashdiff
            """
            
            cur.execute(insert_sql, (run_id,))
            rows_inserted = cur.rowcount
            
            logger.info(
                "RAW to STAGING loaded",
                table=table_name,
                rows=rows_inserted,
                mode=load_mode,
            )
            
            return rows_inserted
