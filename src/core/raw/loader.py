"""
============================================================================
RAW Loader - Chargement parquet → RAW
============================================================================
"""

from pathlib import Path

import pyarrow.parquet as pq
from sqlalchemy import create_engine

from src.config.constants import ProcessingStatus, Schema
from src.config.settings import get_settings
from src.db.monitoring import update_sftp_file_status
from src.utils.logging import get_logger

logger = get_logger(__name__)


def load_parquet_to_raw(
    parquet_path: Path,
    table_name: str,
    log_id: int,
    conn,
) -> int:
    """
    Charger un fichier parquet dans le schéma RAW

    Args:
        parquet_path: Chemin du fichier parquet
        table_name: Nom de la table
        log_id: ID du log dans sftp_monitoring

    Returns:
        Nombre de lignes chargées
    """
    settings = get_settings()
    raw_table = f"{Schema.RAW.value}.raw_{table_name.lower()}"

    logger.info("Loading parquet to RAW", table=table_name, file=parquet_path.name)

    try:
        # Lire le parquet
        table = pq.read_table(parquet_path)
        df = table.to_pandas()
        rows_count = len(df)

        # Créer le schéma RAW s'il n'existe pas

        with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {Schema.RAW.value}")
                cur.execute(f"DROP TABLE IF EXISTS {raw_table} CASCADE")

        # Charger avec pandas to_sql
        engine = create_engine(settings.postgres_url)
        df.to_sql(
            name=f"raw_{table_name.lower()}",
            con=engine,
            schema=Schema.RAW.value,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=10000,
        )
        engine.dispose()

        # Mettre à jour le monitoring
        update_sftp_file_status(log_id, ProcessingStatus.COMPLETED, rows_count)

        logger.info("Parquet loaded to RAW", table=table_name, rows=rows_count)
        return rows_count

    except Exception as e:
        logger.error("Failed to load parquet", table=table_name, error=str(e))
        update_sftp_file_status(log_id, ProcessingStatus.FAILED, error_message=str(e))
        raise
