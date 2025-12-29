"""
============================================================================
RAW Loader - Version thread-safe avec connexion propre
============================================================================
"""

from pathlib import Path
from io import StringIO
from datetime import datetime

import pyarrow.parquet as pq
import psycopg2

from src.config.constants import ProcessingStatus, Schema
from src.config.settings import get_settings
from src.db.monitoring import update_sftp_file_status
from src.utils.logging import get_logger

logger = get_logger(__name__)


def _pandas_to_postgres_type(dtype) -> str:
    """Mapper pandas dtype → PostgreSQL type"""
    dtype_str = str(dtype)
    
    if 'int' in dtype_str:
        return 'BIGINT'
    elif 'float' in dtype_str:
        return 'DOUBLE PRECISION'
    elif 'bool' in dtype_str:
        return 'BOOLEAN'
    elif 'datetime' in dtype_str:
        return 'TIMESTAMP'
    else:
        return 'TEXT'


def load_parquet_to_raw(
    parquet_path: Path,
    table_name: str,
    log_id: int,
    conn=None,  # Ignoré, on crée notre propre connexion
) -> int:
    """
    Charger parquet dans RAW (thread-safe avec connexion propre).
    """
    settings = get_settings()
    file_name = parquet_path.name
    raw_table_name = f"raw_{table_name.lower()}"
    full_table = f"{Schema.RAW.value}.{raw_table_name}"

    logger.info("Loading parquet to RAW", table=table_name, file=file_name)

    # CHAQUE THREAD CRÉE SA PROPRE CONNEXION
    conn = psycopg2.connect(settings.postgres_url)
    
    try:
        with conn.cursor() as cur:
            # DROP
            cur.execute(f"DROP TABLE IF EXISTS {full_table} CASCADE")
            
            # Lire parquet
            parquet_file = pq.ParquetFile(parquet_path)
            
            if parquet_file.num_row_groups == 0 or parquet_file.metadata.num_rows == 0:
                logger.warning(f"Fichier vide", table=table_name)
                conn.commit()
                conn.close()
                # Update monitoring avec NOUVELLE connexion
                conn2 = psycopg2.connect(settings.postgres_url)
                update_sftp_file_status(log_id, ProcessingStatus.COMPLETED, 0, conn=conn2)
                conn2.close()
                return 0
            
            # Sample
            df_sample = parquet_file.read_row_group(0, columns=parquet_file.schema.names).to_pandas().head(1)
            df_sample["_loaded_at"] = datetime.now()
            df_sample["_source_file"] = file_name
            df_sample["_sftp_log_id"] = log_id
            
            # CREATE TABLE
            columns = []
            for col_name, dtype in df_sample.dtypes.items():
                pg_type = _pandas_to_postgres_type(dtype)
                columns.append(f'"{col_name}" {pg_type}')
            
            create_sql = f"CREATE TABLE {full_table} ({', '.join(columns)})"
            cur.execute(create_sql)
            
            # COPY
            col_list = ",".join([f'"{c}"' for c in df_sample.columns])
            copy_sql = f"COPY {full_table} ({col_list}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')"
            
            total_rows = 0
            for batch in parquet_file.iter_batches(batch_size=50000):
                chunk = batch.to_pandas()
                chunk["_loaded_at"] = datetime.now()
                chunk["_source_file"] = file_name
                chunk["_sftp_log_id"] = log_id
                
                output = StringIO()
                chunk.to_csv(output, sep="\t", header=False, index=False, na_rep="\\N")
                output.seek(0)
                
                cur.copy_expert(copy_sql, output)
                total_rows += len(chunk)
            
            # ANALYZE
            cur.execute(f"ANALYZE {full_table}")
            
        conn.commit()
        conn.close()
        
        # Update monitoring avec NOUVELLE connexion
        conn2 = psycopg2.connect(settings.postgres_url)
        update_sftp_file_status(log_id, ProcessingStatus.COMPLETED, total_rows, conn=conn2)
        conn2.close()
        
        logger.info("RAW loaded", table=table_name, rows=total_rows)
        return total_rows

    except Exception as e:
        conn.rollback()
        conn.close()
        logger.error("RAW load failed", table=table_name, error=str(e))
        
        # Update monitoring avec NOUVELLE connexion
        conn2 = psycopg2.connect(settings.postgres_url)
        update_sftp_file_status(log_id, ProcessingStatus.FAILED, error_message=str(e), conn=conn2)
        conn2.close()
        
        raise