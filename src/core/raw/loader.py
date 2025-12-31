"""
============================================================================
RAW Loader - Copie directe Parquet → RAW (TOUT EN TEXT)
============================================================================
Version finale tenant compte de la réalité du Parquet :
- TOUT en TEXT dans RAW (pas de typage)
- Les colonnes EXTENT sont stockées comme TEXT avec séparateur ";"
- Pas d'éclatement en RAW, juste copie directe
- L'éclatement ET le typage se feront en STAGING
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


def load_parquet_to_raw(
    parquet_path: Path,
    table_name: str,
    columns_metadata: list[dict],
    log_id: int,
    conn=None,  # Ignoré, on crée notre propre connexion
) -> int:
    """
    Charger parquet dans RAW - TOUT EN TEXT
    
    IMPORTANT : TOUTES les colonnes sont en TEXT dans RAW (données brutes)
    Le typage strict se fera en STAGING
    
    Args:
        parquet_path: Chemin du fichier parquet
        table_name: Nom de la table (ex: "client")
        columns_metadata: Métadonnées colonnes enrichies depuis metadata.json
        log_id: ID du log monitoring
        conn: Connection (ignorée, on crée la nôtre)
    
    Returns:
        Nombre de lignes chargées
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
            # ============================================================
            # 1. DROP TABLE
            # ============================================================
            logger.debug(f"Dropping table {full_table}")
            cur.execute(f"DROP TABLE IF EXISTS {full_table} CASCADE")
            
            # ============================================================
            # 2. LIRE PARQUET
            # ============================================================
            logger.debug(f"Reading parquet file: {parquet_path}")
            parquet_file = pq.ParquetFile(parquet_path)
            
            if parquet_file.num_row_groups == 0 or parquet_file.metadata.num_rows == 0:
                logger.warning(f"Empty parquet file", table=table_name)
                update_sftp_file_status(conn, log_id, ProcessingStatus.COMPLETED, 0)
                conn.commit()
                conn.close()
                return 0
            
            # ============================================================
            # 3. CREATE TABLE - TOUT EN TEXT
            # ============================================================
            logger.debug("Building CREATE TABLE statement (all TEXT)")
            
            columns_def = []
            
            # TOUTES les colonnes en TEXT (RAW = données brutes)
            for col in columns_metadata:
                col_name = col["column_name"]
                columns_def.append(f'"{col_name}" TEXT')
            
            # Ajouter colonnes ETL
            columns_def.extend([
                '"_loaded_at" TIMESTAMP DEFAULT NOW()',
                '"_source_file" TEXT',
                '"_sftp_log_id" INTEGER'
            ])
            
            create_sql = f"CREATE TABLE {full_table} ({', '.join(columns_def)})"
            
            logger.info(
                f"Creating RAW table with {len(columns_metadata)} columns (all TEXT)"
            )
            
            logger.debug(f"Executing CREATE TABLE")
            cur.execute(create_sql)
            
            # ============================================================
            # 4. COPY DATA (streaming par chunks)
            # ============================================================
            # Construire liste colonnes pour COPY
            col_names = [col["column_name"] for col in columns_metadata]
            col_names.extend(["_loaded_at", "_source_file", "_sftp_log_id"])
            
            col_list = ",".join([f'"{c}"' for c in col_names])
            copy_sql = f"COPY {full_table} ({col_list}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')"
            
            logger.debug("Starting COPY operation")
            total_rows = 0
            chunk_size = 50000
            
            for batch in parquet_file.iter_batches(batch_size=chunk_size):
                chunk = batch.to_pandas()
                chunk["_loaded_at"] = datetime.now()
                chunk["_source_file"] = file_name
                chunk["_sftp_log_id"] = log_id
                
                output = StringIO()
                chunk.to_csv(output, sep="\t", header=False, index=False, na_rep="\\N")
                output.seek(0)
                
                cur.copy_expert(copy_sql, output)
                total_rows += len(chunk)
                
                if total_rows % 100000 == 0:
                    logger.debug(f"Loaded {total_rows:,} rows so far")
            
            # ============================================================
            # 5. ANALYZE TABLE
            # ============================================================
            logger.debug(f"Analyzing table {full_table}")
            cur.execute(f"ANALYZE {full_table}")
            
        conn.commit()
        
        logger.info(
            "RAW loaded successfully", 
            table=table_name, 
            rows=total_rows
        )
        
        # Update monitoring AVANT de fermer connexion
        update_sftp_file_status(conn, log_id, ProcessingStatus.COMPLETED, total_rows)
        
        conn.close()
        
        return total_rows

    except Exception as e:
        logger.error("RAW load failed", table=table_name, error=str(e))
        
        # Update monitoring AVANT rollback
        try:
            update_sftp_file_status(conn, log_id, ProcessingStatus.FAILED, error_message=str(e))
        except:
            pass  # Ignore si update monitoring échoue
        
        conn.rollback()
        conn.close()
        
        raise