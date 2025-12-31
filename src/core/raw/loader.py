"""
============================================================================
RAW Loader - Copie directe Parquet → RAW (TOUT EN TEXT)
============================================================================
CORRECTIONS :
1. Utilise l'ordre des colonnes du PARQUET (pas metadata.json)
2. Gestion robuste des guillemets et caractères spéciaux dans CSV
============================================================================
"""

from pathlib import Path
from io import StringIO
from datetime import datetime
from typing import Optional

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
    config_name: Optional[str],
    columns_metadata: list[dict],
    log_id: int,
    conn=None,
) -> int:
    """
    Charger parquet dans RAW - TOUT EN TEXT
    
    CORRECTIONS :
    - Utilise l'ordre du PARQUET (source de vérité)
    - Échappe correctement les guillemets et tabs dans les données
    - Gère les caractères spéciaux sans casser le COPY
    
    Args:
        parquet_path: Chemin du fichier parquet
        table_name: Nom de la table logique
        config_name: Nom config ou None
        columns_metadata: Métadonnées (pour enrichissement)
        log_id: ID du log monitoring
        conn: Connection (ignorée)
    
    Returns:
        Nombre de lignes chargées
    """
    settings = get_settings()
    file_name = parquet_path.name
    
    physical_name = (config_name or table_name).lower()
    raw_table_name = f"raw_{physical_name}"
    full_table = f"{Schema.RAW.value}.{raw_table_name}"

    logger.info(
        "Loading parquet to RAW",
        table_name=table_name,
        config_name=config_name,
        physical_name=physical_name,
        target_table=full_table,
        file=file_name
    )

    # Créer nouvelle connexion
    conn = psycopg2.connect(settings.postgres_url)
    
    try:
        with conn.cursor() as cur:
            # ============================================================
            # 1. DROP TABLE
            # ============================================================
            logger.debug(f"Dropping table {full_table}")
            cur.execute(f"DROP TABLE IF EXISTS {full_table} CASCADE")
            
            # ============================================================
            # 2. LIRE PARQUET et obtenir l'ORDRE RÉEL des colonnes
            # ============================================================
            logger.debug(f"Reading parquet file: {parquet_path}")
            parquet_file = pq.ParquetFile(parquet_path)
            
            if parquet_file.num_row_groups == 0 or parquet_file.metadata.num_rows == 0:
                logger.warning(f"Empty parquet file", table=table_name)
                update_sftp_file_status(conn, log_id, ProcessingStatus.COMPLETED, 0)
                conn.commit()
                conn.close()
                return 0
            
            # ✅ OBTENIR L'ORDRE DES COLONNES DU PARQUET
            parquet_schema = parquet_file.schema_arrow
            parquet_column_names = parquet_schema.names
            
            logger.debug(
                f"Parquet column order (first 10): {parquet_column_names[:10]}"
            )
            
            # ============================================================
            # 3. CREATE TABLE - DANS L'ORDRE DU PARQUET
            # ============================================================
            logger.debug("Building CREATE TABLE statement")
            
            columns_def = []
            
            # ✅ UTILISER L'ORDRE DU PARQUET
            for col_name in parquet_column_names:
                columns_def.append(f'"{col_name}" TEXT')
            
            # Colonnes ETL
            columns_def.extend([
                '"_loaded_at" TIMESTAMP DEFAULT NOW()',
                '"_source_file" TEXT',
                '"_sftp_log_id" INTEGER'
            ])
            
            create_sql = f"CREATE TABLE {full_table} ({', '.join(columns_def)})"
            
            logger.info(
                f"Creating RAW table: {raw_table_name} with {len(parquet_column_names)} columns (all TEXT)"
            )
            
            cur.execute(create_sql)
            
            # ============================================================
            # 4. CONVERTIR PARQUET EN CSV (AVEC ÉCHAPPEMENT ROBUSTE)
            # ============================================================
            logger.debug("Converting parquet to CSV with proper escaping")
            
            df = parquet_file.read().to_pandas()
            
            # ✅ ÉCHAPPEMENT ROBUSTE : Remplacer None/NA et échapper les caractères spéciaux
            for col in df.columns:
                # Convertir en string
                df[col] = df[col].astype(str)
                
                # Remplacer les valeurs nulles
                df[col] = df[col].replace(['None', '<NA>', 'nan', 'NaT'], '')
                
                # ✅ ÉCHAPPER LES CARACTÈRES SPÉCIAUX pour CSV
                # Remplacer tabs par espaces (pour éviter confusion avec délimiteur)
                df[col] = df[col].str.replace('\t', ' ', regex=False)
                
                # Remplacer newlines par espaces
                df[col] = df[col].str.replace('\n', ' ', regex=False)
                df[col] = df[col].str.replace('\r', ' ', regex=False)
                
                # ✅ ÉCHAPPER LES GUILLEMETS DOUBLES
                # En CSV, les guillemets sont échappés en les doublant
                df[col] = df[col].str.replace('"', '""', regex=False)
            
            # Colonnes ETL
            df['_loaded_at'] = datetime.now().isoformat()
            df['_source_file'] = file_name
            df['_sftp_log_id'] = log_id
            
            # ============================================================
            # 5. COPY VERS POSTGRESQL (FORMAT CSV AVEC QUOTE)
            # ============================================================
            logger.debug("Executing COPY with CSV format")
            
            csv_buffer = StringIO()
            
            # ✅ FORMAT CSV STANDARD avec QUOTE pour protéger les données
            df.to_csv(
                csv_buffer,
                index=False,
                header=False,
                sep=',',           # Virgule comme séparateur
                na_rep='',
                quoting=1,         # QUOTE_MINIMAL : quote seulement si nécessaire
                escapechar='\\',   # Backslash pour échapper
                doublequote=True   # Double-quote pour échapper les quotes
            )
            csv_buffer.seek(0)
            
            # ✅ COPY avec FORMAT CSV et QUOTE
            copy_sql = f"""
                COPY {full_table} FROM STDIN 
                WITH (
                    FORMAT CSV,
                    DELIMITER ',',
                    NULL '',
                    QUOTE '"',
                    ESCAPE '\\'
                )
            """
            
            cur.copy_expert(copy_sql, csv_buffer)
            rows_loaded = cur.rowcount
            
            # ============================================================
            # 6. UPDATE MONITORING
            # ============================================================
            update_sftp_file_status(conn, log_id, ProcessingStatus.COMPLETED, rows_loaded)
            conn.commit()
            
            logger.info(
                "RAW loaded successfully",
                table_name=table_name,
                physical_name=physical_name,
                rows=rows_loaded
            )
            
            return rows_loaded
    
    except Exception as e:
        conn.rollback()
        logger.error(
            "RAW load failed",
            table=table_name,
            error=str(e)
        )
        update_sftp_file_status(conn, log_id, ProcessingStatus.FAILED, 0)
        conn.commit()
        raise
    
    finally:
        conn.close()