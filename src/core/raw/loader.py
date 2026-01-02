"""
============================================================================
RAW Loader - v6.2 FINAL (DAGSTER-SAFE)
============================================================================
- Lecture parquet par row_group
- Nettoyage UTF-8 MINIMAL et UNIQUEMENT par chunk
- COPY CSV UTF8
- SAVEPOINT en fallback
- Aucun traitement CPU massif hors chunk
============================================================================
"""

from pathlib import Path
from io import StringIO
from datetime import datetime
from typing import Optional

import pyarrow.parquet as pq
import psycopg2
import pandas as pd

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
    Charger parquet dans RAW - v6.2 FINAL
    """
    settings = get_settings()
    file_name = parquet_path.name

    physical_name = (config_name or table_name).lower()
    raw_table_name = f"raw_{physical_name}"
    full_table = f"{Schema.RAW.value}.{raw_table_name}"

    logger.info(
        "Loading parquet to RAW (v6.2 FINAL)",
        table_name=table_name,
        physical_name=physical_name,
        target_table=full_table,
        file=file_name,
    )

    conn = psycopg2.connect(settings.postgres_url)

    try:
        with conn.cursor() as cur:
            # ============================================================
            # 0. TABLE D'ERREURS
            # ============================================================
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {Schema.RAW.value}.raw_load_errors (
                    error_id SERIAL PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    source_file TEXT NOT NULL,
                    sftp_log_id INTEGER,
                    chunk_number INTEGER,
                    row_number_in_chunk INTEGER,
                    error_message TEXT,
                    row_data_sample TEXT,
                    error_timestamp TIMESTAMP DEFAULT NOW()
                )
            """)
            conn.commit()

            # ============================================================
            # 1. DROP + CREATE TABLE RAW
            # ============================================================
            cur.execute(f"DROP TABLE IF EXISTS {full_table} CASCADE")
            conn.commit()

            parquet_file = pq.ParquetFile(parquet_path)
            total_rows = parquet_file.metadata.num_rows

            if total_rows == 0:
                logger.warning("Empty parquet file", table=table_name)
                update_sftp_file_status(conn, log_id, ProcessingStatus.COMPLETED, 0)
                conn.commit()
                return 0

            parquet_columns = parquet_file.schema_arrow.names

            logger.info(
                f"Parquet: {total_rows:,} rows × {len(parquet_columns)} columns"
            )

            columns_def = [f'"{col}" TEXT' for col in parquet_columns]
            columns_def.extend([
                '"_loaded_at" TIMESTAMP DEFAULT NOW()',
                '"_source_file" TEXT',
                '"_sftp_log_id" INTEGER',
            ])

            cur.execute(
                f"CREATE TABLE {full_table} ({', '.join(columns_def)})"
            )
            conn.commit()

            # ============================================================
            # 2. LOAD CONFIG
            # ============================================================
            chunk_size = 50_000
            loaded_at = datetime.now().isoformat()

            total_loaded = 0
            total_errors = 0

            copy_sql = f"""
                COPY {full_table}
                FROM STDIN
                WITH (
                    FORMAT CSV,
                    DELIMITER E'\\t',
                    NULL '\\N',
                    QUOTE '"',
                    ESCAPE '"',
                    ENCODING 'UTF8'
                )
            """

            # ============================================================
            # 3. PAR ROW_GROUP
            # ============================================================
            for rg_idx in range(parquet_file.num_row_groups):
                table_rg = parquet_file.read_row_group(rg_idx)

                df_rg = table_rg.to_pandas(
                    safe=False,
                    zero_copy_only=False,
                )

                # ========================================================
                # 4. CHUNKING (NETTOYAGE ICI, PAS AVANT)
                # ========================================================
                for chunk_start in range(0, len(df_rg), chunk_size):
                    df_chunk = df_rg.iloc[
                        chunk_start:chunk_start + chunk_size
                    ].copy()
                    chunk_rows = len(df_chunk)

                    # ====================================================
                    # UTF-8 CLEAN MINIMAL (SAFE & RAPIDE)
                    # ====================================================
                    text_cols = df_chunk.select_dtypes(include=["object"]).columns

                    for col in text_cols:
                        s = df_chunk[col]

                        # bytes -> str si nécessaire
                        s = s.apply(
                            lambda x: x.decode("utf-8", "ignore")
                            if isinstance(x, bytes)
                            else x
                        )

                        # suppression des caractères interdits Postgres
                        s = (
                            s.astype(str)
                             .str.replace(
                                 r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]",
                                 "",
                                 regex=True,
                             )
                        )

                        df_chunk[col] = s

                    df_chunk = df_chunk.replace(
                        ['None', '<NA>', 'nan', 'NaT'],
                        ''
                    )

                    # ====================================================
                    # ETL COLS
                    # ====================================================
                    etl_cols = pd.DataFrame(
                        {
                            '_loaded_at': [loaded_at] * chunk_rows,
                            '_source_file': [file_name] * chunk_rows,
                            '_sftp_log_id': [log_id] * chunk_rows,
                        },
                        index=df_chunk.index,
                    )

                    df_chunk = pd.concat([df_chunk, etl_cols], axis=1)

                    # ====================================================
                    # COPY
                    # ====================================================
                    csv_buffer = StringIO()
                    df_chunk.to_csv(
                        csv_buffer,
                        sep='\t',
                        header=False,
                        index=False,
                        na_rep='\\N',
                        quoting=1,
                    )
                    csv_buffer.seek(0)

                    try:
                        cur.copy_expert(copy_sql, csv_buffer)
                        total_loaded += chunk_rows
                        conn.commit()

                    except Exception as chunk_error:
                        conn.rollback()
                        logger.warning(
                            f"Chunk failed → row-by-row SAVEPOINT: "
                            f"{str(chunk_error)[:120]}"
                        )

                        for row_idx, row in enumerate(
                            df_chunk.itertuples(index=False)
                        ):
                            sp = f"sp_{rg_idx}_{chunk_start}_{row_idx}"
                            cur.execute(f"SAVEPOINT {sp}")

                            try:
                                row_csv = StringIO()
                                pd.DataFrame([row]).to_csv(
                                    row_csv,
                                    sep='\t',
                                    header=False,
                                    index=False,
                                    na_rep='\\N',
                                    quoting=1,
                                )
                                row_csv.seek(0)

                                cur.copy_expert(copy_sql, row_csv)
                                cur.execute(f"RELEASE SAVEPOINT {sp}")
                                total_loaded += 1

                            except Exception as row_error:
                                cur.execute(f"ROLLBACK TO SAVEPOINT {sp}")
                                cur.execute(f"RELEASE SAVEPOINT {sp}")
                                total_errors += 1

                                cur.execute(
                                    f"""
                                    INSERT INTO {Schema.RAW.value}.raw_load_errors
                                    (table_name, source_file, sftp_log_id,
                                     chunk_number, row_number_in_chunk,
                                     error_message, row_data_sample)
                                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                                    """,
                                    (
                                        table_name,
                                        file_name,
                                        log_id,
                                        rg_idx + 1,
                                        row_idx,
                                        str(row_error)[:500],
                                        str(row)[:200],
                                    ),
                                )

                        conn.commit()

            # ============================================================
            # 5. FINAL STATUS
            # ============================================================
            update_sftp_file_status(
                conn,
                log_id,
                ProcessingStatus.COMPLETED
                if total_loaded > 0
                else ProcessingStatus.FAILED,
                total_loaded,
            )
            conn.commit()

            logger.info(
                "RAW load completed",
                rows_loaded=total_loaded,
                rows_failed=total_errors,
            )

            return total_loaded

    except Exception as e:
        conn.rollback()
        logger.error("RAW load FATAL", table=table_name, error=str(e))
        update_sftp_file_status(conn, log_id, ProcessingStatus.FAILED, 0)
        conn.commit()
        raise

    finally:
        conn.close()
