"""
============================================================================
Monitoring - Logs dans sftp_monitoring
============================================================================
"""

from pathlib import Path

from src.config.constants import ProcessingStatus, Schema
from src.db.connection import get_connection
from src.utils.logging import get_logger

logger = get_logger(__name__)


def log_sftp_file(
    file_path: Path,
    table_name: str,
    load_mode: str,
) -> int:
    """
    Logger un fichier SFTP dans sftp_monitoring.sftp_file_log

    Returns:
        log_id du fichier inséré
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {Schema.SFTP_MONITORING.value}.sftp_file_log (
                    file_name,
                    file_path,
                    table_name,
                    load_mode,
                    file_size_bytes,
                    processing_status,
                    detected_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                RETURNING log_id
            """,
                (
                    file_path.name,
                    str(file_path),
                    table_name,
                    load_mode,
                    file_path.stat().st_size,
                    ProcessingStatus.PENDING.value,
                ),
            )

            log_id = cur.fetchone()[0]
            logger.info("SFTP file logged", log_id=log_id, file=file_path.name)
            return log_id


def update_sftp_file_status(
    log_id: int,
    status: ProcessingStatus,
    row_count: int | None = None,
    error_message: str | None = None,
) -> None:
    """Mettre à jour le statut d'un fichier SFTP"""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {Schema.SFTP_MONITORING.value}.sftp_file_log
                SET 
                    processing_status = %s,
                    row_count = COALESCE(%s, row_count),
                    error_message = %s,
                    processed_at = CASE WHEN %s IN ('COMPLETED', 'FAILED') 
                                        THEN CURRENT_TIMESTAMP 
                                        ELSE processed_at 
                                   END
                WHERE log_id = %s
            """,
                (
                    status.value,
                    row_count,
                    error_message,
                    status.value,
                    log_id,
                ),
            )

            logger.info("SFTP file status updated", log_id=log_id, status=status.value)
