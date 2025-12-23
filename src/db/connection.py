"""
============================================================================
Database Connection - Gestion pool connexions PostgreSQL
============================================================================
"""
from contextlib import contextmanager
from typing import Generator

import psycopg2
from psycopg2 import pool
from psycopg2.extensions import connection

from src.config.settings import get_settings
from src.utils.logging import get_logger

logger = get_logger(__name__)

# Pool global de connexions
_connection_pool: pool.SimpleConnectionPool | None = None


def get_connection_pool() -> pool.SimpleConnectionPool:
    """Obtenir le pool de connexions (singleton)"""
    global _connection_pool
    
    if _connection_pool is None:
        settings = get_settings()
        _connection_pool = pool.SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=settings.postgres_url,
        )
        logger.info("Connection pool created", min=1, max=10)
    
    return _connection_pool


@contextmanager
def get_connection() -> Generator[connection, None, None]:
    """Context manager pour obtenir une connexion du pool"""
    conn_pool = get_connection_pool()
    conn = conn_pool.getconn()
    
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error("Database error", error=str(e))
        raise
    finally:
        conn_pool.putconn(conn)


def close_connection_pool() -> None:
    """Fermer le pool de connexions"""
    global _connection_pool
    if _connection_pool is not None:
        _connection_pool.closeall()
        _connection_pool = None
        logger.info("Connection pool closed")