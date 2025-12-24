# src/resources/postgres.py

from dagster import ConfigurableResource
import psycopg2
from contextlib import contextmanager


class PostgresResource(ConfigurableResource):
    dsn: str

    @contextmanager
    def get_connection(self):
        conn = psycopg2.connect(self.dsn)
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
