"""
============================================================================
Dagster I/O Manager - PostgreSQL
============================================================================
"""

import psycopg2
from dagster import IOManager, io_manager, InputContext, OutputContext
from .config import etl_config_resource


class PostgresIOManager(IOManager):
    """I/O Manager pour logging dans PostgreSQL"""
    
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
    
    def _get_connection(self):
        return psycopg2.connect(self.connection_string)
    
    def handle_output(self, context: OutputContext, obj):
        """Sauvegarder metadata des assets"""
        if isinstance(obj, dict):
            self._log_asset_metadata(context, obj)
    
    def load_input(self, context: InputContext):
        """Load input - retourne None par défaut"""
        return None
    
    def _log_asset_metadata(self, context: OutputContext, metadata: dict):
        """Logger dans etl_logs.dagster_asset_runs"""
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS etl_logs;
                
                CREATE TABLE IF NOT EXISTS etl_logs.dagster_asset_runs (
                    run_id TEXT,
                    asset_key TEXT,
                    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    metadata JSONB,
                    PRIMARY KEY (run_id, asset_key)
                );
            """)
            
            import json
            cur.execute("""
                INSERT INTO etl_logs.dagster_asset_runs (run_id, asset_key, metadata)
                VALUES (%s, %s, %s::jsonb)
                ON CONFLICT (run_id, asset_key) 
                DO UPDATE SET metadata = EXCLUDED.metadata
            """, (
                context.run_id,
                context.asset_key.to_user_string(),
                json.dumps(metadata)
            ))
            
            conn.commit()
            cur.close()
            conn.close()
            
        except Exception as e:
            context.log.error(f"Error logging metadata: {e}")


@io_manager
def postgres_io_manager():
    """Factory pour PostgresIOManager"""
    return PostgresIOManager(etl_config_resource.get_connection_string())