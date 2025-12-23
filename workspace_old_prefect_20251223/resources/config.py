"""
============================================================================
Dagster Resource - Configuration ETL
============================================================================
"""

import sys
import os
from pathlib import Path
from dagster import ConfigurableResource

# Ajouter shared au path
sys.path.insert(0, '/data/dagster')
sys.path.insert(0, '/data/prefect/projects')

from shared.config import config as shared_config, sftp_config


class ETLConfig(ConfigurableResource):
    """Configuration ETL réutilisant shared.config"""
    
    def get_connection_string(self) -> str:
        """Retourne connection string PostgreSQL"""
        return shared_config.get_connection_string()
    
    @property
    def pg_host(self) -> str:
        return shared_config.host
    
    @property
    def pg_port(self) -> int:
        return shared_config.port
    
    @property
    def pg_database(self) -> str:
        return shared_config.database
    
    @property
    def pg_user(self) -> str:
        return shared_config.user
    
    @property
    def sftp_parquet_dir(self) -> Path:
        return Path(sftp_config.sftp_parquet_dir)
    
    @property
    def sftp_archive_dir(self) -> Path:
        return Path(sftp_config.sftp_archive_dir)
    
    @property
    def dbt_project_dir(self) -> Path:
        return Path(shared_config.dbt_project_dir)


# Instance singleton
etl_config_resource = ETLConfig()