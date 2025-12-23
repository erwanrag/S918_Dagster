"""
============================================================================
Settings - Configuration centralisée avec Pydantic
============================================================================
"""

from pathlib import Path
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration de l'application ETL"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # PostgreSQL
    pg_host: str = Field(default="localhost", alias="ETL_PG_HOST")
    pg_port: int = Field(default=5432, alias="ETL_PG_PORT")
    pg_database: str = Field(default="etl_db", alias="ETL_PG_DATABASE")
    pg_user: str = Field(default="postgres", alias="ETL_PG_USER")
    pg_password: str = Field(alias="ETL_PG_PASSWORD")

    # SFTP
    sftp_root: Path = Field(default=Path("/data/sftp_cbmdata01"), alias="ETL_SFTP_ROOT")

    # Dagster
    dagster_home: Path = Field(
        default=Path("/data/dagster/dagster_home"), alias="DAGSTER_HOME"
    )

    # Logging
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")

    @property
    def postgres_url(self) -> str:
        """URL de connexion PostgreSQL"""
        return f"postgresql://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_database}"

    @property
    def sftp_parquet_dir(self) -> Path:
        """Répertoire des fichiers parquet"""
        return self.sftp_root / "Incoming" / "data" / "parquet"

    @field_validator("sftp_root", mode="before")
    @classmethod
    def validate_sftp_root(cls, v: str | Path) -> Path:
        """Valider que le répertoire SFTP existe"""
        path = Path(v)
        if not path.exists():
            raise ValueError(f"SFTP root directory does not exist: {path}")
        return path


# Singleton
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Obtenir l'instance singleton des settings"""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
