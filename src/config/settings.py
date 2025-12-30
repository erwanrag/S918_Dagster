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
    """
    Configuration centralisée de l'application ETL.
    
    Charge automatiquement depuis .env et variables d'environnement.
    Validation automatique des types et valeurs.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",  # Ignorer variables d'env non définies
    )

    # =========================================================================
    # PostgreSQL
    # =========================================================================
    pg_host: str = Field(default="localhost", alias="ETL_PG_HOST")
    pg_port: int = Field(default=5432, alias="ETL_PG_PORT")
    pg_database: str = Field(default="etl_db", alias="ETL_PG_DATABASE")
    pg_user: str = Field(default="postgres", alias="ETL_PG_USER")
    pg_password: str = Field(alias="ETL_PG_PASSWORD")
    pg_pool_size: int = Field(default=5, alias="ETL_PG_POOL_SIZE")
    pg_pool_timeout: int = Field(default=30, alias="ETL_PG_POOL_TIMEOUT")

    # =========================================================================
    # SFTP
    # =========================================================================
    sftp_root: Path = Field(
        default=Path("/data/sftp_cbmdata01"), 
        alias="ETL_SFTP_ROOT"
    )
    sftp_parallel_workers: int = Field(default=4, alias="ETL_SFTP_PARALLEL_WORKERS")

    # =========================================================================
    # Dagster
    # =========================================================================
    dagster_home: Path = Field(
        default=Path("/data/dagster/dagster_home"), 
        alias="DAGSTER_HOME"
    )
    max_concurrent_runs: int = Field(default=10, alias="DAGSTER_MAX_CONCURRENT_RUNS")
    compute_log_retention_days: int = Field(
        default=30, 
        alias="DAGSTER_COMPUTE_LOG_RETENTION_DAYS"
    )

    # =========================================================================
    # dbt
    # =========================================================================
    dbt_project_dir: Path = Field(
        default=Path("/data/dbt/etl_db"),
        alias="ETL_DBT_PROJECT"
    )
    dbt_threads: int = Field(default=4, alias="DBT_THREADS")
    dbt_target: str = Field(default="prod", alias="DBT_TARGET")

    # =========================================================================
    # Alerting
    # =========================================================================
    teams_webhook_url: Optional[str] = Field(default=None, alias="TEAMS_WEBHOOK_URL")
    smtp_host: str = Field(default="localhost", alias="SMTP_HOST")
    smtp_port: int = Field(default=25, alias="SMTP_PORT")
    smtp_user: Optional[str] = Field(default=None, alias="SMTP_USER")
    smtp_password: Optional[str] = Field(default=None, alias="SMTP_PASSWORD")
    alert_from_email: str = Field(
        default="etl@cbm.local", 
        alias="ALERT_FROM_EMAIL"
    )
    alert_to_emails_raw: str = Field(
        default="data-team@cbm.local",
        alias="ALERT_TO_EMAILS"
    )
    email_alerts_enabled: bool = Field(default=False, alias="EMAIL_ALERTS_ENABLED")

    # =========================================================================
    # Logging
    # =========================================================================
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_format: str = Field(
        default="json",  # ou "console"
        alias="LOG_FORMAT"
    )

    # =========================================================================
    # Maintenance
    # =========================================================================
    etl_logs_retention_days: int = Field(
        default=90,
        alias="ETL_LOGS_RETENTION_DAYS"
    )
    sftp_monitoring_retention_days: int = Field(
        default=180,
        alias="SFTP_MONITORING_RETENTION_DAYS"
    )

    # =========================================================================
    # Computed Properties
    # =========================================================================

    @property
    def postgres_url(self) -> str:
        """URL de connexion PostgreSQL complète"""
        return (
            f"postgresql://{self.pg_user}:{self.pg_password}"
            f"@{self.pg_host}:{self.pg_port}/{self.pg_database}"
        )

    @property
    def sftp_parquet_dir(self) -> Path:
        """Répertoire des fichiers parquet entrants"""
        return self.sftp_root / "Incoming" / "data" / "parquet"

    @property
    def sftp_metadata_dir(self) -> Path:
        """Répertoire des fichiers metadata"""
        return self.sftp_root / "Incoming" / "data" / "metadata"

    @property
    def sftp_status_dir(self) -> Path:
        """Répertoire des fichiers status"""
        return self.sftp_root / "Incoming" / "data" / "status"

    @property
    def sftp_processed_dir(self) -> Path:
        """Répertoire des archives traitées"""
        return self.sftp_root / "Processed"

    @property
    def sftp_logs_dir(self) -> Path:
        """Répertoire des logs SFTP"""
        return self.sftp_root / "Logs"

    @property
    def alert_to_emails(self) -> list[str]:
        """Liste des emails pour alertes"""
        if not self.alert_to_emails_raw or self.alert_to_emails_raw.strip() == "":
            return []
        return [email.strip() for email in self.alert_to_emails_raw.split(",") if email.strip()]

    # =========================================================================
    # Validators
    # =========================================================================

    @field_validator("sftp_root", mode="before")
    @classmethod
    def validate_sftp_root(cls, v: str | Path) -> Path:
        """Valider que le répertoire SFTP existe"""
        path = Path(v)
        if not path.exists():
            raise ValueError(f"SFTP root directory does not exist: {path}")
        return path

    @field_validator("dbt_project_dir", mode="before")
    @classmethod
    def validate_dbt_project(cls, v: str | Path) -> Path:
        """Valider que le projet dbt existe"""
        path = Path(v)
        if not path.exists():
            raise ValueError(f"dbt project directory does not exist: {path}")
        # Vérifier présence dbt_project.yml
        if not (path / "dbt_project.yml").exists():
            raise ValueError(f"Invalid dbt project (no dbt_project.yml): {path}")
        return path

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Valider le niveau de log"""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"Invalid log level: {v}. Must be one of {valid_levels}")
        return v_upper


# =============================================================================
# Singleton Pattern
# =============================================================================

_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """
    Obtenir l'instance singleton des settings.
    
    Usage:
        from src.config.settings import get_settings
        settings = get_settings()
        print(settings.postgres_url)
    """
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reset_settings() -> None:
    """
    Réinitialiser le singleton (utile pour tests).
    
    Usage dans tests:
        from src.config.settings import reset_settings
        reset_settings()
        # Charger nouveau .env
        settings = get_settings()
    """
    global _settings
    _settings = None