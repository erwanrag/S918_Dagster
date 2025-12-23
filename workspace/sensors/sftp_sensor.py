"""
============================================================================
Dagster Sensor - SFTP File Detection
============================================================================
Détecte automatiquement les nouveaux fichiers parquet dans SFTP
et déclenche le pipeline complet
"""

import sys
sys.path.insert(0, '/data/prefect/projects')

from dagster import (
    sensor,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    DefaultSensorStatus,
)
from pathlib import Path
from datetime import datetime
from typing import List

from shared.config import sftp_config
from ..jobs.full_pipeline import full_pipeline_job


@sensor(
    name="sftp_file_sensor",
    job=full_pipeline_job,
    minimum_interval_seconds=60,  # Check toutes les 60 secondes
    description="Détecte nouveaux fichiers parquet dans SFTP et déclenche pipeline",
    default_status=DefaultSensorStatus.STOPPED  # Démarrer manuellement
)
def sftp_file_sensor(context: SensorEvaluationContext):
    """
    Sensor: Détecter nouveaux fichiers SFTP
    
    Vérifie le répertoire SFTP toutes les 60 secondes.
    Si nouveaux fichiers détectés → déclenche full_pipeline_job
    
    Équivalent Prefect: Scheduler automatique dans serve_scheduler.py
    """
    parquet_dir = Path(sftp_config.sftp_parquet_dir)
    
    if not parquet_dir.exists():
        return SkipReason(f"SFTP directory does not exist: {parquet_dir}")
    
    # Scanner fichiers parquet
    parquet_files = list(parquet_dir.glob("*.parquet"))
    
    if not parquet_files:
        return SkipReason("No parquet files found in SFTP")
    
    # Récupérer cursor (dernière exécution)
    last_run_time = context.cursor or "1970-01-01T00:00:00"
    last_run_dt = datetime.fromisoformat(last_run_time)
    
    # Filtrer fichiers modifiés depuis dernière exécution
    new_files = []
    latest_mtime = last_run_dt
    
    for f in parquet_files:
        mtime = datetime.fromtimestamp(f.stat().st_mtime)
        if mtime > last_run_dt:
            new_files.append(f)
            if mtime > latest_mtime:
                latest_mtime = mtime
    
    if not new_files:
        return SkipReason(
            f"No new files since {last_run_time} "
            f"(checked {len(parquet_files)} files)"
        )
    
    # Déclencher pipeline
    context.log.info(f"Found {len(new_files)} new file(s)")
    for f in new_files:
        context.log.info(f"  - {f.name}")
    
    # Mettre à jour cursor
    context.update_cursor(latest_mtime.isoformat())
    
    # Créer run request
    yield RunRequest(
        run_key=f"sftp_{latest_mtime.strftime('%Y%m%d_%H%M%S')}",
        run_config={},
        tags={
            "source": "sftp_sensor",
            "new_files_count": str(len(new_files)),
            "detected_at": latest_mtime.isoformat()
        }
    )


# ============================================================================
# SENSOR ALTERNATIF: Polling Horaire (si sensor précédent trop fréquent)
# ============================================================================

@sensor(
    name="sftp_hourly_sensor",
    job=full_pipeline_job,
    minimum_interval_seconds=3600,  # Check toutes les heures
    description="Déclenche pipeline ETL toutes les heures (mode polling)",
    default_status=DefaultSensorStatus.STOPPED
)
def sftp_hourly_sensor(context: SensorEvaluationContext):
    """
    Sensor alternatif: Polling horaire simple
    
    Déclenche le pipeline toutes les heures si fichiers présents
    Plus simple que sensor avec cursor
    """
    parquet_dir = Path(sftp_config.sftp_parquet_dir)
    
    if not parquet_dir.exists():
        return SkipReason(f"SFTP directory does not exist")
    
    parquet_files = list(parquet_dir.glob("*.parquet"))
    
    if not parquet_files:
        return SkipReason("No parquet files found")
    
    context.log.info(f"Found {len(parquet_files)} file(s) to process")
    
    # Déclencher pipeline
    yield RunRequest(
        run_key=f"hourly_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        run_config={},
        tags={
            "source": "sftp_hourly_sensor",
            "files_count": str(len(parquet_files))
        }
    )