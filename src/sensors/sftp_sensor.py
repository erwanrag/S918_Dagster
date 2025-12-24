"""Sensors SFTP"""
from dagster import RunRequest, SensorEvaluationContext, sensor
from src.core.sftp.scanner import scan_parquet_files
from src.jobs.pipelines import ingestion_pipeline

@sensor(
    name="sftp_file_sensor",
    job=ingestion_pipeline,
    minimum_interval_seconds=60,
    description="Détecte fichiers SFTP",
)
def sftp_file_sensor(context: SensorEvaluationContext):
    """Sensor SFTP - Détection auto"""
    files = scan_parquet_files()
    
    if not files:
        context.log.info("No new files detected")
        return
    
    file_names = [f.path.name for f in files]
    context.log.info(f"Detected {len(files)} files: {', '.join(file_names)}")
    
    yield RunRequest(
        run_key=f"sftp_{len(files)}_files",
        tags={"source": "sftp_sensor", "file_count": str(len(files))},
    )

@sensor(
    name="sftp_hourly_sensor",
    job=ingestion_pipeline,
    minimum_interval_seconds=3600,
    description="Polling horaire",
)
def sftp_hourly_sensor(context: SensorEvaluationContext):
    """Sensor horaire - Backup"""
    files = scan_parquet_files()
    
    if files:
        context.log.info(f"Hourly check: {len(files)} files")
        yield RunRequest(
            run_key=f"hourly_{context.sensor_name}",
            tags={"source": "hourly_sensor"},
        )
