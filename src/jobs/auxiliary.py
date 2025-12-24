from dagster import job
from src.ops import (
    cleanup_etl_logs_op, cleanup_sftp_monitoring_op, get_logs_stats_op,
    scan_metadata_files_op, load_metadata_file_op, archive_metadata_file_op,
    generate_prep_models_op, compile_generated_models_op
)

@job(name="maintenance_cleanup_job")
def maintenance_cleanup_job():
    get_logs_stats_op()
    cleanup_etl_logs_op()
    cleanup_sftp_monitoring_op()

@job(name="metadata_import_job")
def metadata_import_job():
    files = scan_metadata_files_op()
    files.map(lambda f: archive_metadata_file_op(load_metadata_file_op(f)))

@job(name="dbt_generation_job")
def dbt_generation_job():
    generate_prep_models_op()
    compile_generated_models_op()