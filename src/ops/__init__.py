"""Ops - Point d'entrée"""

# --- VOS IMPORTS EXISTANTS (Ne pas toucher) ---
from .metadata_ops import (
    get_table_metadata_op, 
    list_active_tables_op, 
    determine_load_mode_op
)
from .sftp_ops import (
    scan_sftp_files_op, 
    parse_filename_op, 
    read_metadata_json_op, 
    log_file_to_monitoring_op, 
    load_parquet_to_raw_op
)
from .staging_ops import (
    create_staging_table_op, 
    load_raw_to_staging_op, 
    verify_staging_table_op
)
from .ods_ops import (
    merge_staging_to_ods_op, 
    verify_ods_table_op
)

# --- NOUVEAUX IMPORTS (Fonctionnalités manquantes) ---
from .maintenance import (
    cleanup_etl_logs_op,
    get_logs_stats_op
)
from .metadata_import import (
    scan_metadata_files_op,
    load_metadata_file_op,
    archive_metadata_file_op
)
from .dbt_generator import (
    generate_prep_models_op,
    compile_generated_models_op
)

__all__ = [
    # Existing
    "get_table_metadata_op", "list_active_tables_op", "determine_load_mode_op",
    "scan_sftp_files_op", "parse_filename_op", "read_metadata_json_op", "log_file_to_monitoring_op", "load_parquet_to_raw_op",
    "create_staging_table_op", "load_raw_to_staging_op", "verify_staging_table_op",
    "merge_staging_to_ods_op", "verify_ods_table_op",
    
    # New
    "cleanup_etl_logs_op", "get_logs_stats_op",
    "scan_metadata_files_op", "load_metadata_file_op", "archive_metadata_file_op",
    "generate_prep_models_op", "compile_generated_models_op"
]