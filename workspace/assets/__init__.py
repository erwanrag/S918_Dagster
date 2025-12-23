"""Assets"""

from .ingestion import (
    sftp_discovered_files,
    raw_tables_loaded,
    staging_tables_ready
)

from .ods import (
    ods_tables_merged,
    ods_pipeline_summary
)

from .dbt_prep import (
    dbt_prep_models,
    dbt_prep_tests,
    prep_pipeline_summary
)

from .services import (
    currency_codes_loaded,
    exchange_rates_loaded,
    time_dimension_built
)

__all__ = [
    # Ingestion
    "sftp_discovered_files",
    "raw_tables_loaded",
    "staging_tables_ready",
    
    # ODS
    "ods_tables_merged",
    "ods_pipeline_summary",
    
    # PREP (dbt)
    "dbt_prep_models",
    "dbt_prep_tests",
    "prep_pipeline_summary",
    
    # Services
    "currency_codes_loaded",
    "exchange_rates_loaded",
    "time_dimension_built",
]