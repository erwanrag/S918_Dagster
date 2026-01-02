from dataclasses import dataclass
from typing import Optional


@dataclass
class TableStat:
    run_id: str
    layer: str
    table_name: str
    rows_source: int
    rows_loaded: int
    duration_sec: float
    load_mode: str
    status: str
    error_message: Optional[str] = None


@dataclass
class RunStat:
    run_id: str
    pipeline: str
    status: str
    tables_processed: int
    total_rows: int
