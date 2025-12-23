"""Partitions"""

from .table_partitions import tables_partition, get_active_tables, get_table_load_mode

__all__ = ["tables_partition", "get_active_tables", "get_table_load_mode"]