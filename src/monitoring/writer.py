from psycopg2.extras import execute_values
from src.monitoring.models import TableStat, RunStat


class MonitoringWriter:
    def __init__(self, conn):
        self.conn = conn

    def write_run(self, run: RunStat):
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO etl_monitoring.etl_run (
                    run_id, pipeline, status, tables_processed, total_rows, started_at
                )
                VALUES (%s, %s, %s, %s, %s, now())
                ON CONFLICT (run_id) DO NOTHING
                """,
                (
                    run.run_id,
                    run.pipeline,
                    run.status,
                    run.tables_processed,
                    run.total_rows,
                ),
            )

    def write_table_stats(self, stats: list[TableStat]):
        rows = [
            (
                s.run_id,
                s.layer,
                s.table_name,
                s.rows_source,
                s.rows_loaded,
                s.duration_sec,
                s.load_mode,
                s.status,
                s.error_message,
            )
            for s in stats
        ]

        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO etl_monitoring.etl_table_stats (
                    run_id, layer, table_name,
                    rows_source, rows_loaded,
                    duration_sec, load_mode,
                    status, error_message
                )
                VALUES %s
                """,
                rows,
            )
