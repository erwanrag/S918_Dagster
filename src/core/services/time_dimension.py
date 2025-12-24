from datetime import date, timedelta
from psycopg2 import sql

from src.config.constants import Schema
from src.utils.logging import get_logger

logger = get_logger(__name__)


def build_time_dimension(
    conn,
    start_year: int = 2015,
    end_year: int = 2035,
) -> int:
    start_date = date(start_year, 1, 1)
    end_date = date(end_year, 12, 31)

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}")
            .format(sql.Identifier(Schema.REFERENCE.value))
        )

        cur.execute(
            sql.SQL("""
                CREATE TABLE IF NOT EXISTS {}.dim_time (
                    date_key DATE PRIMARY KEY,
                    year INTEGER,
                    quarter INTEGER,
                    month INTEGER,
                    day INTEGER,
                    day_of_week INTEGER,
                    day_name VARCHAR(10),
                    week_of_year INTEGER,
                    is_weekend BOOLEAN,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """).format(sql.Identifier(Schema.REFERENCE.value))
        )

        cur.execute(
            sql.SQL("TRUNCATE TABLE {}.dim_time")
            .format(sql.Identifier(Schema.REFERENCE.value))
        )

        current_date = start_date
        count = 0

        while current_date <= end_date:
            cur.execute(
                sql.SQL("""
                    INSERT INTO {}.dim_time (
                        date_key,
                        year,
                        quarter,
                        month,
                        day,
                        day_of_week,
                        day_name,
                        week_of_year,
                        is_weekend
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """).format(sql.Identifier(Schema.REFERENCE.value)),
                (
                    current_date,
                    current_date.year,
                    (current_date.month - 1) // 3 + 1,
                    current_date.month,
                    current_date.day,
                    current_date.weekday() + 1,
                    current_date.strftime("%A"),
                    current_date.isocalendar()[1],
                    current_date.weekday() >= 5,
                ),
            )
            current_date += timedelta(days=1)
            count += 1

    logger.info("Time dimension built", days=count)
    return count
