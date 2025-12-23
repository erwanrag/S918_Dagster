"""
============================================================================
Time Dimension - Génération dimension temporelle 2015-2035
============================================================================
"""

from datetime import date, timedelta

from src.config.constants import Schema
from src.db.connection import get_connection
from src.utils.logging import get_logger

logger = get_logger(__name__)


def build_time_dimension(start_year: int = 2015, end_year: int = 2035) -> int:
    """
    Construire la dimension temporelle

    Génère une ligne par jour entre start_year et end_year

    Returns:
        Nombre de jours créés
    """
    start_date = date(start_year, 1, 1)
    end_date = date(end_year, 12, 31)

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {Schema.REFERENCE.value}")

            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {Schema.REFERENCE.value}.dim_time (
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
            """
            )

            cur.execute(f"TRUNCATE TABLE {Schema.REFERENCE.value}.dim_time")

            current_date = start_date
            count = 0

            while current_date <= end_date:
                day_of_week = current_date.weekday()
                day_names = [
                    "Lundi",
                    "Mardi",
                    "Mercredi",
                    "Jeudi",
                    "Vendredi",
                    "Samedi",
                    "Dimanche",
                ]

                cur.execute(
                    f"""
                    INSERT INTO {Schema.REFERENCE.value}.dim_time (
                        date_key, year, quarter, month, day,
                        day_of_week, day_name, week_of_year, is_weekend
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        current_date,
                        current_date.year,
                        (current_date.month - 1) // 3 + 1,
                        current_date.month,
                        current_date.day,
                        day_of_week + 1,
                        day_names[day_of_week],
                        current_date.isocalendar()[1],
                        day_of_week >= 5,
                    ),
                )

                current_date += timedelta(days=1)
                count += 1

            logger.info(
                "Time dimension built", days=count, start=start_year, end=end_year
            )
            return count
