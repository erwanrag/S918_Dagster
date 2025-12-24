import requests
from tenacity import retry, stop_after_attempt, wait_exponential
from psycopg2 import sql

from src.config.constants import Schema
from src.utils.logging import get_logger

logger = get_logger(__name__)

CURRENCIES_API = "https://openexchangerates.org/api/currencies.json"
EXCHANGE_RATES_API = "https://open.er-api.com/v6/latest/EUR"


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
def fetch_currency_codes() -> dict[str, str]:
    try:
        response = requests.get(CURRENCIES_API, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception:
        logger.exception("Failed to fetch currency codes")
        raise


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
def fetch_exchange_rates() -> dict[str, float]:
    try:
        response = requests.get(EXCHANGE_RATES_API, timeout=10)
        response.raise_for_status()
        return response.json()["rates"]
    except Exception:
        logger.exception("Failed to fetch exchange rates")
        raise


def load_currency_codes(conn) -> int:
    currencies = fetch_currency_codes()

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}")
            .format(sql.Identifier(Schema.REFERENCE.value))
        )

        cur.execute(
            sql.SQL("""
                CREATE TABLE IF NOT EXISTS {}.currency_codes (
                    code VARCHAR(3) PRIMARY KEY,
                    name VARCHAR(255),
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """).format(sql.Identifier(Schema.REFERENCE.value))
        )

        cur.execute(
            sql.SQL("TRUNCATE TABLE {}.currency_codes")
            .format(sql.Identifier(Schema.REFERENCE.value))
        )

        for code, name in currencies.items():
            cur.execute(
                sql.SQL("""
                    INSERT INTO {}.currency_codes (code, name)
                    VALUES (%s, %s)
                """).format(sql.Identifier(Schema.REFERENCE.value)),
                (code, name),
            )

    logger.info("Currency codes loaded", count=len(currencies))
    return len(currencies)


def load_exchange_rates(conn) -> int:
    rates = fetch_exchange_rates()

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}")
            .format(sql.Identifier(Schema.REFERENCE.value))
        )

        cur.execute(
            sql.SQL("""
                CREATE TABLE IF NOT EXISTS {}.exchange_rates (
                    currency_code VARCHAR(3),
                    rate NUMERIC(18, 6),
                    base_currency VARCHAR(3) DEFAULT 'EUR',
                    rate_date DATE DEFAULT CURRENT_DATE,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (currency_code, rate_date)
                )
            """).format(sql.Identifier(Schema.REFERENCE.value))
        )

        for code, rate in rates.items():
            cur.execute(
                sql.SQL("""
                    INSERT INTO {}.exchange_rates (currency_code, rate)
                    VALUES (%s, %s)
                    ON CONFLICT (currency_code, rate_date)
                    DO UPDATE SET
                        rate = EXCLUDED.rate,
                        updated_at = CURRENT_TIMESTAMP
                """).format(sql.Identifier(Schema.REFERENCE.value)),
                (code, rate),
            )

    logger.info("Exchange rates loaded", count=len(rates))
    return len(rates)
