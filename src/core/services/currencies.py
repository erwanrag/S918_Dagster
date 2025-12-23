
"""
============================================================================
Currencies - Chargement codes devises et taux de change
============================================================================
"""
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config.constants import Schema
from src.db.connection import get_connection
from src.utils.logging import get_logger

logger = get_logger(__name__)

CURRENCIES_API = "https://openexchangerates.org/api/currencies.json"
EXCHANGE_RATES_API = "https://open.er-api.com/v6/latest/EUR"


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
def fetch_currency_codes() -> dict[str, str]:
    """Récupérer les codes ISO 4217 depuis l'API"""
    response = requests.get(CURRENCIES_API, timeout=10)
    response.raise_for_status()
    return response.json()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
def fetch_exchange_rates() -> dict[str, float]:
    """Récupérer les taux de change (base EUR)"""
    response = requests.get(EXCHANGE_RATES_API, timeout=10)
    response.raise_for_status()
    data = response.json()
    return data["rates"]


def load_currency_codes() -> int:
    """Charger les codes devises dans PostgreSQL"""
    currencies = fetch_currency_codes()
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {Schema.REFERENCE.value}")
            
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {Schema.REFERENCE.value}.currency_codes (
                    code VARCHAR(3) PRIMARY KEY,
                    name VARCHAR(255),
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            cur.execute(f"TRUNCATE TABLE {Schema.REFERENCE.value}.currency_codes")
            
            for code, name in currencies.items():
                cur.execute(f"""
                    INSERT INTO {Schema.REFERENCE.value}.currency_codes (code, name)
                    VALUES (%s, %s)
                """, (code, name))
            
            count = len(currencies)
            logger.info("Currency codes loaded", count=count)
            return count


def load_exchange_rates() -> int:
    """Charger les taux de change dans PostgreSQL"""
    rates = fetch_exchange_rates()
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {Schema.REFERENCE.value}")
            
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {Schema.REFERENCE.value}.exchange_rates (
                    currency_code VARCHAR(3),
                    rate NUMERIC(18, 6),
                    base_currency VARCHAR(3) DEFAULT 'EUR',
                    rate_date DATE DEFAULT CURRENT_DATE,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (currency_code, rate_date)
                )
            """)
            
            for code, rate in rates.items():
                cur.execute(f"""
                    INSERT INTO {Schema.REFERENCE.value}.exchange_rates 
                        (currency_code, rate)
                    VALUES (%s, %s)
                    ON CONFLICT (currency_code, rate_date) 
                    DO UPDATE SET rate = EXCLUDED.rate, updated_at = CURRENT_TIMESTAMP
                """, (code, rate))
            
            count = len(rates)
            logger.info("Exchange rates loaded", count=count)
            return count
