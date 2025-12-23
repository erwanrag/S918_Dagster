
"""
============================================================================
Assets Services - Devises + Dimension temporelle
============================================================================
"""
from dagster import AssetExecutionContext, asset

from src.core.services.currencies import load_currency_codes, load_exchange_rates
from src.core.services.time_dimension import build_time_dimension
from src.utils.logging import get_logger

logger = get_logger(__name__)


@asset(name="currency_codes_loaded", group_name="services")
def currency_codes_loaded(context: AssetExecutionContext) -> int:
    """Charger codes devises ISO 4217"""
    count = load_currency_codes()
    context.log.info(f"Currency codes loaded: {count}")
    return count


@asset(name="exchange_rates_loaded", group_name="services")
def exchange_rates_loaded(context: AssetExecutionContext) -> int:
    """Charger taux de change EUR"""
    count = load_exchange_rates()
    context.log.info(f"Exchange rates loaded: {count}")
    return count


@asset(name="time_dimension_built", group_name="services")
def time_dimension_built(context: AssetExecutionContext) -> int:
    """Construire dimension temporelle 2015-2035"""
    count = build_time_dimension()
    context.log.info(f"Time dimension built: {count} days")
    return count
