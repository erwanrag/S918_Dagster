from dagster import AssetExecutionContext, asset

from src.core.services.currencies import (
    load_currency_codes,
    load_exchange_rates,
)
from src.core.services.time_dimension import build_time_dimension
from src.utils.logging import get_logger

logger = get_logger(__name__)


@asset(
    name="currency_codes",
    group_name="services",
    required_resource_keys={"postgres"},
    description="Référentiel des devises ISO",
)
def currency_codes_loaded(context: AssetExecutionContext) -> int:
    with context.resources.postgres.get_connection() as conn:
        count = load_currency_codes(conn)
    context.log.info(f"Currency codes loaded: {count}")
    return count


@asset(
    name="exchange_rates",
    group_name="services",
    required_resource_keys={"postgres"},
    description="Taux de change journaliers",
)
def exchange_rates_loaded(context: AssetExecutionContext) -> int:
    with context.resources.postgres.get_connection() as conn:
        count = load_exchange_rates(conn)
    context.log.info(f"Exchange rates loaded: {count}")
    return count

  
@asset(
    name="time_dimension",
    group_name="services",
    required_resource_keys={"postgres"},
    description="Dimension temporelle",
)
def time_dimension_built(context: AssetExecutionContext) -> int:
    with context.resources.postgres.get_connection() as conn:
        count = build_time_dimension(conn)
    context.log.info(f"Time dimension built: {count} days")
    return count
