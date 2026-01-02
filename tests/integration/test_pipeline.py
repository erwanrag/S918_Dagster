"""
Tests d'intégration - Pipeline complet
"""

import pytest
from unittest.mock import patch, MagicMock
from dagster import build_op_context


@pytest.mark.integration
def test_services_assets():
    """Test assets services (imports)"""
    from src.assets.services import (
        currency_codes_loaded,
        exchange_rates_loaded,
        time_dimension_built,
    )

    assert currency_codes_loaded is not None
    assert exchange_rates_loaded is not None
    assert time_dimension_built is not None
