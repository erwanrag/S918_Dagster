"""
Test d'intégration - Chargement Dagster (happy path)
"""

import pytest


@pytest.mark.integration
def test_dagster_definitions_import():
    """
    Vérifie que les definitions Dagster se chargent sans erreur
    """
    from src.definitions import definitions

    assert definitions is not None
