"""
Tests unitaires - Décision SCD2 (ODS)
"""

import pytest
from src.core.ods.merger import scd2_decision


@pytest.mark.unit
def test_new_primary_key():
    assert scd2_decision(
        existing_hash=None,
        incoming_hash="A",
    ) == "insert"


@pytest.mark.unit
def test_no_change_same_hash():
    assert scd2_decision(
        existing_hash="A",
        incoming_hash="A",
        is_current=True,
    ) == "noop"


@pytest.mark.unit
def test_hash_change_closes_current():
    assert scd2_decision(
        existing_hash="A",
        incoming_hash="B",
        is_current=True,
    ) == "close"


@pytest.mark.unit
def test_reopen_closed_record():
    assert scd2_decision(
        existing_hash="A",
        incoming_hash="A",
        is_current=False,
    ) == "insert"


@pytest.mark.unit
def test_deleted_record_reinserted():
    assert scd2_decision(
        existing_hash="A",
        incoming_hash="B",
        is_deleted=True,
    ) == "insert"
