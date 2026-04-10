"""Tests for JsonClassificationStore adapter."""

from __future__ import annotations

import pytest

from autoinvoice.adapters.storage.json_file_store import JsonClassificationStore


@pytest.fixture()
def store(tmp_path: object) -> JsonClassificationStore:
    return JsonClassificationStore(base_dir=str(tmp_path))


# -- get / save classification -----------------------------------------------


@pytest.mark.integration
def test_save_and_get_classification(store: JsonClassificationStore) -> None:
    store.save_classification("SKU-001", "12345678", "rules")
    result = store.get_classification("SKU-001")
    assert result is not None
    assert result["code"] == "12345678"
    assert result["source"] == "rules"
    assert "updated_at" in result


@pytest.mark.integration
def test_get_nonexistent_returns_none(store: JsonClassificationStore) -> None:
    assert store.get_classification("MISSING") is None


@pytest.mark.integration
def test_save_overwrites_existing(store: JsonClassificationStore) -> None:
    store.save_classification("SKU-001", "11111111", "rules")
    store.save_classification("SKU-001", "22222222", "llm")
    result = store.get_classification("SKU-001")
    assert result is not None
    assert result["code"] == "22222222"
    assert result["source"] == "llm"


# -- corrections -------------------------------------------------------------


@pytest.mark.integration
def test_save_and_get_correction(store: JsonClassificationStore) -> None:
    store.save_correction("SKU-002", "11111111", "22222222")
    corrections = store.get_corrections("SKU-002")
    assert len(corrections) == 1
    assert corrections[0]["old_code"] == "11111111"
    assert corrections[0]["new_code"] == "22222222"
    assert "corrected_at" in corrections[0]


@pytest.mark.integration
def test_get_corrections_empty(store: JsonClassificationStore) -> None:
    assert store.get_corrections("UNKNOWN") == []


@pytest.mark.integration
def test_multiple_corrections_accumulate(store: JsonClassificationStore) -> None:
    store.save_correction("SKU-003", "aaa", "bbb")
    store.save_correction("SKU-003", "bbb", "ccc")
    corrections = store.get_corrections("SKU-003")
    assert len(corrections) == 2


# -- import_asycuda ----------------------------------------------------------


@pytest.mark.integration
def test_import_asycuda_counts(store: JsonClassificationStore) -> None:
    records = [
        {"sku": "SKU-A", "hs_code": "11110000", "source": "asycuda"},
        {"sku": "SKU-B", "hs_code": "22220000"},
        {"sku": "", "hs_code": "33330000"},  # missing sku -> skipped
        {"sku": "SKU-C"},  # missing code -> skipped
    ]
    counts = store.import_asycuda(records)
    assert counts["imported"] == 2
    assert counts["skipped"] == 2
    assert counts["updated"] == 0


@pytest.mark.integration
def test_import_asycuda_updates_existing(store: JsonClassificationStore) -> None:
    store.save_classification("SKU-X", "00000000", "rules")
    records = [{"sku": "SKU-X", "hs_code": "99999999"}]
    counts = store.import_asycuda(records)
    assert counts["updated"] == 1
    assert counts["imported"] == 0
    result = store.get_classification("SKU-X")
    assert result is not None
    assert result["code"] == "99999999"
