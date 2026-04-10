"""Tests for SqliteCodeRepository adapter."""

from __future__ import annotations

import sqlite3

import pytest

from autoinvoice.adapters.storage.sqlite_code_repo import SqliteCodeRepository


@pytest.fixture()
def db_path(tmp_path: object) -> str:
    return str(tmp_path / "test_cet.db")  # type: ignore[operator]


# -- is_valid_code -----------------------------------------------------------


@pytest.mark.integration
def test_is_valid_code_in_set(db_path: str) -> None:
    repo = SqliteCodeRepository(
        db_path=db_path,
        invalid_codes={},
        valid_codes={"12345678"},
    )
    assert repo.is_valid_code("12345678") is True


@pytest.mark.integration
def test_is_valid_code_in_db(db_path: str) -> None:
    repo = SqliteCodeRepository(db_path=db_path)
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO cet_codes (hs_code, description, updated_at) VALUES (?, ?, datetime('now'))",
        ("87654321", "Test code"),
    )
    conn.commit()
    conn.close()
    assert repo.is_valid_code("87654321") is True


@pytest.mark.integration
def test_is_valid_code_not_found(db_path: str) -> None:
    repo = SqliteCodeRepository(db_path=db_path)
    assert repo.is_valid_code("99999999") is False


# -- get_correction ----------------------------------------------------------


@pytest.mark.integration
def test_get_correction_found(db_path: str) -> None:
    corrections = {
        "33051010": {"correct_code": "33051000", "reason": "subcategory header"},
    }
    repo = SqliteCodeRepository(db_path=db_path, invalid_codes=corrections)
    assert repo.get_correction("33051010") == "33051000"


@pytest.mark.integration
def test_get_correction_found_flat(db_path: str) -> None:
    """Support flat string values as well as dicts."""
    repo = SqliteCodeRepository(
        db_path=db_path,
        invalid_codes={"11111111": "22222222"},  # type: ignore[dict-item]
    )
    assert repo.get_correction("11111111") == "22222222"


@pytest.mark.integration
def test_get_correction_not_found(db_path: str) -> None:
    repo = SqliteCodeRepository(db_path=db_path, invalid_codes={})
    assert repo.get_correction("00000000") is None


# -- lookup_by_description ---------------------------------------------------


@pytest.mark.integration
def test_lookup_by_description_empty_db(db_path: str) -> None:
    repo = SqliteCodeRepository(db_path=db_path)
    assert repo.lookup_by_description("widget") == []


@pytest.mark.integration
def test_lookup_by_description_finds_match(db_path: str) -> None:
    repo = SqliteCodeRepository(db_path=db_path)
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO cet_codes (hs_code, description, updated_at) VALUES (?, ?, datetime('now'))",
        ("44011100", "Fuel wood, in logs"),
    )
    conn.commit()
    conn.close()
    results = repo.lookup_by_description("Fuel wood")
    assert len(results) >= 1
    assert results[0][0] == "44011100"
    assert 0 < results[0][1] <= 1.0


# -- get_assessed_classification --------------------------------------------


@pytest.mark.integration
def test_get_assessed_classification_found(db_path: str) -> None:
    repo = SqliteCodeRepository(db_path=db_path)
    conn = sqlite3.connect(db_path)
    conn.execute(
        "INSERT INTO asycuda_classifications "
        "(sku, hs_code, description, created_at) "
        "VALUES (?, ?, ?, datetime('now'))",
        ("SKU-001", "12345678", "Test item"),
    )
    conn.commit()
    conn.close()
    result = repo.get_assessed_classification("SKU-001")
    assert result is not None
    assert result["hs_code"] == "12345678"
    assert result["sku"] == "SKU-001"


@pytest.mark.integration
def test_get_assessed_classification_not_found(db_path: str) -> None:
    repo = SqliteCodeRepository(db_path=db_path)
    assert repo.get_assessed_classification("MISSING") is None
