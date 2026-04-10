"""Tests for the code_validator domain service."""

from __future__ import annotations

from autoinvoice.domain.models.classification import TariffCode
from autoinvoice.domain.services.code_validator import (
    fix_invalid_code,
    is_end_node_code,
    validate_cet_code,
)
from tests.fakes.fake_code_repo import InMemoryCodeRepository

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _repo(
    valid: set[str] | None = None,
    corrections: dict[str, str] | None = None,
) -> InMemoryCodeRepository:
    return InMemoryCodeRepository(
        valid_codes=valid or {"33051000", "84715000", "96034020"},
        corrections=corrections or {"96034000": "96034020"},
    )


# ---------------------------------------------------------------------------
# Tests — validate_cet_code
# ---------------------------------------------------------------------------


class TestValidateCetCode:
    """Tests for validate_cet_code."""

    def test_valid_8_digit_code_passes(self) -> None:
        """'33051000' is valid when present in the repo."""
        repo = _repo()
        assert validate_cet_code("33051000", repo) is True

    def test_invalid_code_not_in_repo_fails(self) -> None:
        """'99999999' not in repo -> invalid."""
        repo = _repo()
        assert validate_cet_code("99999999", repo) is False

    def test_category_heading_rejected(self) -> None:
        """4-digit or 6-digit codes are headings, not end-nodes -> invalid."""
        repo = _repo()
        assert validate_cet_code("3305", repo) is False
        assert validate_cet_code("330510", repo) is False

    def test_non_digit_code_rejected(self) -> None:
        """Non-digit strings are invalid."""
        repo = _repo()
        assert validate_cet_code("ABCD1234", repo) is False
        assert validate_cet_code("", repo) is False


# ---------------------------------------------------------------------------
# Tests — is_end_node_code
# ---------------------------------------------------------------------------


class TestIsEndNodeCode:
    """Tests for is_end_node_code."""

    def test_8_digit_is_end_node(self) -> None:
        assert is_end_node_code("33051000") is True

    def test_4_digit_is_not_end_node(self) -> None:
        assert is_end_node_code("3305") is False

    def test_6_digit_is_not_end_node(self) -> None:
        assert is_end_node_code("330510") is False

    def test_non_digit_is_not_end_node(self) -> None:
        assert is_end_node_code("ABCD1234") is False


# ---------------------------------------------------------------------------
# Tests — fix_invalid_code
# ---------------------------------------------------------------------------


class TestFixInvalidCode:
    """Tests for fix_invalid_code."""

    def test_known_wrong_code_gets_corrected(self) -> None:
        """'96034000' corrected to '96034020' via repo."""
        repo = _repo()
        result = fix_invalid_code("96034000", repo)
        assert result is not None
        assert result.code == "96034020"

    def test_correction_not_found_returns_none(self) -> None:
        """Invalid code with no correction -> None."""
        repo = _repo(valid={"33051000"}, corrections={})
        result = fix_invalid_code("99999999", repo)
        assert result is None

    def test_fix_invalid_code_returns_corrected_tariff(self) -> None:
        """Full flow: validate -> detect invalid -> correct -> return TariffCode."""
        repo = _repo()
        result = fix_invalid_code("96034000", repo)
        assert isinstance(result, TariffCode)
        assert result.code == "96034020"

    def test_fix_valid_code_returns_same(self) -> None:
        """Valid code passes through unchanged."""
        repo = _repo()
        result = fix_invalid_code("33051000", repo)
        assert isinstance(result, TariffCode)
        assert result.code == "33051000"
