"""In-memory fake for CodeRepositoryPort."""

from __future__ import annotations

from typing import Any


class InMemoryCodeRepository:
    """Fake tariff code repository backed by in-memory sets and dicts."""

    def __init__(
        self,
        valid_codes: set[str] | None = None,
        corrections: dict[str, str] | None = None,
        assessed: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self._valid_codes = valid_codes or set()
        self._corrections = corrections or {}
        self._assessed = assessed or {}
        self._description_results: list[tuple[str, float]] = []

    def is_valid_code(self, code: str) -> bool:
        """Check whether a tariff code exists in the in-memory set."""
        return code in self._valid_codes

    def get_correction(self, invalid_code: str) -> str | None:
        """Look up a corrected code from the in-memory corrections dict."""
        return self._corrections.get(invalid_code)

    def lookup_by_description(self, description: str) -> list[tuple[str, float]]:
        """Return pre-configured description results (empty by default)."""
        return self._description_results

    def get_assessed_classification(self, sku: str) -> dict[str, Any] | None:
        """Retrieve a previously assessed classification from the in-memory dict."""
        return self._assessed.get(sku)
