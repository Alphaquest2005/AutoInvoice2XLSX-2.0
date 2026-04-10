"""Port for persisting and retrieving classification data."""

from __future__ import annotations

from typing import Any, Protocol


class ClassificationStorePort(Protocol):
    """Interface for storing and querying tariff classifications."""

    def get_classification(self, sku: str) -> dict[str, Any] | None:
        """Retrieve the stored classification for an SKU.

        Args:
            sku: Product SKU identifier.

        Returns:
            Classification dict, or None if not found.
        """
        ...

    def save_classification(self, sku: str, code: str, source: str) -> None:
        """Persist a classification for an SKU.

        Args:
            sku: Product SKU identifier.
            code: 8-digit tariff code.
            source: Origin of the classification (e.g. 'rules', 'llm').
        """
        ...

    def get_corrections(self, sku: str) -> list[dict[str, Any]]:
        """Retrieve correction history for an SKU.

        Args:
            sku: Product SKU identifier.

        Returns:
            List of correction records.
        """
        ...

    def save_correction(self, sku: str, old_code: str, new_code: str) -> None:
        """Record a tariff code correction for an SKU.

        Args:
            sku: Product SKU identifier.
            old_code: Previous incorrect code.
            new_code: Corrected code.
        """
        ...

    def import_asycuda(self, classifications: list[dict[str, Any]]) -> dict[str, int]:
        """Bulk import classifications from ASYCUDA export data.

        Args:
            classifications: List of classification records to import.

        Returns:
            Summary with counts (e.g. imported, skipped, updated).
        """
        ...
