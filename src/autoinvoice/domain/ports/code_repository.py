"""Port for tariff code validation and lookup."""

from __future__ import annotations

from typing import Any, Protocol


class CodeRepositoryPort(Protocol):
    """Interface for querying and validating tariff codes."""

    def is_valid_code(self, code: str) -> bool:
        """Check whether a tariff code exists in the repository.

        Args:
            code: 8-digit tariff code string.

        Returns:
            True if the code is valid.
        """
        ...

    def get_correction(self, invalid_code: str) -> str | None:
        """Look up the corrected code for a known invalid code.

        Args:
            invalid_code: The invalid tariff code.

        Returns:
            Corrected code, or None if no correction is known.
        """
        ...

    def lookup_by_description(self, description: str) -> list[tuple[str, float]]:
        """Search for tariff codes matching a textual description.

        Args:
            description: Item description to search for.

        Returns:
            List of (code, similarity_score) tuples, highest score first.
        """
        ...

    def get_assessed_classification(self, sku: str) -> dict[str, Any] | None:
        """Retrieve a previously assessed classification for an SKU.

        Args:
            sku: Product SKU identifier.

        Returns:
            Classification dict, or None if no assessment exists.
        """
        ...
