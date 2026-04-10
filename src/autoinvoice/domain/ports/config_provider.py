"""Port for loading application configuration."""

from __future__ import annotations

from typing import Any, Protocol


class ConfigProviderPort(Protocol):
    """Interface for loading configuration data from external sources."""

    def load_column_spec(self) -> dict[str, Any]:
        """Load the XLSX column specification.

        Returns:
            Raw column spec dictionary.
        """
        ...

    def load_pipeline_config(self) -> dict[str, Any]:
        """Load pipeline stage configuration.

        Returns:
            Pipeline config dictionary.
        """
        ...

    def load_grouping_config(self) -> dict[str, Any]:
        """Load item grouping rules.

        Returns:
            Grouping config dictionary.
        """
        ...

    def load_classification_rules(self) -> list[dict[str, Any]]:
        """Load static classification rules.

        Returns:
            List of classification rule dictionaries.
        """
        ...

    def load_invalid_codes(self) -> dict[str, str]:
        """Load the mapping of known invalid codes to their corrections.

        Returns:
            Mapping of invalid_code -> corrected_code.
        """
        ...

    def load_format_spec(self, format_name: str) -> dict[str, Any] | None:
        """Load a named invoice format specification.

        Args:
            format_name: Identifier of the format (e.g. supplier name).

        Returns:
            Format spec dictionary, or None if not found.
        """
        ...

    def list_format_specs(self) -> list[str]:
        """List all available invoice format specification names.

        Returns:
            List of format spec identifiers.
        """
        ...

    def load_shipment_rules(self) -> dict[str, Any]:
        """Load shipment processing rules.

        Returns:
            Shipment rules dictionary.
        """
        ...
