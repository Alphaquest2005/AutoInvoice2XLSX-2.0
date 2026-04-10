"""In-memory fake for ConfigProviderPort."""

from __future__ import annotations

from typing import Any


class FakeConfigProvider:
    """Fake config provider that returns pre-configured values."""

    def __init__(
        self,
        column_spec: dict[str, Any] | None = None,
        pipeline_config: dict[str, Any] | None = None,
        grouping_config: dict[str, Any] | None = None,
        classification_rules: list[dict[str, Any]] | None = None,
        invalid_codes: dict[str, str] | None = None,
        format_specs: dict[str, dict[str, Any]] | None = None,
        shipment_rules: dict[str, Any] | None = None,
    ) -> None:
        self._column_spec = column_spec or {}
        self._pipeline_config = pipeline_config or {}
        self._grouping_config = grouping_config or {}
        self._classification_rules = classification_rules or []
        self._invalid_codes = invalid_codes or {}
        self._format_specs = format_specs or {}
        self._shipment_rules = shipment_rules or {}

    def load_column_spec(self) -> dict[str, Any]:
        """Return the stored column specification."""
        return self._column_spec

    def load_pipeline_config(self) -> dict[str, Any]:
        """Return the stored pipeline configuration."""
        return self._pipeline_config

    def load_grouping_config(self) -> dict[str, Any]:
        """Return the stored grouping configuration."""
        return self._grouping_config

    def load_classification_rules(self) -> list[dict[str, Any]]:
        """Return the stored classification rules."""
        return self._classification_rules

    def load_invalid_codes(self) -> dict[str, str]:
        """Return the stored invalid-code-to-correction mapping."""
        return self._invalid_codes

    def load_format_spec(self, format_name: str) -> dict[str, Any] | None:
        """Return the format spec for the given name, or None."""
        return self._format_specs.get(format_name)

    def list_format_specs(self) -> list[str]:
        """Return all available format spec names."""
        return sorted(self._format_specs.keys())

    def load_shipment_rules(self) -> dict[str, Any]:
        """Return the stored shipment rules."""
        return self._shipment_rules
