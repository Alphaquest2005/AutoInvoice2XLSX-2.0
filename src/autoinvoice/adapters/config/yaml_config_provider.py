"""YAML/JSON configuration provider adapter."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml


class YamlConfigProvider:
    """Loads configuration from YAML and JSON files on disk.

    Implements :class:`~autoinvoice.domain.ports.config_provider.ConfigProviderPort`.
    """

    def __init__(self, base_dir: str) -> None:
        self._base = Path(base_dir)

    # ------------------------------------------------------------------
    # YAML loaders
    # ------------------------------------------------------------------

    def load_column_spec(self) -> dict[str, Any]:
        """Load ``config/columns.yaml``."""
        return self._load_yaml(self._base / "config" / "columns.yaml")

    def load_pipeline_config(self) -> dict[str, Any]:
        """Load ``config/pipeline.yaml``."""
        return self._load_yaml(self._base / "config" / "pipeline.yaml")

    def load_grouping_config(self) -> dict[str, Any]:
        """Load ``config/grouping.yaml``."""
        return self._load_yaml(self._base / "config" / "grouping.yaml")

    def load_shipment_rules(self) -> dict[str, Any]:
        """Load ``config/shipment_rules.yaml``."""
        return self._load_yaml(self._base / "config" / "shipment_rules.yaml")

    def load_format_spec(self, format_name: str) -> dict[str, Any] | None:
        """Load ``config/formats/{format_name}.yaml``, or *None* if missing."""
        path = self._base / "config" / "formats" / f"{format_name}.yaml"
        if not path.exists():
            return None
        return self._load_yaml(path)

    def list_format_specs(self) -> list[str]:
        """List available format spec names in ``config/formats/``."""
        formats_dir = self._base / "config" / "formats"
        if not formats_dir.is_dir():
            return []
        return sorted(p.stem for p in formats_dir.glob("*.yaml"))

    # ------------------------------------------------------------------
    # JSON loaders
    # ------------------------------------------------------------------

    def load_classification_rules(self) -> list[dict[str, Any]]:
        """Load ``rules/classification_rules.json``."""
        return self._load_json_list(self._base / "rules" / "classification_rules.json")

    def load_invalid_codes(self) -> dict[str, str]:
        """Load ``rules/invalid_codes.json`` as ``{invalid: corrected}``."""
        path = self._base / "rules" / "invalid_codes.json"
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, dict):
            return {str(k): str(v) for k, v in data.items()}
        return {}

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _load_yaml(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        with path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
        if isinstance(data, dict):
            return data
        return {}

    def _load_json_list(self, path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, list):
            return data
        return []
