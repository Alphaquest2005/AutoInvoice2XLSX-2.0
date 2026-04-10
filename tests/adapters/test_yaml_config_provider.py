"""Tests for the YAML/JSON configuration provider adapter."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import yaml

from autoinvoice.adapters.config.yaml_config_provider import YamlConfigProvider

if TYPE_CHECKING:
    from pathlib import Path

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_yaml(path: Path, data: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(data, fh)


def _write_json(path: Path, data: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestYamlConfigProviderYaml:
    """Tests for YAML-based config loading."""

    def test_load_column_spec(self, tmp_path: Path) -> None:
        spec_data = {
            "version": "2.0.0",
            "columns": [
                {"name": "Tariff Code", "index": 0},
                {"name": "Description", "index": 1},
            ],
        }
        _write_yaml(tmp_path / "config" / "columns.yaml", spec_data)

        provider = YamlConfigProvider(str(tmp_path))
        result = provider.load_column_spec()

        assert result["version"] == "2.0.0"
        assert len(result["columns"]) == 2

    def test_load_pipeline_config(self, tmp_path: Path) -> None:
        data = {"pipeline": {"name": "test", "version": "1.0.0"}}
        _write_yaml(tmp_path / "config" / "pipeline.yaml", data)

        provider = YamlConfigProvider(str(tmp_path))
        result = provider.load_pipeline_config()

        assert result["pipeline"]["name"] == "test"

    def test_load_grouping_config(self, tmp_path: Path) -> None:
        data = {"grouping": {"group_by": "tariff_code"}}
        _write_yaml(tmp_path / "config" / "grouping.yaml", data)

        provider = YamlConfigProvider(str(tmp_path))
        result = provider.load_grouping_config()

        assert result["grouping"]["group_by"] == "tariff_code"

    def test_load_shipment_rules(self, tmp_path: Path) -> None:
        data = {"rules": [{"field": "weight", "max": 1000}]}
        _write_yaml(tmp_path / "config" / "shipment_rules.yaml", data)

        provider = YamlConfigProvider(str(tmp_path))
        result = provider.load_shipment_rules()

        assert result["rules"][0]["field"] == "weight"

    def test_load_format_spec_found(self, tmp_path: Path) -> None:
        data = {"format": "amazon", "columns": ["A", "B"]}
        _write_yaml(tmp_path / "config" / "formats" / "amazon.yaml", data)

        provider = YamlConfigProvider(str(tmp_path))
        result = provider.load_format_spec("amazon")

        assert result is not None
        assert result["format"] == "amazon"

    def test_load_format_spec_not_found(self, tmp_path: Path) -> None:
        provider = YamlConfigProvider(str(tmp_path))
        result = provider.load_format_spec("nonexistent")

        assert result is None

    def test_list_format_specs(self, tmp_path: Path) -> None:
        formats_dir = tmp_path / "config" / "formats"
        formats_dir.mkdir(parents=True)
        _write_yaml(formats_dir / "amazon.yaml", {"format": "amazon"})
        _write_yaml(formats_dir / "ebay.yaml", {"format": "ebay"})
        _write_yaml(formats_dir / "shopify.yaml", {"format": "shopify"})

        provider = YamlConfigProvider(str(tmp_path))
        result = provider.list_format_specs()

        assert result == ["amazon", "ebay", "shopify"]

    def test_load_missing_yaml_returns_empty_dict(self, tmp_path: Path) -> None:
        provider = YamlConfigProvider(str(tmp_path))
        result = provider.load_column_spec()

        assert result == {}


class TestYamlConfigProviderJson:
    """Tests for JSON-based config loading."""

    def test_load_classification_rules(self, tmp_path: Path) -> None:
        rules = [
            {"pattern": "laptop.*", "tariff_code": "84713000", "confidence": 0.9},
            {"pattern": "cable.*", "tariff_code": "85444290", "confidence": 0.8},
        ]
        _write_json(tmp_path / "rules" / "classification_rules.json", rules)

        provider = YamlConfigProvider(str(tmp_path))
        result = provider.load_classification_rules()

        assert len(result) == 2
        assert result[0]["tariff_code"] == "84713000"

    def test_load_invalid_codes(self, tmp_path: Path) -> None:
        codes = {"8471300": "84713000", "854442": "85444290"}
        _write_json(tmp_path / "rules" / "invalid_codes.json", codes)

        provider = YamlConfigProvider(str(tmp_path))
        result = provider.load_invalid_codes()

        assert result == {"8471300": "84713000", "854442": "85444290"}

    def test_load_missing_classification_rules_returns_empty_list(self, tmp_path: Path) -> None:
        provider = YamlConfigProvider(str(tmp_path))
        result = provider.load_classification_rules()

        assert result == []

    def test_load_missing_invalid_codes_returns_empty_dict(self, tmp_path: Path) -> None:
        provider = YamlConfigProvider(str(tmp_path))
        result = provider.load_invalid_codes()

        assert result == {}
