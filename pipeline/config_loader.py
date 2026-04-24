"""Single entry point for loading every config/*.yaml|json file.

Every pipeline module that needs a config value goes through a loader
function here — NOT direct yaml.safe_load() calls scattered across the
pipeline. This:

    1. Gives the magic-constant hook a single audit surface (code that
       does ``from pipeline.config_loader import load_columns`` is by
       construction reading from the SSOT, not hardcoding).
    2. Caches parsed configs per-process so hot code paths don't reread
       YAML on every call.
    3. Strips the '_meta:' block from returned data so downstream code
       never has to think about it.

Pattern:
    from pipeline.config_loader import load_columns, load_document_types
    cols = load_columns()
    dt   = load_document_types()

To add a config:
    1. Create config/<name>.yaml (or .json) with a '_meta:' block
       (enforced by scripts/hooks/check_config_meta.py).
    2. Add a ``load_<name>()`` function below using ``_load_yaml`` or
       ``_load_json``.
    3. Reference it from pipeline code via
       ``from pipeline.config_loader import load_<name>``.
"""

from __future__ import annotations

import json
import threading
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

_REPO_ROOT = Path(__file__).resolve().parent.parent
_CONFIG_DIR = _REPO_ROOT / "config"
_META_KEY = "_meta"

_lock = threading.Lock()


def _resolve(name: str) -> Path:
    """Resolve a config filename to an absolute path under config/."""
    p = _CONFIG_DIR / name
    if not p.is_file():
        raise FileNotFoundError(
            f"config file not found: {p} "
            f"(expected under {_CONFIG_DIR})"
        )
    return p


def _strip_meta(data: Any) -> Any:
    """Return a copy of ``data`` with the ``_meta`` key removed.
    Only applies to dict roots; other types pass through."""
    if isinstance(data, dict) and _META_KEY in data:
        out = dict(data)
        out.pop(_META_KEY, None)
        return out
    return data


@lru_cache(maxsize=None)
def _load_yaml(name: str) -> Any:
    """Parse a config/*.yaml file and return its content (minus _meta:)."""
    path = _resolve(name)
    with _lock:
        with path.open("r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
    return _strip_meta(data)


@lru_cache(maxsize=None)
def _load_json(name: str) -> Any:
    """Parse a config/*.json file and return its content (minus _meta)."""
    path = _resolve(name)
    with _lock:
        with path.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
    return _strip_meta(data)


def clear_cache() -> None:
    """Invalidate the in-process config cache. Use in tests that mutate
    config files on disk."""
    _load_yaml.cache_clear()
    _load_json.cache_clear()


# --------------------------------------------------------------- loaders


def load_columns() -> dict:
    """config/columns.yaml — 37-column XLSX schema."""
    return _load_yaml("columns.yaml")


def load_document_types() -> dict:
    """config/document_types.json — doc type codes + consignee rules."""
    return _load_json("document_types.json")


def load_grouping() -> dict:
    """config/grouping.yaml — tariff-code grouping behavior."""
    return _load_yaml("grouping.yaml")


def load_invoice_formats() -> dict:
    """config/invoice_formats.yaml — per-supplier format definitions."""
    return _load_yaml("invoice_formats.yaml")


def load_office_locations() -> dict:
    """config/office_locations.yaml — customs office code -> location."""
    return _load_yaml("office_locations.yaml")


def load_pipeline() -> dict:
    """config/pipeline.yaml — global pipeline config + stage sequence."""
    return _load_yaml("pipeline.yaml")


def load_shipment_rules() -> dict:
    """config/shipment_rules.yaml — BL identification + freight rules."""
    return _load_yaml("shipment_rules.yaml")


def load_variance_strategies() -> dict:
    """config/variance_analysis_strategies.yaml — LLM variance strategies."""
    return _load_yaml("variance_analysis_strategies.yaml")


def load_financial_constants() -> dict:
    """config/financial_constants.yaml — CSC/VAT rates, currency, precision."""
    return _load_yaml("financial_constants.yaml")


def load_validation_tolerances() -> dict:
    """config/validation_tolerances.yaml — variance thresholds + epsilons."""
    return _load_yaml("validation_tolerances.yaml")


def load_patterns() -> dict:
    """config/patterns.yaml — named regex patterns (raw strings, uncompiled)."""
    return _load_yaml("patterns.yaml")


def load_hs_structure() -> dict:
    """config/hs_structure.yaml — HS/tariff slice positions + sentinels."""
    return _load_yaml("hs_structure.yaml")


def load_issue_types() -> dict:
    """config/issue_types.yaml — status/severity/issue enum vocabulary."""
    return _load_yaml("issue_types.yaml")


def load_api_settings() -> dict:
    """config/api_settings.yaml — HTTP timeouts, retries, model IDs."""
    return _load_yaml("api_settings.yaml")


def load_cache_settings() -> dict:
    """config/cache_settings.yaml — cache sizes, TTLs, directories."""
    return _load_yaml("cache_settings.yaml")


def load_ocr_corrections() -> dict:
    """config/ocr_corrections.yaml — OCR misread fixups."""
    return _load_yaml("ocr_corrections.yaml")


def load_file_paths() -> dict:
    """config/file_paths.yaml — canonical dir names + file extensions."""
    return _load_yaml("file_paths.yaml")


def load_library_enums() -> dict:
    """config/library_enums.yaml — openpyxl/argparse/logging enum strings."""
    return _load_yaml("library_enums.yaml")


def load_country_codes() -> dict:
    """config/country_codes.yaml — ISO / CARICOM country-of-origin codes."""
    return _load_yaml("country_codes.yaml")


def load_xlsx_labels() -> dict:
    """config/xlsx_labels.yaml — user-visible row/section labels used by
    the BL XLSX generator (totals, duty section, reference section)."""
    return _load_yaml("xlsx_labels.yaml")


# --------------------------------------------------------------- derived

# Helpers that do common one-line transforms on top of the raw loaders so
# callers don't have to poke into dict shape everywhere.


def resolve_doc_type(consignee_name: str) -> str:
    """Return the doc_type code for a given consignee, falling back to
    the global default. Used by pipeline/run.py and xlsx_generator.py.

    Budget Marine regression: matches substring 'budget marine' -> 7400-000.
    """
    dt = load_document_types()
    needle = (consignee_name or "").strip().lower()
    if needle:
        for rule in dt.get("consignee_rules", []):
            match = (rule.get("match") or "").lower()
            aliases = [a.lower() for a in rule.get("aliases") or []]
            if match and match in needle:
                return rule["doc_type"]
            for alias in aliases:
                if alias and alias in needle:
                    return rule["doc_type"]
    return dt.get("default") or "4000-000"


def is_carrier_bl(reference: str) -> bool:
    """Return True if ``reference`` matches any configured carrier BL pattern."""
    import re
    rules = load_shipment_rules().get("bl_identification") or {}
    for pattern in rules.get("carrier_bl_patterns") or []:
        if re.fullmatch(pattern, reference or ""):
            return True
    return False
