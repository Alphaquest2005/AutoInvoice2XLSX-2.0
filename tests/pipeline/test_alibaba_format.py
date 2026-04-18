"""Regression tests for config/formats/alibaba_marketplace_invoice.yaml.

Background: the YAML was previously corrupted (unterminated regex strings,
triplicated top-level blocks). This test pins down the contract so the file
cannot regress silently again.
"""

import os
import sys

import yaml

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "pipeline"))

from format_parser import FormatParser  # noqa: E402
from format_registry import FormatRegistry  # noqa: E402

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SPEC_PATH = os.path.join(REPO_ROOT, "config", "formats", "alibaba_marketplace_invoice.yaml")


def test_yaml_is_parseable():
    """File must load via yaml.safe_load — no dangling quotes, no duplicate keys."""
    with open(SPEC_PATH) as f:
        spec = yaml.safe_load(f)
    assert isinstance(spec, dict)
    assert spec["name"] == "alibaba_marketplace_invoice"


def test_items_schema_matches_format_parser():
    """items.line must use singular `pattern` (format_parser.py:736 reads singular)."""
    with open(SPEC_PATH) as f:
        spec = yaml.safe_load(f)
    line = spec["items"]["line"]
    assert "pattern" in line, "line.pattern (singular) is required by format_parser"
    assert isinstance(line["pattern"], str)
    # extra_patterns is optional but if present must be a list of dicts
    for extra in line.get("extra_patterns", []):
        assert "regex" in extra
        assert "field_map" in extra


def test_no_duplicate_toplevel_keys():
    """Guard against the triplicated sections/ocr_config/validation blocks bug."""
    with open(SPEC_PATH) as f:
        raw = f.read()
    # YAML's safe_load silently drops duplicate keys; count them manually.
    for key in ("sections:", "ocr_config:", "validation:", "items:", "metadata:"):
        # Match only top-level (column 0) occurrences
        lines = [ln for ln in raw.splitlines() if ln.startswith(key)]
        assert len(lines) <= 1, f"Duplicate top-level block detected: {key}"


def test_format_parser_accepts_spec():
    """FormatParser(spec) must initialise without error."""
    with open(SPEC_PATH) as f:
        spec = yaml.safe_load(f)
    fp = FormatParser(spec)
    assert fp.name == "alibaba_marketplace_invoice"
    assert fp.spec["items"]["strategy"] == "line"


def test_registry_loads_and_detects():
    """The registry must load the spec and detect it on a representative sample."""
    reg = FormatRegistry(REPO_ROOT)
    names = [f.get("name") for f in reg.formats]
    assert "alibaba_marketplace_invoice" in names

    sample = (
        "Marketplace Facilitator\n"
        "Alibaba.com Singapore E-Commerce Private Limited\n"
        "Supplier name SZREBOW ELECTRONICS\n"
        "BLOMZFL12345678901\n"
        "Total USD of Sales Tax (7.000 %) 258.07\n"
    )
    match = reg.detect_format(sample)
    assert match is not None, "Sample text should match the alibaba format"
    assert match["name"] == "alibaba_marketplace_invoice"
