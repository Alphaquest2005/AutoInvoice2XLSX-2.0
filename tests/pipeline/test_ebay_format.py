"""Regression tests for config/formats/ebay.yaml.

Background — HAWB9758348 leaked an item row "Seller vehicleusa2021 ...
Shipping $5.99" into the line-item table because the receipt-header columns
collapsed during OCR and the trailing dollar amount fooled the line-item
regex. The fix added receipt-header skip patterns (Buyer/Seller/Placed) and
bumped the spec to v1.1.

Per-format expectations live in the fixture manifest at
``tests/fixtures/<format>/index.yaml``. The format name itself is derived
from this test module's filename via the SSOT helper, so the test source
contains no domain string literals.
"""

from __future__ import annotations

import re

import yaml

from tests._fixtures import load_test_manifest, load_text_fixture
from tests._paths import (
    REPO_ROOT,
    add_pipeline_to_sys_path,
    as_str,
    format_name_from_test_file,
    format_spec_path,
)

add_pipeline_to_sys_path()

from format_parser import FormatParser  # noqa: E402
from format_registry import FormatRegistry  # noqa: E402

FORMAT_NAME = format_name_from_test_file(__file__)
MANIFEST = load_test_manifest(FORMAT_NAME)
SPEC_PATH = as_str(format_spec_path(FORMAT_NAME))


def _load_spec() -> dict:
    with open(SPEC_PATH, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def test_yaml_parses_and_names_match():
    spec = _load_spec()
    assert isinstance(spec, dict)
    assert spec["name"] == MANIFEST["format_name"]
    assert MANIFEST["format_name"] == FORMAT_NAME


def test_version_at_least_min():
    spec = _load_spec()
    version = str(spec.get("version", ""))
    assert version >= str(MANIFEST["min_version"])


def test_meta_block_has_required_keys():
    spec = _load_spec()
    meta = spec.get("_meta", {})
    assert isinstance(meta, dict)
    for key in MANIFEST["required_meta_keys"]:
        assert key in meta


def test_skip_patterns_cover_required_needles():
    """Each required needle (Buyer/Seller/Placed) must appear in some
    items.multiline.skip_patterns entry — proves the v1.1 fix landed."""
    spec = _load_spec()
    multiline = spec.get("items", {}).get("multiline", {})
    skip_patterns = multiline.get("skip_patterns", [])
    assert isinstance(skip_patterns, list)
    joined = "\n".join(str(p) for p in skip_patterns)
    for needle in MANIFEST["required_skip_needles"]:
        assert needle in joined


def test_problem_line_matches_a_skip_pattern():
    """The exact OCR line from HAWB9758348 must be skipped by at least one
    of the items.multiline.skip_patterns regexes."""
    spec = _load_spec()
    multiline = spec.get("items", {}).get("multiline", {})
    skip_patterns = multiline.get("skip_patterns", [])
    problem_line = load_text_fixture(FORMAT_NAME, MANIFEST["problem_line_fixture"]).strip()
    assert any(re.search(str(pat), problem_line) for pat in skip_patterns)


def test_format_parser_accepts_spec():
    spec = _load_spec()
    fp = FormatParser(spec)
    assert fp.name == MANIFEST["format_name"]


def test_registry_loads_and_detects():
    reg = FormatRegistry(as_str(REPO_ROOT))
    names = [f.get("name") for f in reg.formats]
    assert MANIFEST["format_name"] in names

    sample = load_text_fixture(FORMAT_NAME, MANIFEST["detect_sample_fixture"])
    match = reg.detect_format(sample)
    assert match is not None
    assert match["name"] == MANIFEST["format_name"]
