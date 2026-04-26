"""SSOT for loading test fixture files.

Tests import ``load_text_fixture`` / ``load_json_fixture`` instead of
inlining sample OCR text or expected JSON in Python source. The
fixture files live at ``tests/fixtures/<format>/<name>``.
"""

from __future__ import annotations

import json
from typing import Any

import yaml

from tests._paths import FIXTURE_MANIFEST_NAME, fixture_path


def load_text_fixture(format_name: str, fixture_name: str) -> str:
    """Read a UTF-8 text fixture and return its contents."""
    return fixture_path(format_name, fixture_name).read_text(encoding="utf-8")


def load_json_fixture(format_name: str, fixture_name: str) -> Any:
    """Read a JSON fixture and return the parsed object."""
    with fixture_path(format_name, fixture_name).open(encoding="utf-8") as fh:
        return json.load(fh)


def fixture_exists(format_name: str, fixture_name: str) -> bool:
    return fixture_path(format_name, fixture_name).is_file()


def load_test_manifest(format_name: str) -> dict:
    """Load tests/fixtures/<format>/<FIXTURE_MANIFEST_NAME> as a dict.

    The manifest declares the per-format expected values (min_version,
    required_meta_keys, required_skip_needles, fixture filenames) so the
    test source itself stays free of policy literals.
    """
    path = fixture_path(format_name, FIXTURE_MANIFEST_NAME)
    with path.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict):
        raise ValueError(f"manifest at {path} did not parse to a dict (got {type(data).__name__})")
    return data
