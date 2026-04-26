"""SSOT for filesystem paths used by tests.

Reads the canonical directory names from config/repo_layout.yaml via
pipeline.repo_layout.load_repo_layout(). All test files import constants
from here instead of inlining 'config'/'formats'/etc. literals.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# tests/_paths.py → tests/ → repo root.
TESTS_DIR: Path = Path(__file__).resolve().parent
REPO_ROOT: Path = TESTS_DIR.parent

# Make the pipeline package importable so we can pull the layout loader.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipeline.repo_layout import load_repo_layout  # noqa: E402

_LAYOUT = load_repo_layout()
_SRC = _LAYOUT.get("source_dirs", {})
_CFG_SUB = _LAYOUT.get("config_subdirs", {})
_TEST_SUB = _LAYOUT.get("test_subdirs", {})
_SUFFIX = _LAYOUT.get("file_suffixes", {})
_TEST_FILES = _LAYOUT.get("test_files", {})

# Source-tree directories.
PIPELINE_DIR: Path = REPO_ROOT / _SRC["pipeline"]
CONFIG_DIR: Path = REPO_ROOT / _SRC["config"]
CONFIG_FORMATS_DIR: Path = CONFIG_DIR / _CFG_SUB["formats"]
PROMPTS_DIR: Path = REPO_ROOT / _SRC["prompts"]
SCRIPTS_DIR: Path = REPO_ROOT / _SRC["scripts"]

# Test-tree directories.
FIXTURES_DIR: Path = TESTS_DIR / _TEST_SUB["fixtures"]
REGRESSION_ARTIFACTS_DIR: Path = TESTS_DIR / _TEST_SUB["regression_artifacts"]

# File suffixes.
YAML_SUFFIX: str = _SUFFIX["yaml"]
JSON_SUFFIX: str = _SUFFIX["json"]

# Per-format fixture manifest filename.
FIXTURE_MANIFEST_NAME: str = _TEST_FILES["fixture_manifest"]

# Per-format test module filename convention (test_<format>_format.py).
_FORMAT_TEST_PREFIX: str = _TEST_FILES["format_test_prefix"]
_FORMAT_TEST_SUFFIX: str = _TEST_FILES["format_test_suffix"]


def add_pipeline_to_sys_path() -> None:
    """Insert pipeline/ on sys.path so legacy tests can do bare imports
    like `from format_parser import FormatParser`."""
    p = str(PIPELINE_DIR)
    if p not in sys.path:
        sys.path.insert(0, p)


def format_spec_path(format_name: str) -> Path:
    """Return the absolute path to a format YAML under config/formats/."""
    return CONFIG_FORMATS_DIR / f"{format_name}{YAML_SUFFIX}"


def fixture_path(format_name: str, fixture_name: str) -> Path:
    """Return the absolute path to a fixture file for one format."""
    return FIXTURES_DIR / format_name / fixture_name


def as_str(path: Path) -> str:
    return os.fspath(path)


def format_name_from_test_file(test_file: str) -> str:
    """Derive the target format name from a test module's __file__ path.

    Convention: tests/pipeline/<format_test_prefix><format><format_test_suffix>.py
    e.g. ``tests/pipeline/test_ebay_format.py`` -> ``"ebay"``.
    Lets per-format regression tests avoid hard-coding the format name as a
    string literal in their own source.
    """
    stem = Path(test_file).stem
    if stem.startswith(_FORMAT_TEST_PREFIX):
        stem = stem[len(_FORMAT_TEST_PREFIX) :]
    if stem.endswith(_FORMAT_TEST_SUFFIX):
        stem = stem[: -len(_FORMAT_TEST_SUFFIX)]
    return stem
