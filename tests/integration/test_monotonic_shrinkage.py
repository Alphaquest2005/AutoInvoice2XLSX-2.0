"""Monotonic size guard for the strangler-fig migration.

Enforces two simple invariants called out in ``docs/V2_MIGRATION_PLAN.md`` §5:

* ``wc -l pipeline/run.py`` — strictly non-increasing.
* ``find pipeline -name '*.py' | wc -l`` — strictly non-increasing.
* ``find src/autoinvoice -name '*.py' | wc -l`` — strictly non-decreasing.

The baseline lives at ``tests/integration/_monotonic_baseline.json`` and is
updated by ``scripts/update_monotonic_baseline.py`` **only** when the
direction is correct (down for pipeline_*, up for src_*). Manually relaxing
the numbers is exactly the drift the test is there to catch, so anyone
loosening them in a commit triggers review.

Why a test (not a hook)
-----------------------
Hooks are per-machine; the test fails in CI for everyone. A PR that reverts
the SSOT category migration or adds a new ``fix_*.py`` helper trips this
immediately.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_BASELINE = _REPO_ROOT / "tests" / "integration" / "_monotonic_baseline.json"


def _count_py_files(root: Path) -> int:
    if not root.exists():
        return 0
    return sum(
        1 for p in root.rglob("*.py") if "__pycache__" not in p.parts
    )


def _loc(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for _ in path.open(encoding="utf-8", errors="ignore"))


@pytest.fixture(scope="module")
def baseline() -> dict:
    return json.loads(_BASELINE.read_text(encoding="utf-8"))


def test_pipeline_run_py_loc_non_increasing(baseline: dict) -> None:
    """pipeline/run.py must shrink monotonically toward deletion."""
    current = _loc(_REPO_ROOT / "pipeline" / "run.py")
    limit = baseline["pipeline_run_py_loc"]
    assert current <= limit, (
        f"pipeline/run.py grew: baseline={limit} LOC, current={current} LOC. "
        "New logic MUST land in src/autoinvoice/, not pipeline/run.py. "
        "To ratchet the baseline DOWN after a legitimate shrink, run "
        "scripts/update_monotonic_baseline.py."
    )


def test_pipeline_py_file_count_non_increasing(baseline: dict) -> None:
    """Number of Python files under pipeline/ must only decrease."""
    current = _count_py_files(_REPO_ROOT / "pipeline")
    limit = baseline["pipeline_py_file_count"]
    assert current <= limit, (
        f"pipeline/ gained files: baseline={limit}, current={current}. "
        "Adding another fix_*.py / review_*.py / apply_*.py is exactly the "
        "drift the strangler-fig is meant to reverse. Put new code in "
        "src/autoinvoice/."
    )


def test_src_autoinvoice_py_file_count_non_decreasing(baseline: dict) -> None:
    """src/autoinvoice/ must only grow."""
    current = _count_py_files(_REPO_ROOT / "src" / "autoinvoice")
    floor = baseline["src_autoinvoice_py_file_count"]
    assert current >= floor, (
        f"src/autoinvoice/ lost files: baseline={floor}, current={current}. "
        "Retiring the v2 home reverses the migration. Fix by re-adding the "
        "deleted modules or updating the baseline with a documented reason."
    )
