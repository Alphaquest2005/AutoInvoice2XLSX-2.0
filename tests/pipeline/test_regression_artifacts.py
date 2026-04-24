"""Unit tests for tests/pipeline/_regression_artifacts.py.

These exercise the snapshot/diff helper without touching the slow Downloads
pipeline run. The helper is the backbone of Joseph's regression-tracking
workflow, so breaking its invariants silently would undermine the whole
downloads test.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from openpyxl import Workbook

from tests.pipeline import _regression_artifacts as ra


@pytest.fixture
def isolated_baseline_dir(tmp_path, monkeypatch):
    """Redirect the baseline dir to a throwaway tmp_path."""
    monkeypatch.setattr(ra, "_BASELINE_DIR", tmp_path / "baselines")
    monkeypatch.setattr(ra, "UPDATE_GOLDENS", False)
    return tmp_path


def _make_xlsx(path: Path, cells: dict[str, object]) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"
    for coord, value in cells.items():
        ws[coord] = value
    wb.save(str(path))
    wb.close()


def _make_email_params(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data), encoding="utf-8")


def test_new_baseline_on_first_run(isolated_baseline_dir, tmp_path):
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    _make_xlsx(output_dir / "INV_001.xlsx", {"A1": "Item", "B1": 42})

    result = ra.snapshot_and_compare("INV_001", output_dir)

    assert result["status"] == "new"
    assert Path(result["baseline_path"]).exists()
    assert result["diffs"] == []


def test_match_on_identical_run(isolated_baseline_dir, tmp_path):
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    _make_xlsx(output_dir / "INV_001.xlsx", {"A1": "Item", "B1": 42})

    ra.snapshot_and_compare("INV_001", output_dir)  # seed baseline
    result = ra.snapshot_and_compare("INV_001", output_dir)

    assert result["status"] == "match"
    assert result["diffs"] == []


def test_drift_reported_on_cell_change(isolated_baseline_dir, tmp_path):
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    xlsx = output_dir / "INV_001.xlsx"
    _make_xlsx(xlsx, {"A1": "Item", "B1": 42})
    ra.snapshot_and_compare("INV_001", output_dir)

    # Regenerate with a changed value — simulates a pipeline regression.
    _make_xlsx(xlsx, {"A1": "Item", "B1": 43})
    result = ra.snapshot_and_compare("INV_001", output_dir)

    assert result["status"] == "drift"
    assert any("B1" in d for d in result["diffs"]), result["diffs"]
    # Current artifacts saved for Excel-side-by-side inspection.
    assert Path(result["current_dir"]).exists()
    assert (Path(result["current_dir"]) / "INV_001.xlsx").exists()


def test_formula_vs_value_do_not_collide(isolated_baseline_dir, tmp_path):
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    xlsx = output_dir / "INV_001.xlsx"
    _make_xlsx(xlsx, {"A1": 1, "B1": 2, "C1": 3})
    ra.snapshot_and_compare("INV_001", output_dir)

    # A hardcoded 3 in C1 must NOT match =A1+B1, even if both display "3".
    # This is Law L2 — variance must be a formula, not a hardcoded zero.
    _make_xlsx(xlsx, {"A1": 1, "B1": 2, "C1": "=A1+B1"})
    result = ra.snapshot_and_compare("INV_001", output_dir)
    assert result["status"] == "drift"
    assert any("C1" in d for d in result["diffs"])


def test_email_params_attachment_path_noise_is_ignored(isolated_baseline_dir, tmp_path):
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    _make_email_params(
        output_dir / "_email_params.json",
        {
            "waybill": "ABC123",
            "attachment_paths": ["/tmp/runA/INV.xlsx", "/tmp/runA/INV.pdf"],
        },
    )
    ra.snapshot_and_compare("EMAIL", output_dir)

    _make_email_params(
        output_dir / "_email_params.json",
        {
            "waybill": "ABC123",
            "attachment_paths": ["/tmp/runB/INV.pdf", "/tmp/runB/INV.xlsx"],
        },
    )
    result = ra.snapshot_and_compare("EMAIL", output_dir)
    # Different full paths + reordering but same basenames → no drift.
    assert result["status"] == "match"


def test_email_params_waybill_change_is_drift(isolated_baseline_dir, tmp_path):
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    _make_email_params(output_dir / "_email_params.json", {"waybill": "ABC123"})
    ra.snapshot_and_compare("EMAIL", output_dir)

    _make_email_params(output_dir / "_email_params.json", {"waybill": "XYZ789"})
    result = ra.snapshot_and_compare("EMAIL", output_dir)
    assert result["status"] == "drift"


def test_update_goldens_promotes_drift(tmp_path, monkeypatch):
    monkeypatch.setattr(ra, "_BASELINE_DIR", tmp_path / "baselines")
    monkeypatch.setattr(ra, "UPDATE_GOLDENS", True)

    output_dir = tmp_path / "out"
    output_dir.mkdir()
    xlsx = output_dir / "INV_001.xlsx"
    _make_xlsx(xlsx, {"A1": 1})

    # Seed baseline WITHOUT promotion (fresh) then change.
    monkeypatch.setattr(ra, "UPDATE_GOLDENS", False)
    ra.snapshot_and_compare("INV_001", output_dir)

    _make_xlsx(xlsx, {"A1": 2})
    monkeypatch.setattr(ra, "UPDATE_GOLDENS", True)
    result = ra.snapshot_and_compare("INV_001", output_dir)

    assert result["status"] == "promoted"
    # Next run against the promoted baseline must match.
    monkeypatch.setattr(ra, "UPDATE_GOLDENS", False)
    next_result = ra.snapshot_and_compare("INV_001", output_dir)
    assert next_result["status"] == "match"
    # Diff audit trail preserved.
    assert (tmp_path / "baselines" / "INV_001.last_diff.json").exists()


def test_repeatable_hash(isolated_baseline_dir, tmp_path):
    """Two fresh runs of the same output yield the same hash — the
    cornerstone of repeatability."""
    output_a = tmp_path / "a"
    output_b = tmp_path / "b"
    output_a.mkdir()
    output_b.mkdir()
    _make_xlsx(output_a / "INV.xlsx", {"A1": 1, "B1": "=A1*2"})
    _make_xlsx(output_b / "INV.xlsx", {"A1": 1, "B1": "=A1*2"})

    ra.snapshot_and_compare("X", output_a)
    # Swap baseline dir target to avoid collision, compare hashes directly.
    snap_a = ra._build_snapshot(output_a)
    snap_b = ra._build_snapshot(output_b)
    assert ra._hash_snapshot(snap_a) == ra._hash_snapshot(snap_b)
