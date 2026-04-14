"""Phase 4 — Learned fixes candidate journal + review CLI.

These tests guard:

* ``apply_fixes.log_fix_candidates`` — appends one JSONL record per
  applied fix to ``data/learned_fixes/candidates/YYYY-MM/YYYY-MM-DD.jsonl``,
  embedding enough context (format, supplier, residual, change list,
  item snapshot) to let a human decide whether to promote the fix into
  a format spec later.
* ``review_candidates.load_candidates`` / ``summarise`` — can read a
  populated journal back and roll it up by format/supplier/change kind.
"""

from __future__ import annotations

import json
import os
import sys
from types import SimpleNamespace

_PIPELINE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "pipeline"))
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)

import apply_fixes  # noqa: E402
import review_candidates  # noqa: E402

# ─── Fixtures ──────────────────────────────────────────────────────────


def _result(invoice_num: str, format_name: str, supplier: str) -> SimpleNamespace:
    return SimpleNamespace(
        invoice_num=invoice_num,
        format_name=format_name,
        supplier_info={"name": supplier},
        matched_items=[
            {
                "supplier_item": "SKU-A",
                "supplier_item_desc": "A",
                "quantity": 1,
                "unit_price": 10.0,
                "total_cost": 10.0,
            },
        ],
        invoice_data={"invoice_total": 10.0},
    )


def _report(invoice_num: str, **kwargs) -> dict:
    base = {
        "invoice_num": invoice_num,
        "items_updated": 1,
        "items_added": 0,
        "items_deleted": 0,
        "residual_after": 0.0,
        "changes": ["SKU-A: qty 1 → 2"],
    }
    base.update(kwargs)
    return base


# ─── log_fix_candidates ───────────────────────────────────────────────


def test_log_fix_candidates_writes_jsonl(tmp_path):
    r = _result("INV001", "shein_us_invoice", "SHEIN US Services, LLC")
    rep = _report("INV001")

    path = apply_fixes.log_fix_candidates([rep], [r], waybill="HAWBS665535", base_dir=str(tmp_path))

    assert path is not None
    assert os.path.exists(path)
    # Path sits under data/learned_fixes/candidates/YYYY-MM/YYYY-MM-DD.jsonl
    assert os.sep + "data" + os.sep + "learned_fixes" + os.sep + "candidates" + os.sep in path
    assert path.endswith(".jsonl")

    # Read back the one record we wrote
    with open(path) as f:
        lines = [ln for ln in f.read().splitlines() if ln.strip()]
    assert len(lines) == 1
    rec = json.loads(lines[0])
    assert rec["waybill"] == "HAWBS665535"
    assert rec["invoice_num"] == "INV001"
    assert rec["format_name"] == "shein_us_invoice"
    assert rec["supplier"] == "SHEIN US Services, LLC"
    assert rec["items_updated"] == 1
    assert rec["changes"] == ["SKU-A: qty 1 → 2"]
    assert len(rec["items_snapshot"]) == 1
    assert rec["items_snapshot"][0]["sku"] == "SKU-A"


def test_log_fix_candidates_appends_on_repeat_calls(tmp_path):
    r = _result("INV001", "fmt_a", "Supplier A")
    apply_fixes.log_fix_candidates([_report("INV001")], [r], waybill="W", base_dir=str(tmp_path))
    apply_fixes.log_fix_candidates([_report("INV001")], [r], waybill="W", base_dir=str(tmp_path))

    # Both writes land in the same daily file → 2 JSONL lines
    records = review_candidates.load_candidates(base_dir=str(tmp_path))
    assert len(records) == 2


def test_log_fix_candidates_noop_on_empty_reports(tmp_path):
    assert apply_fixes.log_fix_candidates([], [], waybill="W", base_dir=str(tmp_path)) is None
    # No journal files were created
    root = tmp_path / "data" / "learned_fixes" / "candidates"
    assert not root.exists() or not any(root.rglob("*.jsonl"))


def test_log_fix_candidates_handles_missing_result(tmp_path):
    """A report for an invoice that isn't in the results list still logs,
    just with empty format/supplier fields."""
    rep = _report("INV_MISSING")
    apply_fixes.log_fix_candidates([rep], [], waybill="W", base_dir=str(tmp_path))
    records = review_candidates.load_candidates(base_dir=str(tmp_path))
    assert len(records) == 1
    assert records[0]["format_name"] == ""
    assert records[0]["supplier"] == ""


# ─── review_candidates.summarise ──────────────────────────────────────


def test_summarise_rolls_up_by_format_and_supplier(tmp_path):
    reports = [
        _report("INV1", items_updated=2),
        _report("INV2", items_updated=1, items_added=1),
        _report("INV3", items_updated=0, items_deleted=1),
    ]
    results = [
        _result("INV1", "shein_us_invoice", "SHEIN US Services, LLC"),
        _result("INV2", "shein_us_invoice", "SHEIN US Services, LLC"),
        _result("INV3", "alibaba_marketplace_invoice", "Other LLC"),
    ]
    for rep, r in zip(reports, results, strict=True):
        apply_fixes.log_fix_candidates([rep], [r], waybill="W", base_dir=str(tmp_path))

    records = review_candidates.load_candidates(base_dir=str(tmp_path))
    summary = review_candidates.summarise(records)
    assert summary["total_records"] == 3
    assert summary["totals"]["updated"] == 3
    assert summary["totals"]["added"] == 1
    assert summary["totals"]["deleted"] == 1

    fmt = dict(summary["by_format"])
    assert fmt["shein_us_invoice"] == 2
    assert fmt["alibaba_marketplace_invoice"] == 1

    sup = dict(summary["by_supplier"])
    assert sup["SHEIN US Services, LLC"] == 2


def test_summarise_empty_is_safe():
    summary = review_candidates.summarise([])
    assert summary["total_records"] == 0
    assert summary["totals"] == {"updated": 0, "added": 0, "deleted": 0}
    assert summary["by_format"] == []


def test_review_cli_main_prints_summary(tmp_path, capsys):
    r = _result("INV1", "fmt_x", "Supplier X")
    apply_fixes.log_fix_candidates([_report("INV1")], [r], waybill="W", base_dir=str(tmp_path))

    rc = review_candidates.main(["--base-dir", str(tmp_path)])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Total fix records: 1" in out
    assert "fmt_x" in out
    assert "Supplier X" in out


def test_review_cli_format_filter(tmp_path, capsys):
    apply_fixes.log_fix_candidates(
        [_report("INV1")],
        [_result("INV1", "fmt_a", "A")],
        waybill="W",
        base_dir=str(tmp_path),
    )
    apply_fixes.log_fix_candidates(
        [_report("INV2")],
        [_result("INV2", "fmt_b", "B")],
        waybill="W",
        base_dir=str(tmp_path),
    )

    rc = review_candidates.main(
        [
            "--base-dir",
            str(tmp_path),
            "--format",
            "fmt_a",
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "Total fix records: 1" in out
    assert "fmt_a" in out
    assert "fmt_b" not in out


def test_review_cli_json_output(tmp_path, capsys):
    r = _result("INV1", "fmt_x", "Supplier X")
    apply_fixes.log_fix_candidates(
        [_report("INV1")],
        [r],
        waybill="W",
        base_dir=str(tmp_path),
    )
    rc = review_candidates.main(
        [
            "--base-dir",
            str(tmp_path),
            "--json",
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["total_records"] == 1
    assert data["totals"]["updated"] == 1
