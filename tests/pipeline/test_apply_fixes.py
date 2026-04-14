"""Phase 3 — Replay pipeline with reviewer-edited Proposed Fixes YAML.

These tests guard ``pipeline/apply_fixes.py``:

* ``load_fixes_yaml``   — parses + shallow-validates a patch file
* ``discover_fixes``    — only picks up ``proposed_fixes_*.yaml`` files
* ``build_fixes_map``   — flattens into ``{invoice_num: invoice_fixes}``
* ``apply_fixes_to_result`` — applies override, delete, add_items in place,
  recomputes residual, clears uncertainty when balanced
* ``archive_fixes_yaml`` — moves applied patch into learned_fixes/YYYY-MM/
"""

from __future__ import annotations

import os
import sys
from types import SimpleNamespace

import pytest
import yaml

_PIPELINE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "pipeline"))
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)

from apply_fixes import (  # noqa: E402
    apply_fixes_to_result,
    apply_fixes_to_results,
    archive_fixes_yaml,
    build_fixes_map,
    discover_fixes,
    load_fixes_yaml,
)

# ─── Fixtures ──────────────────────────────────────────────────────────


def _result_with_recovered_orphan() -> SimpleNamespace:
    """Fake pipeline result mirroring SHEIN invoice 8 after Phase 1 recovery.

    Items sum to 32.90 vs invoice_total 33.10 → residual $0.20 absorbed into
    the ADJUSTMENTS row, with a data_quality_notes annotation and SHEIN-4
    flagged as orphan_price_recovered.
    """
    return SimpleNamespace(
        invoice_num="INVUS20240728002522185",
        invoice_data={
            "invoice_num": "INVUS20240728002522185",
            "invoice_total": 33.10,
            "sub_total": 33.10,
            "data_quality_notes": ["orphan_price_recovered: +1 item $13.70"],
            "invoice_total_uncertain": True,
            "freight": 0.0,
            "tax": 0.0,
            "other_cost": 0.0,
            "credits": 0.0,
            "discount": 0.0,
            "free_shipping": 0.0,
        },
        matched_items=[
            {
                "supplier_item": "SHEIN-1",
                "supplier_item_desc": "Other item 1",
                "quantity": 1,
                "unit_price": 4.60,
                "total_cost": 4.60,
                "data_quality": "",
            },
            {
                "supplier_item": "SHEIN-2",
                "supplier_item_desc": "Other item 2",
                "quantity": 1,
                "unit_price": 7.54,
                "total_cost": 7.54,
                "data_quality": "",
            },
            {
                "supplier_item": "SHEIN-3",
                "supplier_item_desc": "Other item 3",
                "quantity": 1,
                "unit_price": 7.06,
                "total_cost": 7.06,
                "data_quality": "",
            },
            {
                "supplier_item": "SHEIN-4",
                "supplier_item_desc": "Recovered orphan",
                "quantity": 1,
                "unit_price": 13.70,
                "total_cost": 13.70,
                "data_quality": "orphan_price_recovered",
            },
        ],
        supplier_info={"name": "SHEIN US Services, LLC"},
        xlsx_path="",
        pdf_output_path="",
    )


def _fixes_doc(**overrides) -> dict:
    doc = {
        "waybill": "HAWBS665535",
        "replay_mode": True,
        "invoices": [
            {
                "invoice_num": "INVUS20240728002522185",
                "supplier": "SHEIN US Services, LLC",
                "current": {
                    "sub_total": 33.10,
                    "items_sum": 32.90,
                    "invoice_total": 33.10,
                    "residual": 0.20,
                },
                "recovered_items": [
                    {
                        "sku": "SHEIN-4",
                        "current": {"quantity": 1, "unit_cost": 13.70, "total_cost": 13.70},
                        "override": {
                            "quantity": None,
                            "unit_cost": None,
                            "total_cost": None,
                            "description": None,
                            "delete": False,
                        },
                    },
                ],
                "other_items": [],
                "add_items": [],
            }
        ],
    }
    doc.update(overrides)
    return doc


# ─── load_fixes_yaml + discover_fixes ──────────────────────────────────


def test_load_fixes_yaml_parses_valid_file(tmp_path):
    p = tmp_path / "proposed_fixes_X.yaml"
    p.write_text(yaml.safe_dump(_fixes_doc()))
    doc = load_fixes_yaml(str(p))
    assert doc["waybill"] == "HAWBS665535"
    assert doc["replay_mode"] is True
    assert len(doc["invoices"]) == 1


def test_load_fixes_yaml_rejects_non_mapping(tmp_path):
    p = tmp_path / "proposed_fixes_X.yaml"
    p.write_text("- just\n- a\n- list\n")
    with pytest.raises(ValueError, match="mapping"):
        load_fixes_yaml(str(p))


def test_load_fixes_yaml_rejects_missing_waybill(tmp_path):
    p = tmp_path / "proposed_fixes_X.yaml"
    p.write_text("invoices: []\n")
    with pytest.raises(ValueError, match="waybill"):
        load_fixes_yaml(str(p))


def test_discover_fixes_only_matches_pattern(tmp_path):
    (tmp_path / "proposed_fixes_ABC.yaml").write_text("waybill: ABC\ninvoices: []\n")
    (tmp_path / "proposed_fixes_XYZ.yaml").write_text("waybill: XYZ\ninvoices: []\n")
    (tmp_path / "random.yaml").write_text("foo: bar\n")  # wrong name
    (tmp_path / "proposed_fixes.txt").write_text("ignored\n")  # wrong extension
    paths = discover_fixes(str(tmp_path))
    names = sorted(os.path.basename(p) for p in paths)
    assert names == ["proposed_fixes_ABC.yaml", "proposed_fixes_XYZ.yaml"]


def test_discover_fixes_empty_and_missing_dir(tmp_path):
    assert discover_fixes("") == []
    assert discover_fixes(str(tmp_path / "does_not_exist")) == []
    assert discover_fixes(str(tmp_path)) == []


def test_build_fixes_map_flattens_by_invoice_num():
    docs = [_fixes_doc(), _fixes_doc()]
    # Second doc references a different invoice
    docs[1]["invoices"][0]["invoice_num"] = "INV_OTHER"
    m = build_fixes_map(docs)
    assert set(m.keys()) == {"INVUS20240728002522185", "INV_OTHER"}


# ─── apply_fixes_to_result — overrides ────────────────────────────────


def test_apply_override_updates_quantity_and_recomputes_total():
    r = _result_with_recovered_orphan()
    fixes = _fixes_doc()["invoices"][0]
    # Reviewer says qty=2 at unit=13.70 — total should recompute to 27.40
    fixes["recovered_items"][0]["override"] = {
        "quantity": 2,
        "unit_cost": 13.70,
        "total_cost": None,
        "description": None,
        "delete": False,
    }
    report = apply_fixes_to_result(r, fixes)
    sh4 = next(m for m in r.matched_items if m["supplier_item"] == "SHEIN-4")
    assert sh4["quantity"] == 2
    assert sh4["unit_price"] == 13.70
    assert sh4["total_cost"] == 27.40
    assert sh4["data_quality"] == ""  # uncertainty marker cleared
    assert report["items_updated"] == 1


def test_apply_override_delete_removes_item():
    r = _result_with_recovered_orphan()
    fixes = _fixes_doc()["invoices"][0]
    fixes["recovered_items"][0]["override"]["delete"] = True
    report = apply_fixes_to_result(r, fixes)
    skus = [m["supplier_item"] for m in r.matched_items]
    assert "SHEIN-4" not in skus
    assert report["items_deleted"] == 1
    # items_sum is now 19.20, residual = 33.10 - 19.20 = 13.90 — uncertainty remains
    assert abs(report["residual_after"] - 13.90) < 0.01


def test_apply_override_null_block_is_noop():
    r = _result_with_recovered_orphan()
    fixes = _fixes_doc()["invoices"][0]
    before = [dict(m) for m in r.matched_items]
    report = apply_fixes_to_result(r, fixes)
    assert report["items_updated"] == 0
    assert report["items_added"] == 0
    assert report["items_deleted"] == 0
    # matched_items unchanged except data_quality markers get cleared
    # by the residual reconciliation branch — check the numeric fields only
    for old, new in zip(before, r.matched_items, strict=True):
        assert old["quantity"] == new["quantity"]
        assert old["unit_price"] == new["unit_price"]
        assert old["total_cost"] == new["total_cost"]


def test_apply_override_clears_uncertainty_when_balanced():
    r = _result_with_recovered_orphan()
    # Start: items_sum=32.90, total=33.10, residual=0.20
    # Override SHEIN-4 to 13.90 → items_sum=33.10, residual=0.00
    fixes = _fixes_doc()["invoices"][0]
    fixes["recovered_items"][0]["override"]["total_cost"] = 13.90
    report = apply_fixes_to_result(r, fixes)
    assert abs(report["residual_after"]) < 0.01
    assert r.invoice_data["invoice_total_uncertain"] is False
    # Notes now reflect the reviewer action, not the orphan recovery
    notes = r.invoice_data["data_quality_notes"]
    assert any("Reviewer applied fixes" in n for n in notes)


def test_apply_override_description_update():
    r = _result_with_recovered_orphan()
    fixes = _fixes_doc()["invoices"][0]
    fixes["recovered_items"][0]["override"]["description"] = "Plus Size Round Neck Top"
    apply_fixes_to_result(r, fixes)
    sh4 = next(m for m in r.matched_items if m["supplier_item"] == "SHEIN-4")
    assert sh4["supplier_item_desc"] == "Plus Size Round Neck Top"


# ─── apply_fixes_to_result — add_items ────────────────────────────────


def test_apply_add_items_appends_new_row():
    r = _result_with_recovered_orphan()
    fixes = _fixes_doc()["invoices"][0]
    fixes["add_items"] = [
        {
            "sku": "SHEIN-NEW",
            "description": "Missed item",
            "quantity": 1,
            "unit_cost": 9.99,
            # total left off — should be recomputed from qty*unit
            "tariff_code": "61091000",
        }
    ]
    report = apply_fixes_to_result(r, fixes)
    new = next(m for m in r.matched_items if m["supplier_item"] == "SHEIN-NEW")
    assert new["quantity"] == 1
    assert new["unit_price"] == 9.99
    assert new["total_cost"] == 9.99
    assert new["tariff_code"] == "61091000"
    assert new["classification_source"] == "fixes_yaml"
    assert report["items_added"] == 1


# ─── apply_fixes_to_results (batch) ───────────────────────────────────


def test_apply_fixes_to_results_skips_invoices_without_match():
    r1 = _result_with_recovered_orphan()
    r2 = SimpleNamespace(
        invoice_num="INV_UNRELATED",
        invoice_data={"invoice_total": 10.00, "data_quality_notes": []},
        matched_items=[
            {"supplier_item": "X", "quantity": 1, "unit_price": 10.00, "total_cost": 10.00}
        ],
    )
    fixes_map = {"INVUS20240728002522185": _fixes_doc()["invoices"][0]}
    reports = apply_fixes_to_results([r1, r2], fixes_map)
    assert len(reports) == 1
    assert reports[0]["invoice_num"] == "INVUS20240728002522185"
    # r2 is untouched
    assert r2.matched_items[0]["total_cost"] == 10.00


# ─── archive_fixes_yaml ───────────────────────────────────────────────


def test_archive_fixes_yaml_moves_file_to_dated_dir(tmp_path):
    src = tmp_path / "proposed_fixes_HAWBS665535.yaml"
    src.write_text("waybill: HAWBS665535\ninvoices: []\n")
    base_dir = tmp_path / "project"
    base_dir.mkdir()
    dest = archive_fixes_yaml(str(src), "HAWBS665535", str(base_dir))
    assert dest is not None
    assert os.path.exists(dest)
    assert not os.path.exists(src)  # source was moved
    # Destination sits under data/learned_fixes/YYYY-MM/
    assert os.sep + "data" + os.sep + "learned_fixes" + os.sep in dest
    assert "HAWBS665535" in os.path.basename(dest)


def test_archive_fixes_yaml_handles_missing_source(tmp_path):
    assert archive_fixes_yaml(str(tmp_path / "missing.yaml"), "WB", str(tmp_path)) is None


def test_archive_fixes_yaml_sanitizes_waybill(tmp_path):
    src = tmp_path / "proposed_fixes_X.yaml"
    src.write_text("waybill: X\ninvoices: []\n")
    dest = archive_fixes_yaml(str(src), "HAWB S66/55", str(tmp_path))
    assert dest is not None
    assert "HAWB_S66_55" in os.path.basename(dest)
