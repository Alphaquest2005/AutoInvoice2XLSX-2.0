"""Tests for reverse-calc-driven item allocation in _split_items_per_declaration.

Per feedback_reverse_calc_allocation: for multi-waybill shipments sharing
one source invoice, the handwritten declared duty value on each Simplified
Declaration is the authoritative signal for item allocation. The splitter
reverse-calculates each duty → implied items USD and uses those as the
subset-sum target.

Reference case: H&M receipt with HAWB9600998 ($8.96 duty) + HAWB9603312
($80.06 duty). At 20% CET the implied items are ~$2/$57 — so the big
items should go to 9603312, not get round-robin split.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "pipeline"))

from run import (  # noqa: E402
    _avg_cet_rate_for_items,
    _implied_items_usd_from_duty,
    _split_items_per_declaration,
)
from stages.invoice_processor import InvoiceResult  # noqa: E402

# ---------------------------------------------------------------------------
# Reverse-calc helper
# ---------------------------------------------------------------------------


def test_implied_items_matches_memory_reference_8_96():
    """Declared $8.96 @ 20% CET, $5 freight → items ≈ $2.35 USD.
    From feedback_reverse_calc_allocation reference math."""
    items_usd = _implied_items_usd_from_duty(8.96, 0.20, freight_usd=5.0)
    assert 2.0 < items_usd < 2.7


def test_implied_items_matches_memory_reference_80_06():
    """Declared $80.06 @ 20% CET, $9 freight → items ≈ $56.63 USD."""
    items_usd = _implied_items_usd_from_duty(80.06, 0.20, freight_usd=9.0)
    assert 55.0 < items_usd < 58.0


def test_implied_items_floored_at_zero():
    """When freight exceeds implied CIF, items_usd = 0 (never negative)."""
    items_usd = _implied_items_usd_from_duty(1.0, 0.20, freight_usd=100.0)
    assert items_usd == 0.0


def test_implied_items_zero_when_no_duty():
    assert _implied_items_usd_from_duty(0, 0.20) == 0.0
    assert _implied_items_usd_from_duty(-5, 0.20) == 0.0


def test_implied_items_guards_bad_cet_rate():
    """Composite formula with cet_rate that makes composite ≤ 0 → 0."""
    # 1.15 * (-0.3) + 0.219 = -0.126 < 0
    assert _implied_items_usd_from_duty(10, -0.3) == 0.0


# ---------------------------------------------------------------------------
# Avg CET rate helper
# ---------------------------------------------------------------------------


def test_avg_cet_rate_defaults_to_20pct_for_unknown_codes():
    items = [{"tariff_code": "99999999", "total_cost": 100.0}]
    rate = _avg_cet_rate_for_items(items)
    # Unknown code → default 0.20
    assert rate == pytest.approx(0.20, abs=0.01)


def test_avg_cet_rate_defaults_for_empty_items():
    assert _avg_cet_rate_for_items([]) == 0.20


# ---------------------------------------------------------------------------
# End-to-end splitter behaviour — XLSX generation is monkey-patched out
# ---------------------------------------------------------------------------


def _make_result(items, invoice_data=None):
    r = InvoiceResult(
        pdf_file="test.pdf",
        invoice_num="INV-TEST",
        invoice_data=invoice_data or {"invoice_total": sum(i["total_cost"] for i in items)},
        matched_items=items,
        supplier_info={"name": "Test Supplier", "code": "T01"},
        xlsx_path="",
        pdf_output_path="",
        classified_count=len(items),
        matched_count=len(items),
        format_name="test",
    )
    return r


@pytest.fixture
def stub_xlsx(monkeypatch):
    """Replace bl_xlsx_generator.generate_bl_xlsx with a no-op that records
    per-decl invocations so tests can inspect items actually assigned."""
    calls = []

    def fake_generate(*args, **kwargs):
        # Record the items passed in — first positional arg varies, so
        # accept flexibly via kwargs or positional.
        items = kwargs.get("matched_items")
        if items is None and len(args) > 0:
            # try common positions
            for a in args:
                if isinstance(a, list) and a and isinstance(a[0], dict) and "total_cost" in a[0]:
                    items = a
                    break
        calls.append({"items": items or [], "kwargs": kwargs})
        return "/tmp/fake.xlsx"

    import bl_xlsx_generator

    monkeypatch.setattr(bl_xlsx_generator, "generate_bl_xlsx", fake_generate)
    return calls


def test_reverse_calc_splits_hm_receipt_correctly(stub_xlsx, tmp_path):
    """Reference case: 3 items, declared duties [$8.96, $80.06].

    Reverse-calc → implied items ≈ [$2, $57]. The small item ($5.99)
    should go to 9600998; the big items ($41.98 + $12.99) to 9603312.
    """
    all_items = [
        {"tariff_code": "62034220", "total_cost": 41.98, "supplier_item_desc": "Shirt"},
        {"tariff_code": "65050000", "total_cost": 12.99, "supplier_item_desc": "Hat"},
        {"tariff_code": "00000000", "total_cost": 5.99, "supplier_item_desc": "Small"},
    ]
    results = [
        _make_result(
            all_items,
            {
                "invoice_total": 60.96,
                "_customs_freight": 14.0,  # split 5/9 across two decls
                "_customs_insurance": 0,
            },
        )
    ]
    all_declarations = [
        ({"waybill": "HAWB9600998", "_customs_value_ec": "8.96", "freight": "5"}, "a.pdf"),
        ({"waybill": "HAWB9603312", "_customs_value_ec": "80.06", "freight": "9"}, "b.pdf"),
    ]

    per_decl = _split_items_per_declaration(
        all_declarations, results, str(tmp_path), document_type="auto"
    )

    assert len(per_decl) == 2
    # HAWB9600998 should have the small item; HAWB9603312 should have the bigger ones
    d0 = per_decl[0]
    d1 = per_decl[1]
    assert d0["decl_meta"]["waybill"] == "HAWB9600998"
    assert d1["decl_meta"]["waybill"] == "HAWB9603312"

    d0_total = sum(it["total_cost"] for it in d0["items"])
    d1_total = sum(it["total_cost"] for it in d1["items"])
    # 9600998 (small target ~$2) must have less than 9603312 (target ~$57)
    assert d0_total < d1_total
    # Total preserved
    assert abs((d0_total + d1_total) - 60.96) < 0.05


def test_reverse_calc_with_usd_duty_field(stub_xlsx, tmp_path):
    """When LLM puts duty in _customs_value_usd (treated as USD), it gets
    converted to XCD internally and still reverse-calcs correctly."""
    all_items = [
        {"tariff_code": "62034220", "total_cost": 50.0, "supplier_item_desc": "Big"},
        {"tariff_code": "62034220", "total_cost": 3.0, "supplier_item_desc": "Small"},
    ]
    results = [
        _make_result(
            all_items,
            {
                "invoice_total": 53.0,
                "_customs_freight": 0,
                "_customs_insurance": 0,
            },
        )
    ]
    # USD duty ≈ 3.30 → XCD ≈ 8.96 (matches ec form of reference case)
    all_declarations = [
        ({"waybill": "W1", "_customs_value_usd": "3.30", "freight": "0"}, "a.pdf"),
        ({"waybill": "W2", "_customs_value_ec": "80.06", "freight": "0"}, "b.pdf"),
    ]

    per_decl = _split_items_per_declaration(
        all_declarations, results, str(tmp_path), document_type="auto"
    )

    assert len(per_decl) == 2
    d0_total = sum(it["total_cost"] for it in per_decl[0]["items"])
    d1_total = sum(it["total_cost"] for it in per_decl[1]["items"])
    # Small target (W1) < big target (W2)
    assert d0_total < d1_total


def test_no_duty_values_falls_back_to_other_strategy(stub_xlsx, tmp_path):
    """Without any handwritten duties, Strategy 1 returns None targets and
    we fall through to Strategy 2 (quantity-aware) or 3 (round-robin)."""
    all_items = [
        {"tariff_code": "62034220", "total_cost": 10.0, "quantity": 1},
        {"tariff_code": "62034220", "total_cost": 20.0, "quantity": 1},
    ]
    results = [_make_result(all_items)]
    all_declarations = [
        ({"waybill": "W1"}, "a.pdf"),
        ({"waybill": "W2"}, "b.pdf"),
    ]

    per_decl = _split_items_per_declaration(
        all_declarations, results, str(tmp_path), document_type="auto"
    )
    # Should still produce 2 declarations (round-robin split)
    assert len(per_decl) == 2
    # All items preserved
    total_items = sum(len(d["items"]) for d in per_decl)
    assert total_items == 2


def test_single_declaration_returns_empty(stub_xlsx, tmp_path):
    """Single declaration = no split needed."""
    results = [_make_result([{"tariff_code": "62034220", "total_cost": 10.0}])]
    all_declarations = [({"waybill": "W1"}, "a.pdf")]
    per_decl = _split_items_per_declaration(
        all_declarations, results, str(tmp_path), document_type="auto"
    )
    assert per_decl == []


def test_missing_per_bl_freight_uses_zero_and_warns(stub_xlsx, tmp_path, caplog):
    """When a declaration has no 'freight' key, reverse-calc uses 0 (never
    borrows from another BL) and logs a warning. The implied target will be
    overstated by the true freight, but we never smuggle cross-BL data."""
    import logging as _logging

    all_items = [
        {"tariff_code": "62034220", "total_cost": 50.0},
        {"tariff_code": "62034220", "total_cost": 3.0},
    ]
    results = [
        _make_result(
            all_items,
            {
                "invoice_total": 53.0,
                "_customs_freight": 14.0,  # invoice-level total, should NOT leak per-BL
                "_customs_insurance": 0,
            },
        )
    ]
    # Neither declaration has a 'freight' field
    all_declarations = [
        ({"waybill": "W1", "_customs_value_ec": "8.96"}, "a.pdf"),
        ({"waybill": "W2", "_customs_value_ec": "80.06"}, "b.pdf"),
    ]

    with caplog.at_level(_logging.WARNING, logger="run"):
        per_decl = _split_items_per_declaration(
            all_declarations, results, str(tmp_path), document_type="auto"
        )

    # Must warn for BOTH declarations
    warnings = [r.message for r in caplog.records if "no per-BL freight" in r.message]
    assert any("W1" in w for w in warnings)
    assert any("W2" in w for w in warnings)
    assert len(per_decl) == 2


def test_reverse_calc_survives_unknown_tariff(stub_xlsx, tmp_path):
    """Unknown tariff codes default to 20% CET — reverse-calc still runs."""
    all_items = [
        {"tariff_code": "99999999", "total_cost": 50.0},
        {"tariff_code": "99999999", "total_cost": 3.0},
    ]
    results = [
        _make_result(
            all_items,
            {
                "invoice_total": 53.0,
                "_customs_freight": 0,
                "_customs_insurance": 0,
            },
        )
    ]
    all_declarations = [
        ({"waybill": "W1", "_customs_value_ec": "8.96"}, "a.pdf"),
        ({"waybill": "W2", "_customs_value_ec": "80.06"}, "b.pdf"),
    ]
    per_decl = _split_items_per_declaration(
        all_declarations, results, str(tmp_path), document_type="auto"
    )
    assert len(per_decl) == 2
    d0 = sum(it["total_cost"] for it in per_decl[0]["items"])
    d1 = sum(it["total_cost"] for it in per_decl[1]["items"])
    assert d0 < d1
