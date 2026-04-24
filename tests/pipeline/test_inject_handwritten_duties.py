"""Regression tests for _inject_handwritten_duties.

Covers Fix A: multi-declaration shipments must NOT have a single decl's
handwritten customs value fanned out across every ``r.invoice_data`` —
that caused cross-shipment contamination (e.g. HAWB9600998's $8.96 was
leaking onto HAWB9603312's XLSX because both lived under one folder).
"""

import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "pipeline"))

from run import _inject_handwritten_duties  # noqa: E402


def _make_result(waybill: str = "HAWB9999999"):
    """Lightweight stand-in for a pipeline result object."""
    return SimpleNamespace(
        invoice_data={"waybill": waybill},
        xlsx_path=None,
        matched_items=[],
        supplier_info=None,
    )


# ---------------------------------------------------------------------------
# Single-decl (legacy dict) path
# ---------------------------------------------------------------------------


def test_single_decl_dict_with_ec_value_injects():
    """Legacy single-decl dict path: EC value is wired into invoice_data."""
    decl_meta = {"_customs_value_ec": "80.06", "freight": "5.00"}
    results = [_make_result("HAWB9603312")]

    injected = _inject_handwritten_duties(decl_meta, results)

    assert injected is True
    assert results[0].invoice_data["_client_declared_duties"] == 80.06
    assert results[0].invoice_data["_customs_freight"] == 5.00


def test_single_decl_dict_without_value_no_inject():
    """No handwritten value → nothing injected, returns False."""
    decl_meta = {"_customs_value_ec": "", "_customs_value_usd": ""}
    results = [_make_result("HAWB9603312")]

    injected = _inject_handwritten_duties(decl_meta, results)

    assert injected is False
    assert "_client_declared_duties" not in results[0].invoice_data


def test_single_decl_usd_converts_to_xcd():
    """USD-only value is converted using XCD_RATE=2.7169."""
    decl_meta = {"_customs_value_usd": "10.00"}
    results = [_make_result()]

    injected = _inject_handwritten_duties(decl_meta, results)

    assert injected is True
    # 10.00 * 2.7169 = 27.169 → round to 27.17
    assert results[0].invoice_data["_client_declared_duties"] == 27.17


# ---------------------------------------------------------------------------
# Multi-decl contamination guard (the Fix A regression gate)
# ---------------------------------------------------------------------------


def test_multi_decl_list_skipped_no_mutation():
    """Two decls → batch injector must NOT mutate ANY result (per-decl path handles it).

    This is the primary regression for Fix A: HAWB9600998's $8.96 was
    leaking onto HAWB9603312 because the old code fanned one decl's value
    out across every result. The new code must skip when len>1.
    """
    # Two declarations with DIFFERENT handwritten values.
    decl_list = [
        ({"_customs_value_ec": "8.96", "waybill": "HAWB9600998"}, "/tmp/decl1.pdf"),
        ({"_customs_value_ec": "80.06", "waybill": "HAWB9603312"}, "/tmp/decl2.pdf"),
    ]
    results = [
        _make_result("HAWB9600998"),
        _make_result("HAWB9603312"),
    ]

    injected = _inject_handwritten_duties(decl_list, results)

    # Must skip → False
    assert injected is False
    # And must NOT have mutated any invoice_data
    for r in results:
        assert "_client_declared_duties" not in r.invoice_data
        assert "_customs_freight" not in r.invoice_data


def test_single_decl_list_wrapper_still_injects():
    """list[(meta, path)] with exactly one decl should behave like the dict path."""
    decl_list = [({"_customs_value_ec": "80.06"}, "/tmp/decl.pdf")]
    results = [_make_result("HAWB9603312")]

    injected = _inject_handwritten_duties(decl_list, results)

    assert injected is True
    assert results[0].invoice_data["_client_declared_duties"] == 80.06


def test_empty_list_returns_false():
    """Empty declaration list → False, no mutation."""
    results = [_make_result()]

    injected = _inject_handwritten_duties([], results)

    assert injected is False
    assert "_client_declared_duties" not in results[0].invoice_data


def test_empty_results_returns_false():
    """Empty results list → False."""
    decl_meta = {"_customs_value_ec": "80.06"}

    injected = _inject_handwritten_duties(decl_meta, [])

    assert injected is False


def test_malformed_list_returns_false():
    """A list of plain dicts (not tuples) must not crash; returns False safely."""
    # Callers should pass [(meta, path), ...] but guard against accidents.
    decl_list = [{"_customs_value_ec": "80.06"}]  # not a tuple
    results = [_make_result()]

    injected = _inject_handwritten_duties(decl_list, results)

    assert injected is False
    assert "_client_declared_duties" not in results[0].invoice_data


def test_does_not_overwrite_existing_value():
    """If _client_declared_duties is already set, leave it alone."""
    decl_meta = {"_customs_value_ec": "80.06"}
    r = _make_result()
    r.invoice_data["_client_declared_duties"] = 42.00  # pre-existing (e.g. manual)

    injected = _inject_handwritten_duties(decl_meta, [r])

    assert injected is False
    assert r.invoice_data["_client_declared_duties"] == 42.00
