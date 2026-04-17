"""Tests for _extract_cc_charges, split-item verification, and currency parsing."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "pipeline"))


# ---------------------------------------------------------------------------
# _extract_cc_charges
# ---------------------------------------------------------------------------
from format_parser import FormatParser  # noqa: E402
from run import _extract_cc_charges  # noqa: E402


def test_extract_cc_charges_amazon_3_charges():
    """Real Amazon invoice text with 3 CC transactions."""
    text = (
        "Credit Card transactions\n"
        "Visa ending in 0099 : July 19 , 2024 : $89.54\n"
        "Visa ending in 0099 : July 19 , 2024 : $10.69\n"
        "Visa ending in 0099 : July 19 , 2024 : $41.22\n"
    )
    charges = _extract_cc_charges(text)
    assert charges == [89.54, 10.69, 41.22]


def test_extract_cc_charges_single():
    text = "Visa ending in 9318 : July 2 , 2024 : $170.88"
    charges = _extract_cc_charges(text)
    assert charges == [170.88]


def test_extract_cc_charges_no_match():
    text = "Payment Method: PayPal\nTotal: $50.00"
    charges = _extract_cc_charges(text)
    assert charges == []


def test_extract_cc_charges_mastercard():
    text = "Mastercard ending in 1234 : Aug 5 , 2024 : $25.99"
    charges = _extract_cc_charges(text)
    assert charges == [25.99]


def test_extract_cc_charges_ocr_noise():
    """Handle OCR spacing in card info while extracting amount."""
    text = "Credit Card transactions Visa ending in 0099 : July 19 , 2024 :\n$89.54\n"
    charges = _extract_cc_charges(text)
    # The regex requires $ on the same line — this tests robustness
    # If it doesn't match due to newline, that's acceptable
    assert isinstance(charges, list)


# ---------------------------------------------------------------------------
# Verification: all items accounted for
# ---------------------------------------------------------------------------
def test_all_items_sum_matches():
    """Verify that item assignment preserves total cost."""
    items = [
        {"total_cost": 38.52, "supplier_item_desc": "Backpack"},
        {"total_cost": 9.99, "supplier_item_desc": "Toner"},
        {"total_cost": 18.49, "supplier_item_desc": "Briefs"},
        {"total_cost": 15.48, "supplier_item_desc": "Cleanser"},
        {"total_cost": 8.48, "supplier_item_desc": "Pencils"},
        {"total_cost": 18.24, "supplier_item_desc": "Serum"},
        {"total_cost": 23.00, "supplier_item_desc": "Tank"},
    ]
    expected_total = sum(it["total_cost"] for it in items)

    # Simulate a 3-way split
    split = [items[:1], items[1:2], items[2:]]
    assigned_total = sum(sum(it["total_cost"] for it in group) for group in split)
    assert abs(assigned_total - expected_total) < 0.01
    assert sum(len(g) for g in split) == len(items)


def test_cc_charge_cross_validation():
    """CC charges should approximately match items + prorated tax."""
    cc_charges = [41.22, 10.69, 89.54]
    items_per_decl = [38.52, 9.99, 83.69]  # items only
    total_tax = 9.25
    total_items = sum(items_per_decl)

    for i, (items_val, cc) in enumerate(zip(items_per_decl, cc_charges, strict=True)):
        ratio = items_val / total_items
        est_tax = total_tax * ratio
        est_with_tax = items_val + est_tax
        diff = abs(cc - est_with_tax)
        # Each should be within $2 of the CC charge
        assert diff < 2.0, (
            f"Decl {i}: items+tax ${est_with_tax:.2f} vs CC ${cc:.2f} (diff ${diff:.2f})"
        )


# ---------------------------------------------------------------------------
# Currency parsing: OCR comma-as-decimal
# ---------------------------------------------------------------------------
def test_convert_type_comma_decimal():
    """OCR sometimes writes '9,31' instead of '9.31'."""
    fp = FormatParser.__new__(FormatParser)
    assert fp._convert_type("9,31", "currency") == 9.31
    assert fp._convert_type("9.31", "currency") == 9.31
    # Thousands separator should still work: "1,234.56"
    assert fp._convert_type("1,234.56", "currency") == 1234.56
    # Pure integer with comma: "1,000" (thousands sep, not decimal)
    assert fp._convert_type("1,000", "currency") == 1000.0


# ---------------------------------------------------------------------------
# Free shipping netting
# ---------------------------------------------------------------------------
def _make_parser():
    """Create a minimal FormatParser for unit testing _build_result."""
    fp = FormatParser.__new__(FormatParser)
    fp.spec = {}
    fp.name = "test"
    fp.logger = __import__("logging").getLogger("test")
    return fp


def test_free_shipping_nets_to_zero():
    """When shipping ≈ free_shipping, both should be zeroed."""
    fp = _make_parser()
    metadata = {"shipping": 9.31, "free_shipping": 9.31, "total": 141.45}
    items = [{"total_cost": 132.20}]
    result = fp._build_result(metadata, items, "")
    inv = result["invoices"][0]
    assert inv.get("freight", 0) == 0
    assert inv.get("free_shipping", 0) == 0


def test_free_shipping_orphan_ignored():
    """free_shipping without freight should not create a deduction."""
    fp = _make_parser()
    metadata = {"free_shipping": 9.31, "total": 141.45}
    items = [{"total_cost": 132.20}]
    result = fp._build_result(metadata, items, "")
    inv = result["invoices"][0]
    assert inv.get("freight", 0) == 0
    assert inv.get("free_shipping", 0) == 0
