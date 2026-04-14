"""Structural item count detection tests.

Verifies that FormatParser._count_structural_items() reliably counts
price-bearing lines between section markers and compares against the
parser's extracted item count to flag mismatches.
"""

from __future__ import annotations

import os
import sys

import yaml

_PIPELINE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "pipeline"))
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)

from format_parser import FormatParser  # noqa: E402

SHEIN_SPEC_PATH = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "config",
        "formats",
        "shein_us_invoice.yaml",
    )
)


def _load_shein_spec() -> dict:
    with open(SHEIN_SPEC_PATH) as f:
        return yaml.safe_load(f)


# ── Synthetic invoice text fragments ────────────────────────────────


SIMPLE_3_ITEMS = """\
Order Number: GSUNJTEST123
Order Date: 2024-07-17
Invoice Date: 2024-07-17

Invoice Detail
Description Quantity Amount(USD)

Women's Casual Summer Top
Short Sleeve Blouse 1 12.99

Men's Running Shoes
Athletic Sneakers Breathable 2 45.50

Kids Cartoon Backpack
School Bag Waterproof 1 8.75

Item(s) Subtotal: 67.24
Shipping/Handling: 0.00
Sales Tax: 4.43
Grand Total: 71.67
"""

SIMPLE_5_ITEMS = """\
Order Number: GSUNJTEST456
Invoice Date: 2024-07-20

Invoice Detail

Floral Print Dress
Maxi Length V-Neck 1 22.99

Wireless Bluetooth Earbuds
Noise Cancelling 1 15.50

Stainless Steel Water Bottle
Insulated 750ml 1 9.99

Cotton Crew Socks 3-Pack
Unisex Athletic 1 5.49

Phone Case Silicone
Clear Protective Cover 1 3.99

Item(s) Subtotal: 57.96
Shipping/Handling: 0.00
Sales Tax: 3.81
Grand Total: 61.77
"""

# OCR-garbled quantities — "1" misread as "i", "l", "|"
OCR_GARBLED_QTY = """\
Invoice Detail

Women's Summer Tank Top
Sleeveless Casual i 7.99

Men's Cargo Shorts
Cotton Relaxed Fit l 19.50

Baby Onesie Set
3-Pack Organic Cotton | 12.99

Subtotal: 40.48
"""

# No section markers — fallback should still work
NO_MARKERS = """\
Order Number: TEST789
Some product description 1 10.00
Another product here 2 5.50
Grand Total: 21.00
"""

# Header with item count: "Invoice Detail (9)"
HEADER_WITH_COUNT = """\
Invoice Detail (9)
Description Quantity Amount(USD)

Item One Description Here 1 5.99
Item Two Description Text 1 3.49
Item Three Another Item 1 7.99
Item Four Product Name 1 2.99
Item Five Something Nice 1 4.49
Item Six Good Product 1 6.99
Item Seven Last One 1 1.99
Item Eight Almost Done 1 8.49
Item Nine Final Item 1 3.99

Item(s) Subtotal: 46.41
"""

# Mismatch: 3 price lines but parser will only extract 2 (one has short desc)
MISMATCH_SHORT_DESC = """\
Invoice Detail

Women's Elegant Evening Dress
Long Formal Gown 1 35.99

XY 1 2.50

Men's Leather Belt
Genuine Cowhide Adjustable 1 11.99

Item(s) Subtotal: 50.48
"""


class TestCountStructuralItems:
    """Test _count_structural_items method."""

    def setup_method(self):
        self.spec = _load_shein_spec()
        self.parser = FormatParser(self.spec)

    def test_simple_3_items(self):
        result = self.parser._count_structural_items(SIMPLE_3_ITEMS)
        assert result["price_line_count"] == 3

    def test_simple_5_items(self):
        result = self.parser._count_structural_items(SIMPLE_5_ITEMS)
        assert result["price_line_count"] == 5

    def test_ocr_garbled_qty_still_counted(self):
        result = self.parser._count_structural_items(OCR_GARBLED_QTY)
        assert result["price_line_count"] == 3

    def test_header_count_extracted(self):
        result = self.parser._count_structural_items(HEADER_WITH_COUNT)
        assert result.get("header_count") == 9

    def test_header_count_none_when_absent(self):
        result = self.parser._count_structural_items(SIMPLE_3_ITEMS)
        assert result.get("header_count") is None

    def test_no_section_markers_returns_zero(self):
        """When no section markers match, price_line_count should be 0 (no section found)."""
        # Build a spec with markers that won't match
        spec = dict(self.spec)
        spec["sections"] = {"items_start": ["NONEXISTENT_START"], "items_end": ["NONEXISTENT_END"]}
        parser = FormatParser(spec)
        result = parser._count_structural_items(NO_MARKERS)
        assert result["price_line_count"] == 0

    def test_empty_text(self):
        result = self.parser._count_structural_items("")
        assert result["price_line_count"] == 0

    def test_result_keys(self):
        result = self.parser._count_structural_items(SIMPLE_3_ITEMS)
        assert "price_line_count" in result
        assert "header_count" in result


class TestStructuralCountInParseResult:
    """Test that structural count is included in parse() output."""

    def setup_method(self):
        self.spec = _load_shein_spec()
        self.parser = FormatParser(self.spec)

    def test_structural_count_in_invoice_data(self):
        result = self.parser.parse(SIMPLE_3_ITEMS)
        assert result["status"] == "success"
        inv = result["invoices"][0]
        assert "structural_item_count" in inv
        sc = inv["structural_item_count"]
        assert sc["price_line_count"] == 3

    def test_no_mismatch_when_counts_agree(self):
        result = self.parser.parse(SIMPLE_3_ITEMS)
        inv = result["invoices"][0]
        # Parser should extract 3 items, structural count is 3
        assert inv.get("item_count_mismatch") is False or inv.get("item_count_mismatch") is None

    def test_mismatch_flagged_when_counts_differ(self):
        """When structural count != extracted items, flag mismatch."""
        result = self.parser.parse(MISMATCH_SHORT_DESC)
        inv = result["invoices"][0]
        sc = inv["structural_item_count"]
        # 3 price lines structurally
        assert sc["price_line_count"] == 3
        # If parser extracts != 3 items, mismatch should be flagged
        extracted = len(inv.get("items", []))
        if extracted != 3:
            assert inv.get("item_count_mismatch") is True

    def test_mismatch_note_in_data_quality(self):
        """Mismatch should produce a data_quality_notes entry."""
        result = self.parser.parse(MISMATCH_SHORT_DESC)
        inv = result["invoices"][0]
        extracted = len(inv.get("items", []))
        sc = inv["structural_item_count"]
        if extracted != sc["price_line_count"]:
            notes = inv.get("data_quality_notes", [])
            assert any("structural" in n.lower() or "mismatch" in n.lower() for n in notes)

    def test_header_count_mismatch(self):
        """When header says (9) but only 9 price lines exist, no mismatch on header."""
        result = self.parser.parse(HEADER_WITH_COUNT)
        inv = result["invoices"][0]
        sc = inv["structural_item_count"]
        assert sc.get("header_count") == 9
        assert sc["price_line_count"] == 9
