"""Tests for the parser domain service."""

from __future__ import annotations

from decimal import Decimal

from autoinvoice.domain.models.invoice import Invoice, InvoiceItem, InvoiceMetadata
from autoinvoice.domain.services.parser import (
    detect_format,
    extract_items,
    extract_metadata,
    parse_currency,
    parse_invoice_text,
)
from tests.fakes.fake_config_provider import FakeConfigProvider

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SIMPLE_TABULAR_INVOICE = (
    "Invoice #12345\n"
    "Date: 01/15/2026\n"
    "Supplier: Acme Corp\n"
    "\n"
    "Description\tQty\tUnit Price\tTotal\n"
    "Widget A\t2\t10.00\t20.00\n"
    "Widget B\t5\t3.50\t17.50\n"
    "\n"
    "Total: $37.50\n"
)

SHEIN_INVOICE_TEXT = (
    "SHEIN US Services, LLC.\n"
    "Sales Invoice\n"
    "Invoice No.: INVUS20260101\n"
    "Invoice Date: 2026-01-01\n"
    "\n"
    "Description\tQuantity\tUnit Price\n"
    "Summer Dress\t1\t25.99\n"
    "\n"
    "Grand Total: $25.99\n"
)


# ---------------------------------------------------------------------------
# parse_invoice_text
# ---------------------------------------------------------------------------


class TestParseSimpleTabularInvoice:
    def test_extracts_line_items(self) -> None:
        result = parse_invoice_text(SIMPLE_TABULAR_INVOICE)

        assert len(result.items) == 2
        assert result.items[0].description == "Widget A"
        assert result.items[0].quantity == Decimal("2")
        assert result.items[0].unit_cost == Decimal("10.00")
        assert result.items[0].total_cost == Decimal("20.00")
        assert result.items[1].description == "Widget B"
        assert result.items[1].quantity == Decimal("5")
        assert result.items[1].unit_cost == Decimal("3.50")
        assert result.items[1].total_cost == Decimal("17.50")

    def test_returns_frozen_invoice(self) -> None:
        result = parse_invoice_text(SIMPLE_TABULAR_INVOICE)
        assert isinstance(result, Invoice)
        assert isinstance(result.metadata, InvoiceMetadata)
        assert all(isinstance(i, InvoiceItem) for i in result.items)


# ---------------------------------------------------------------------------
# extract_metadata
# ---------------------------------------------------------------------------


class TestExtractMetadata:
    def test_extracts_invoice_number(self) -> None:
        text = "Some header\nInvoice #12345\nother stuff"
        meta = extract_metadata(text)
        assert meta.invoice_number == "12345"

    def test_extracts_invoice_number_variant(self) -> None:
        text = "Invoice No.: INV-9876\nDate: 03/01/2026"
        meta = extract_metadata(text)
        assert meta.invoice_number == "INV-9876"

    def test_extracts_invoice_date(self) -> None:
        text = "Invoice #100\nDate: 01/15/2026\nSupplier: X"
        meta = extract_metadata(text)
        assert meta.invoice_date == "01/15/2026"

    def test_extracts_invoice_date_iso(self) -> None:
        text = "Invoice Date: 2026-03-15\nStuff"
        meta = extract_metadata(text)
        assert meta.invoice_date == "2026-03-15"

    def test_extracts_supplier_name(self) -> None:
        text = "Supplier: Acme Corp\nInvoice #1"
        meta = extract_metadata(text)
        assert meta.supplier_name == "Acme Corp"

    def test_extracts_supplier_sold_by(self) -> None:
        text = "Sold by: MegaStore Inc.\nInvoice #2"
        meta = extract_metadata(text)
        assert meta.supplier_name == "MegaStore Inc."

    def test_extracts_invoice_total(self) -> None:
        text = "Items...\nTotal: $500.00\nThank you"
        meta = extract_metadata(text)
        assert meta.invoice_total == Decimal("500.00")

    def test_extracts_grand_total(self) -> None:
        text = "Subtotal: $450.00\nGrand Total: $475.50"
        meta = extract_metadata(text)
        assert meta.invoice_total == Decimal("475.50")

    def test_empty_text_returns_empty_metadata(self) -> None:
        meta = extract_metadata("")
        assert meta.invoice_number == ""
        assert meta.invoice_date == ""
        assert meta.supplier_name == ""
        assert meta.invoice_total == Decimal("0")


# ---------------------------------------------------------------------------
# parse_currency
# ---------------------------------------------------------------------------


class TestParseCurrency:
    def test_simple_dollar(self) -> None:
        assert parse_currency("$500.00") == Decimal("500.00")

    def test_with_commas(self) -> None:
        assert parse_currency("$1,234.56") == Decimal("1234.56")

    def test_no_symbol(self) -> None:
        assert parse_currency("1234.56") == Decimal("1234.56")

    def test_negative_value(self) -> None:
        assert parse_currency("-$50.00") == Decimal("-50.00")

    def test_parenthetical_negative(self) -> None:
        assert parse_currency("($50.00)") == Decimal("-50.00")

    def test_euro_symbol(self) -> None:
        assert parse_currency("\u20ac99.99") == Decimal("99.99")

    def test_empty_string_returns_zero(self) -> None:
        assert parse_currency("") == Decimal("0")

    def test_whitespace_only_returns_zero(self) -> None:
        assert parse_currency("   ") == Decimal("0")


# ---------------------------------------------------------------------------
# extract_items  /  header-row skipping
# ---------------------------------------------------------------------------


class TestExtractItems:
    def test_skips_header_rows(self) -> None:
        text = "Description\tQty\tUnit Price\tTotal\nGadget\t3\t15.00\t45.00\n"
        items = extract_items(text)
        assert len(items) == 1
        assert items[0].description == "Gadget"

    def test_skips_blank_lines(self) -> None:
        text = "\nGadget\t1\t10.00\t10.00\n\nGizmo\t2\t5.00\t10.00\n\n"
        items = extract_items(text)
        assert len(items) == 2

    def test_handles_empty_text(self) -> None:
        items = extract_items("")
        assert items == ()


# ---------------------------------------------------------------------------
# parse_invoice_text  — empty input
# ---------------------------------------------------------------------------


class TestParseHandlesEmptyText:
    def test_empty_string(self) -> None:
        result = parse_invoice_text("")
        assert result.items == ()
        assert result.metadata.invoice_number == ""

    def test_whitespace_only(self) -> None:
        result = parse_invoice_text("   \n\n  ")
        assert result.items == ()


# ---------------------------------------------------------------------------
# detect_format
# ---------------------------------------------------------------------------


class TestDetectFormat:
    def test_known_supplier(self) -> None:
        config = FakeConfigProvider(
            format_specs={
                "shein_us": {
                    "name": "shein_us",
                    "detect": {
                        "all_of": ["SHEIN US Services, LLC."],
                        "any_of": ["Sales Invoice"],
                    },
                },
            },
        )
        result = detect_format(
            SHEIN_INVOICE_TEXT,
            available_formats=["shein_us"],
            config_provider=config,
        )
        assert result == "shein_us"

    def test_unknown_defaults(self) -> None:
        config = FakeConfigProvider(
            format_specs={
                "shein_us": {
                    "name": "shein_us",
                    "detect": {
                        "all_of": ["SHEIN US Services, LLC."],
                    },
                },
            },
        )
        result = detect_format(
            "Random unrelated text with no known patterns.",
            available_formats=["shein_us"],
            config_provider=config,
        )
        assert result == "default"

    def test_no_config_provider_returns_default(self) -> None:
        result = detect_format(
            "Some invoice text",
            available_formats=["shein_us"],
            config_provider=None,
        )
        assert result == "default"


# ---------------------------------------------------------------------------
# parse_with_format_spec
# ---------------------------------------------------------------------------


class TestParseWithFormatSpec:
    def test_format_spec_guides_parsing(self) -> None:
        text = "Summer Dress\t1\t25.99\t25.99\nTank Top\t2\t12.00\t24.00\n"
        spec: dict = {
            "items": {
                "strategy": "line",
                "line": {
                    "delimiter": "\t",
                    "field_map": {
                        "description": 0,
                        "quantity": 1,
                        "unit_price": 2,
                        "total_cost": 3,
                    },
                },
            },
        }
        result = parse_invoice_text(text, format_spec=spec)
        assert len(result.items) == 2
        assert result.items[0].description == "Summer Dress"
        assert result.items[0].quantity == Decimal("1")
        assert result.items[0].unit_cost == Decimal("25.99")
        assert result.items[0].total_cost == Decimal("25.99")
        assert result.items[1].description == "Tank Top"
        assert result.items[1].total_cost == Decimal("24.00")

    def test_format_spec_skip_patterns(self) -> None:
        text = "SHEIN US Services\nDescription\tQty\tPrice\tTotal\nDress\t1\t30.00\t30.00\n"
        spec: dict = {
            "items": {
                "strategy": "line",
                "line": {
                    "delimiter": "\t",
                    "field_map": {
                        "description": 0,
                        "quantity": 1,
                        "unit_price": 2,
                        "total_cost": 3,
                    },
                    "skip_patterns": [
                        "^SHEIN",
                        "^Description",
                    ],
                },
            },
        }
        result = parse_invoice_text(text, format_spec=spec)
        assert len(result.items) == 1
        assert result.items[0].description == "Dress"
