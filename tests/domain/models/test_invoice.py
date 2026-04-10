"""TDD tests for invoice domain models."""

from __future__ import annotations

from decimal import Decimal

import pytest

from autoinvoice.domain.models.invoice import Invoice, InvoiceItem, InvoiceMetadata


class TestInvoiceItem:
    def test_create_with_required_fields(self) -> None:
        item = InvoiceItem(
            description="Widget",
            quantity=Decimal("3"),
            unit_cost=Decimal("10.00"),
            total_cost=Decimal("30.00"),
        )
        assert item.description == "Widget"
        assert item.quantity == Decimal("3")
        assert item.unit_cost == Decimal("10.00")
        assert item.total_cost == Decimal("30.00")

    def test_defaults(self) -> None:
        item = InvoiceItem(
            description="X", quantity=Decimal("1"), unit_cost=Decimal("1"), total_cost=Decimal("1")
        )
        assert item.sku == ""
        assert item.supplier_item_number == ""
        assert item.uom == "Unit"
        assert item.billable is True
        assert item.is_bundle is False

    def test_frozen_immutability(self) -> None:
        item = InvoiceItem(
            description="X", quantity=Decimal("1"), unit_cost=Decimal("1"), total_cost=Decimal("1")
        )
        with pytest.raises(AttributeError):
            item.description = "Y"  # type: ignore[misc]

    def test_equality_by_value(self) -> None:
        args = {
            "description": "X",
            "quantity": Decimal("1"),
            "unit_cost": Decimal("1"),
            "total_cost": Decimal("1"),
        }
        assert InvoiceItem(**args) == InvoiceItem(**args)

    def test_hashable(self) -> None:
        item = InvoiceItem(
            description="X", quantity=Decimal("1"), unit_cost=Decimal("1"), total_cost=Decimal("1")
        )
        assert hash(item) is not None
        assert item in {item}


class TestInvoiceMetadata:
    def test_create_with_required_fields(self) -> None:
        meta = InvoiceMetadata(
            invoice_number="INV-001",
            invoice_date="2026-01-01",
            supplier_name="Acme Corp",
        )
        assert meta.invoice_number == "INV-001"
        assert meta.invoice_date == "2026-01-01"
        assert meta.supplier_name == "Acme Corp"

    def test_defaults(self) -> None:
        meta = InvoiceMetadata(invoice_number="X", invoice_date="2026-01-01", supplier_name="S")
        assert meta.supplier_code == ""
        assert meta.country_code == ""
        assert meta.currency == "USD"
        assert meta.invoice_total == Decimal("0")
        assert meta.freight == Decimal("0")
        assert meta.insurance == Decimal("0")
        assert meta.tax == Decimal("0")
        assert meta.discount == Decimal("0")
        assert meta.po_number == ""

    def test_frozen_immutability(self) -> None:
        meta = InvoiceMetadata(invoice_number="X", invoice_date="2026-01-01", supplier_name="S")
        with pytest.raises(AttributeError):
            meta.invoice_number = "Y"  # type: ignore[misc]


class TestInvoice:
    def test_create_with_items_tuple(self) -> None:
        item = InvoiceItem(
            description="W", quantity=Decimal("1"), unit_cost=Decimal("5"), total_cost=Decimal("5")
        )
        meta = InvoiceMetadata(invoice_number="INV-1", invoice_date="2026-01-01", supplier_name="S")
        inv = Invoice(metadata=meta, items=(item,))
        assert len(inv.items) == 1
        assert inv.items[0].description == "W"

    def test_defaults(self) -> None:
        meta = InvoiceMetadata(invoice_number="X", invoice_date="2026-01-01", supplier_name="S")
        inv = Invoice(metadata=meta, items=())
        assert inv.source_file == ""
        assert inv.format_name == ""

    def test_frozen_immutability(self) -> None:
        meta = InvoiceMetadata(invoice_number="X", invoice_date="2026-01-01", supplier_name="S")
        inv = Invoice(metadata=meta, items=())
        with pytest.raises(AttributeError):
            inv.source_file = "new.pdf"  # type: ignore[misc]

    def test_items_are_tuple_not_list(self) -> None:
        meta = InvoiceMetadata(invoice_number="X", invoice_date="2026-01-01", supplier_name="S")
        inv = Invoice(metadata=meta, items=())
        assert isinstance(inv.items, tuple)
