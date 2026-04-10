"""TDD tests for grouping domain models."""

from __future__ import annotations

from decimal import Decimal

import pytest

from autoinvoice.domain.models.classification import Classification, TariffCode
from autoinvoice.domain.models.grouping import GroupedInvoice, ItemGroup
from autoinvoice.domain.models.invoice import InvoiceItem, InvoiceMetadata


class TestItemGroup:
    def test_create(self) -> None:
        item = InvoiceItem(
            description="X", quantity=Decimal("2"), unit_cost=Decimal("5"), total_cost=Decimal("10")
        )
        tc = TariffCode(code="33051000")
        classification = Classification(item=item, tariff_code=tc, confidence=0.9, source="rules")
        group = ItemGroup(
            tariff_code=tc,
            category="PRODUCTS",
            items=(classification,),
            sum_quantity=Decimal("2"),
            sum_total_cost=Decimal("10"),
            average_unit_cost=Decimal("5"),
        )
        assert group.tariff_code.code == "33051000"
        assert group.category == "PRODUCTS"
        assert len(group.items) == 1
        assert group.sum_quantity == Decimal("2")
        assert group.sum_total_cost == Decimal("10")
        assert group.average_unit_cost == Decimal("5")

    def test_frozen(self) -> None:
        tc = TariffCode(code="33051000")
        group = ItemGroup(
            tariff_code=tc,
            category="X",
            items=(),
            sum_quantity=Decimal("0"),
            sum_total_cost=Decimal("0"),
            average_unit_cost=Decimal("0"),
        )
        with pytest.raises(AttributeError):
            group.category = "Y"  # type: ignore[misc]


class TestGroupedInvoice:
    def test_create(self) -> None:
        meta = InvoiceMetadata(invoice_number="INV-1", invoice_date="2026-01-01", supplier_name="S")
        gi = GroupedInvoice(metadata=meta, groups=(), total_items=0)
        assert gi.total_items == 0
        assert gi.non_billable_total == Decimal("0")

    def test_frozen(self) -> None:
        meta = InvoiceMetadata(invoice_number="INV-1", invoice_date="2026-01-01", supplier_name="S")
        gi = GroupedInvoice(metadata=meta, groups=(), total_items=5)
        with pytest.raises(AttributeError):
            gi.total_items = 10  # type: ignore[misc]
