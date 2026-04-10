"""Tests for the grouper domain service."""

from __future__ import annotations

from decimal import Decimal

from autoinvoice.domain.models.classification import (
    Classification,
    ClassificationResult,
    TariffCode,
)
from autoinvoice.domain.models.grouping import GroupedInvoice
from autoinvoice.domain.models.invoice import InvoiceItem, InvoiceMetadata
from autoinvoice.domain.services.grouper import (
    calculate_group_totals,
    group_by_tariff,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _metadata() -> InvoiceMetadata:
    return InvoiceMetadata(
        invoice_number="INV-001",
        invoice_date="2026-01-15",
        supplier_name="Test Supplier",
    )


def _item(
    desc: str = "Widget",
    qty: str = "1",
    unit: str = "10.00",
    total: str = "10.00",
    billable: bool = True,
) -> InvoiceItem:
    return InvoiceItem(
        description=desc,
        quantity=Decimal(qty),
        unit_cost=Decimal(unit),
        total_cost=Decimal(total),
        billable=billable,
    )


def _classify(
    item: InvoiceItem,
    code: str = "84715000",
    category: str = "PRODUCTS",
    confidence: float = 0.95,
    source: str = "rules",
) -> Classification:
    return Classification(
        item=item,
        tariff_code=TariffCode(code),
        confidence=confidence,
        source=source,
        category=category,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestGroupByTariff:
    """Tests for group_by_tariff."""

    def test_group_single_tariff_code(self) -> None:
        """3 items same code -> 1 group with sum_quantity, sum_total_cost, avg_unit_cost."""
        items = (
            _classify(_item(desc="A", qty="2", unit="5.00", total="10.00"), code="84715000"),
            _classify(_item(desc="B", qty="3", unit="4.00", total="12.00"), code="84715000"),
            _classify(_item(desc="C", qty="5", unit="6.00", total="30.00"), code="84715000"),
        )
        result = group_by_tariff(_metadata(), ClassificationResult(classifications=items))

        assert len(result.groups) == 1
        group = result.groups[0]
        assert group.tariff_code == TariffCode("84715000")
        assert group.sum_quantity == Decimal("10")
        assert group.sum_total_cost == Decimal("52.00")
        assert group.average_unit_cost == Decimal("52.00") / Decimal("10")

    def test_group_multiple_tariff_codes(self) -> None:
        """Items with 3 different codes -> 3 groups sorted by code."""
        items = (
            _classify(_item(desc="A", qty="1", total="10.00"), code="84715000"),
            _classify(_item(desc="B", qty="2", total="20.00"), code="39269099"),
            _classify(_item(desc="C", qty="3", total="30.00"), code="73181500"),
        )
        result = group_by_tariff(_metadata(), ClassificationResult(classifications=items))

        assert len(result.groups) == 3
        codes = [g.tariff_code.code for g in result.groups]
        assert codes == ["39269099", "73181500", "84715000"]

    def test_empty_items_returns_empty_groups(self) -> None:
        """No classifications -> empty GroupedInvoice."""
        result = group_by_tariff(
            _metadata(),
            ClassificationResult(classifications=()),
        )

        assert isinstance(result, GroupedInvoice)
        assert result.groups == ()
        assert result.total_items == 0
        assert result.non_billable_total == Decimal("0")

    def test_non_billable_items_excluded_from_groups(self) -> None:
        """Items with billable=False go to non_billable_total."""
        billable_item = _classify(
            _item(desc="Billable", qty="1", total="50.00", billable=True),
            code="84715000",
        )
        non_billable_1 = _classify(
            _item(desc="Freight", qty="1", total="25.00", billable=False),
            code="84715000",
        )
        non_billable_2 = _classify(
            _item(desc="Tax", qty="1", total="15.00", billable=False),
            code="84715000",
        )
        items = (billable_item, non_billable_1, non_billable_2)
        result = group_by_tariff(_metadata(), ClassificationResult(classifications=items))

        assert result.non_billable_total == Decimal("40.00")
        # Only the billable item should appear in groups
        assert len(result.groups) == 1
        assert result.groups[0].sum_quantity == Decimal("1")
        assert result.groups[0].sum_total_cost == Decimal("50.00")

    def test_average_unit_cost_calculation(self) -> None:
        """Verify average = sum_total / sum_quantity."""
        items = (
            _classify(_item(qty="4", unit="3.00", total="12.00"), code="84715000"),
            _classify(_item(qty="6", unit="5.00", total="30.00"), code="84715000"),
        )
        result = group_by_tariff(_metadata(), ClassificationResult(classifications=items))

        group = result.groups[0]
        expected_avg = Decimal("42.00") / Decimal("10")  # 4.2
        assert group.average_unit_cost == expected_avg

    def test_group_preserves_category(self) -> None:
        """Category from Classification flows to ItemGroup."""
        items = (
            _classify(
                _item(desc="Service", qty="1", total="100.00"),
                code="84715000",
                category="SERVICES",
            ),
        )
        result = group_by_tariff(_metadata(), ClassificationResult(classifications=items))

        assert result.groups[0].category == "SERVICES"

    def test_total_items_count(self) -> None:
        """total_items matches input count (billable + non-billable)."""
        items = (
            _classify(_item(qty="1", total="10.00", billable=True), code="84715000"),
            _classify(_item(qty="1", total="20.00", billable=False), code="84715000"),
            _classify(_item(qty="1", total="30.00", billable=True), code="39269099"),
        )
        result = group_by_tariff(_metadata(), ClassificationResult(classifications=items))

        assert result.total_items == 3

    def test_groups_sorted_by_tariff_code(self) -> None:
        """Groups are sorted ascending by tariff code."""
        items = (
            _classify(_item(total="10.00"), code="99999999"),
            _classify(_item(total="20.00"), code="11111111"),
            _classify(_item(total="30.00"), code="55555555"),
        )
        result = group_by_tariff(_metadata(), ClassificationResult(classifications=items))

        codes = [g.tariff_code.code for g in result.groups]
        assert codes == ["11111111", "55555555", "99999999"]

    def test_calculate_totals_sums_all_groups(self) -> None:
        """Verify total across all groups."""
        items = (
            _classify(_item(qty="2", total="20.00"), code="84715000"),
            _classify(_item(qty="3", total="30.00"), code="39269099"),
            _classify(_item(qty="5", total="50.00"), code="73181500"),
        )
        result = group_by_tariff(_metadata(), ClassificationResult(classifications=items))

        total_cost = sum(g.sum_total_cost for g in result.groups)
        total_qty = sum(g.sum_quantity for g in result.groups)
        assert total_cost == Decimal("100.00")
        assert total_qty == Decimal("10")


class TestCalculateGroupTotals:
    """Tests for calculate_group_totals helper."""

    def test_basic_calculation(self) -> None:
        items = (
            _classify(_item(qty="2", total="20.00"), code="84715000"),
            _classify(_item(qty="3", total="30.00"), code="84715000"),
        )
        sum_qty, sum_total, avg_unit = calculate_group_totals(items)

        assert sum_qty == Decimal("5")
        assert sum_total == Decimal("50.00")
        assert avg_unit == Decimal("50.00") / Decimal("5")

    def test_single_item(self) -> None:
        items = (_classify(_item(qty="7", unit="3.00", total="21.00"), code="84715000"),)
        sum_qty, sum_total, avg_unit = calculate_group_totals(items)

        assert sum_qty == Decimal("7")
        assert sum_total == Decimal("21.00")
        assert avg_unit == Decimal("3.00")

    def test_empty_tuple_returns_zeros(self) -> None:
        sum_qty, sum_total, avg_unit = calculate_group_totals(())

        assert sum_qty == Decimal("0")
        assert sum_total == Decimal("0")
        assert avg_unit == Decimal("0")
