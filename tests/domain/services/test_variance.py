"""Tests for the variance domain service."""

from __future__ import annotations

from decimal import Decimal

from autoinvoice.domain.models.classification import Classification, TariffCode
from autoinvoice.domain.models.grouping import GroupedInvoice, ItemGroup
from autoinvoice.domain.models.invoice import InvoiceItem, InvoiceMetadata
from autoinvoice.domain.services.variance import (
    calculate_items_total,
    check_variance,
    compute_adjustments,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _metadata(
    invoice_total: str = "100.00",
    freight: str = "0",
    insurance: str = "0",
    tax: str = "0",
    discount: str = "0",
    other_cost: str = "0",
) -> InvoiceMetadata:
    return InvoiceMetadata(
        invoice_number="INV-001",
        invoice_date="2026-01-15",
        supplier_name="Test Supplier",
        invoice_total=Decimal(invoice_total),
        freight=Decimal(freight),
        insurance=Decimal(insurance),
        tax=Decimal(tax),
        discount=Decimal(discount),
        other_cost=Decimal(other_cost),
    )


def _item(
    desc: str = "Widget",
    qty: str = "1",
    unit: str = "10.00",
    total: str = "10.00",
) -> InvoiceItem:
    return InvoiceItem(
        description=desc,
        quantity=Decimal(qty),
        unit_cost=Decimal(unit),
        total_cost=Decimal(total),
    )


def _group(
    code: str = "84715000",
    sum_total: str = "50.00",
    sum_qty: str = "5",
) -> ItemGroup:
    item = _item(desc="Widget", qty="1", unit="10.00", total="10.00")
    classification = Classification(
        item=item,
        tariff_code=TariffCode(code),
        confidence=0.95,
        source="rules",
    )
    return ItemGroup(
        tariff_code=TariffCode(code),
        category="PRODUCTS",
        items=(classification,),
        sum_quantity=Decimal(sum_qty),
        sum_total_cost=Decimal(sum_total),
        average_unit_cost=Decimal(sum_total) / Decimal(sum_qty),
    )


# ---------------------------------------------------------------------------
# Tests — check_variance
# ---------------------------------------------------------------------------


class TestCheckVariance:
    """Tests for check_variance."""

    def test_zero_variance_passes(self) -> None:
        """invoice_total equals sum of items -> variance 0."""
        result = check_variance(
            invoice_total=Decimal("100.00"),
            items_total=Decimal("100.00"),
        )
        assert result.variance == Decimal("0")
        assert result.is_within_threshold is True

    def test_positive_variance_detected(self) -> None:
        """invoice_total > sum -> positive variance."""
        result = check_variance(
            invoice_total=Decimal("105.00"),
            items_total=Decimal("100.00"),
        )
        assert result.variance == Decimal("5.00")
        assert result.is_within_threshold is False

    def test_negative_variance_detected(self) -> None:
        """invoice_total < sum -> negative variance."""
        result = check_variance(
            invoice_total=Decimal("95.00"),
            items_total=Decimal("100.00"),
        )
        assert result.variance == Decimal("-5.00")
        assert result.is_within_threshold is False

    def test_variance_within_threshold_passes(self) -> None:
        """Variance of $0.30 with threshold $0.50 -> passes."""
        result = check_variance(
            invoice_total=Decimal("100.30"),
            items_total=Decimal("100.00"),
            threshold=Decimal("0.50"),
        )
        assert result.variance == Decimal("0.30")
        assert result.is_within_threshold is True

    def test_variance_exceeds_threshold_fails(self) -> None:
        """Variance of $5.00 with threshold $0.50 -> fails."""
        result = check_variance(
            invoice_total=Decimal("105.00"),
            items_total=Decimal("100.00"),
            threshold=Decimal("0.50"),
        )
        assert result.variance == Decimal("5.00")
        assert result.is_within_threshold is False


# ---------------------------------------------------------------------------
# Tests — compute_adjustments
# ---------------------------------------------------------------------------


class TestComputeAdjustments:
    """Tests for compute_adjustments."""

    def test_compute_adjustments_freight(self) -> None:
        """Variance matches freight amount -> suggest freight adjustment."""
        meta = _metadata(invoice_total="115.00", freight="15.00")
        adjustments = compute_adjustments(Decimal("15.00"), meta)
        assert any(a.field == "freight" for a in adjustments)
        freight_adj = next(a for a in adjustments if a.field == "freight")
        assert freight_adj.amount == Decimal("15.00")

    def test_compute_adjustments_tax(self) -> None:
        """Variance matches tax -> suggest tax adjustment."""
        meta = _metadata(invoice_total="110.00", tax="10.00")
        adjustments = compute_adjustments(Decimal("10.00"), meta)
        assert any(a.field == "tax" for a in adjustments)
        tax_adj = next(a for a in adjustments if a.field == "tax")
        assert tax_adj.amount == Decimal("10.00")

    def test_compute_adjustments_no_match(self) -> None:
        """Variance doesn't match any known field -> return unresolved."""
        meta = _metadata(invoice_total="107.77")
        adjustments = compute_adjustments(Decimal("7.77"), meta)
        assert len(adjustments) == 1
        assert adjustments[0].field == "unresolved"
        assert adjustments[0].amount == Decimal("7.77")


# ---------------------------------------------------------------------------
# Tests — calculate_items_total
# ---------------------------------------------------------------------------


class TestCalculateItemsTotal:
    """Tests for calculate_items_total."""

    def test_sums_group_totals(self) -> None:
        groups = (
            _group(code="84715000", sum_total="50.00"),
            _group(code="39269099", sum_total="30.00"),
        )
        assert calculate_items_total(groups) == Decimal("80.00")

    def test_empty_groups_returns_zero(self) -> None:
        assert calculate_items_total(()) == Decimal("0")


# ---------------------------------------------------------------------------
# Tests — full flow with GroupedInvoice
# ---------------------------------------------------------------------------


class TestCheckVarianceWithGroupedInvoice:
    """Integration-level test using GroupedInvoice."""

    def test_check_variance_with_grouped_invoice(self) -> None:
        """Full flow using GroupedInvoice."""
        meta = _metadata(invoice_total="80.00")
        groups = (
            _group(code="84715000", sum_total="50.00"),
            _group(code="39269099", sum_total="30.00"),
        )
        grouped = GroupedInvoice(
            metadata=meta,
            groups=groups,
            total_items=2,
        )

        items_total = calculate_items_total(grouped.groups)
        result = check_variance(
            invoice_total=grouped.metadata.invoice_total,
            items_total=items_total,
        )
        assert result.variance == Decimal("0")
        assert result.is_within_threshold is True
