"""Variance checker domain service - pure functions for invoice variance analysis."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from autoinvoice.domain.models.grouping import ItemGroup
    from autoinvoice.domain.models.invoice import InvoiceMetadata


@dataclass(frozen=True)
class VarianceResult:
    """Result of a variance check between invoice total and items total."""

    variance: Decimal
    is_within_threshold: bool
    invoice_total: Decimal
    items_total: Decimal


@dataclass(frozen=True)
class Adjustment:
    """A suggested adjustment to explain a variance."""

    field: str  # 'freight', 'insurance', 'tax', 'discount', 'other_cost', 'unresolved'
    amount: Decimal
    explanation: str


def check_variance(
    invoice_total: Decimal,
    items_total: Decimal,
    threshold: Decimal = Decimal("0.50"),
) -> VarianceResult:
    """Compute and check variance between invoice total and items total.

    Args:
        invoice_total: The stated invoice total.
        items_total: The computed sum of all item totals.
        threshold: Maximum acceptable absolute variance.

    Returns:
        A VarianceResult with the computed variance and threshold check.
    """
    variance = invoice_total - items_total
    is_within = abs(variance) <= threshold
    return VarianceResult(
        variance=variance,
        is_within_threshold=is_within,
        invoice_total=invoice_total,
        items_total=items_total,
    )


def compute_adjustments(
    variance: Decimal,
    metadata: InvoiceMetadata,
) -> list[Adjustment]:
    """Suggest which metadata field might account for the variance.

    Checks freight, insurance, tax, discount, and other_cost against
    the variance amount.  If none match, returns an 'unresolved' adjustment.

    Args:
        variance: The computed variance amount.
        metadata: Invoice metadata containing cost breakdown fields.

    Returns:
        List of Adjustment suggestions.
    """
    adjustments: list[Adjustment] = []

    candidates = (
        ("freight", metadata.freight),
        ("insurance", metadata.insurance),
        ("tax", metadata.tax),
        ("discount", metadata.discount),
        ("other_cost", metadata.other_cost),
    )

    for field, amount in candidates:
        if amount != Decimal("0") and amount == variance:
            adjustments.append(
                Adjustment(
                    field=field,
                    amount=amount,
                    explanation=f"Variance matches {field} amount of {amount}",
                )
            )

    if not adjustments:
        adjustments.append(
            Adjustment(
                field="unresolved",
                amount=variance,
                explanation=f"Variance of {variance} does not match any known field",
            )
        )

    return adjustments


def calculate_items_total(groups: tuple[ItemGroup, ...]) -> Decimal:
    """Sum all group totals to get the items total.

    Args:
        groups: Tuple of ItemGroup from a GroupedInvoice.

    Returns:
        The sum of sum_total_cost across all groups.
    """
    return sum((g.sum_total_cost for g in groups), Decimal("0"))
