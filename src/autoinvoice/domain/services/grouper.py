"""Grouper domain service - groups classified items by tariff code.

Pure functions, no I/O.  All monetary and quantity values use Decimal.
"""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import TYPE_CHECKING

from autoinvoice.domain.models.grouping import GroupedInvoice, ItemGroup

if TYPE_CHECKING:
    from autoinvoice.domain.models.classification import Classification, ClassificationResult
    from autoinvoice.domain.models.invoice import InvoiceMetadata


def calculate_group_totals(
    items: tuple[Classification, ...],
) -> tuple[Decimal, Decimal, Decimal]:
    """Return ``(sum_quantity, sum_total_cost, average_unit_cost)`` for *items*.

    If *items* is empty all three values are ``Decimal("0")``.
    """
    if not items:
        return Decimal("0"), Decimal("0"), Decimal("0")

    sum_qty = sum((c.item.quantity for c in items), Decimal("0"))
    sum_total = sum((c.item.total_cost for c in items), Decimal("0"))
    avg_unit = sum_total / sum_qty if sum_qty else Decimal("0")
    return sum_qty, sum_total, avg_unit


def group_by_tariff(
    metadata: InvoiceMetadata,
    classifications: ClassificationResult,
) -> GroupedInvoice:
    """Group classified items by tariff code.

    1. Separate billable vs non-billable items.
    2. Group billable items by ``tariff_code.code``.
    3. For each group compute sum quantities, sum total costs, average unit cost.
    4. Sort groups by tariff code ascending.
    5. Return a :class:`GroupedInvoice` with a ``groups`` tuple and ``total_items``.
    """
    total_items = len(classifications.classifications)

    # Partition billable / non-billable
    billable: list[Classification] = []
    non_billable_total = Decimal("0")

    for cls in classifications.classifications:
        if cls.item.billable:
            billable.append(cls)
        else:
            non_billable_total += cls.item.total_cost

    # Group billable items by tariff code string
    buckets: dict[str, list[Classification]] = defaultdict(list)
    for cls in billable:
        buckets[cls.tariff_code.code].append(cls)

    # Build ItemGroup per bucket, sorted by tariff code ascending
    groups: list[ItemGroup] = []
    for code in sorted(buckets):
        group_items = tuple(buckets[code])
        sum_qty, sum_total, avg_unit = calculate_group_totals(group_items)
        # Use the category from the first item in the group
        category = group_items[0].category
        groups.append(
            ItemGroup(
                tariff_code=group_items[0].tariff_code,
                category=category,
                items=group_items,
                sum_quantity=sum_qty,
                sum_total_cost=sum_total,
                average_unit_cost=avg_unit,
            )
        )

    return GroupedInvoice(
        metadata=metadata,
        groups=tuple(groups),
        total_items=total_items,
        non_billable_total=non_billable_total,
    )
