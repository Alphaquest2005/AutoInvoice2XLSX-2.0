"""Grouping domain models - items grouped by tariff code."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from autoinvoice.domain.models.classification import Classification, TariffCode
    from autoinvoice.domain.models.invoice import InvoiceMetadata


@dataclass(frozen=True)
class ItemGroup:
    """Items grouped by tariff code."""

    tariff_code: TariffCode
    category: str
    items: tuple[Classification, ...]
    sum_quantity: Decimal
    sum_total_cost: Decimal
    average_unit_cost: Decimal


@dataclass(frozen=True)
class GroupedInvoice:
    """Invoice with items grouped by tariff code."""

    metadata: InvoiceMetadata
    groups: tuple[ItemGroup, ...]
    total_items: int
    non_billable_total: Decimal = Decimal("0")
