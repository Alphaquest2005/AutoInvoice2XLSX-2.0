"""Invoice domain models - SSOT for invoice data structures."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class InvoiceItem:
    """Single line item from an invoice."""

    description: str
    quantity: Decimal
    unit_cost: Decimal
    total_cost: Decimal
    sku: str = ""
    supplier_item_number: str = ""
    uom: str = "Unit"
    billable: bool = True
    is_bundle: bool = False


@dataclass(frozen=True)
class InvoiceMetadata:
    """Invoice header information."""

    invoice_number: str
    invoice_date: str
    supplier_name: str
    supplier_code: str = ""
    supplier_address: str = ""
    country_code: str = ""
    currency: str = "USD"
    invoice_total: Decimal = Decimal("0")
    freight: Decimal = Decimal("0")
    insurance: Decimal = Decimal("0")
    tax: Decimal = Decimal("0")
    discount: Decimal = Decimal("0")
    other_cost: Decimal = Decimal("0")
    po_number: str = ""


@dataclass(frozen=True)
class Invoice:
    """Complete parsed invoice."""

    metadata: InvoiceMetadata
    items: tuple[InvoiceItem, ...]
    source_file: str = ""
    format_name: str = ""
    raw_text: str = ""
