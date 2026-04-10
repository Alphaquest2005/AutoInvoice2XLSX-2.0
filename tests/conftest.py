"""Shared test fixtures for AutoInvoice2XLSX."""

from __future__ import annotations

from decimal import Decimal

import pytest

from autoinvoice.domain.models.invoice import Invoice, InvoiceItem, InvoiceMetadata


@pytest.fixture
def sample_item() -> InvoiceItem:
    return InvoiceItem(
        description="Shampoo 500ml bottle",
        quantity=Decimal("2"),
        unit_cost=Decimal("5.99"),
        total_cost=Decimal("11.98"),
        sku="SHP-500",
    )


@pytest.fixture
def sample_metadata() -> InvoiceMetadata:
    return InvoiceMetadata(
        invoice_number="INV-001",
        invoice_date="2026-01-15",
        supplier_name="Test Supplier",
        supplier_code="TSUP",
        country_code="US",
        currency="USD",
        invoice_total=Decimal("100.00"),
    )


@pytest.fixture
def sample_invoice(sample_metadata: InvoiceMetadata, sample_item: InvoiceItem) -> Invoice:
    return Invoice(
        metadata=sample_metadata,
        items=(sample_item,),
        source_file="test_invoice.pdf",
        format_name="default",
    )
