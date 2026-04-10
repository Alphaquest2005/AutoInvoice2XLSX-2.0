"""Shared BDD fixtures for step definitions."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from autoinvoice.domain.models.invoice import InvoiceItem
from tests.fakes.fake_code_repo import InMemoryCodeRepository


@pytest.fixture
def code_repo() -> InMemoryCodeRepository:
    """In-memory code repository pre-loaded with common valid codes."""
    return InMemoryCodeRepository(
        valid_codes={
            "96081010",
            "84713000",
            "96034020",
            "22021010",
            "33051000",
            "32151100",
        },
        corrections={
            "96034000": "96034020",
            "84710000": "84713000",
        },
    )


@pytest.fixture
def classification_rules() -> list[dict[str, Any]]:
    """Sample classification rules for BDD tests."""
    return [
        {
            "patterns": ["ballpoint pen"],
            "code": "96081010",
            "confidence": 1.0,
            "category": "STATIONERY",
            "priority": 10,
        },
        {
            "patterns": ["shampoo"],
            "code": "33051000",
            "confidence": 0.95,
            "category": "TOILETRIES",
            "priority": 5,
        },
        {
            "patterns": ["ink"],
            "code": "32151100",
            "confidence": 0.9,
            "category": "CHEMICALS",
            "priority": 5,
            "exclude": ["ink cartridge"],
        },
    ]


@pytest.fixture
def sample_item() -> InvoiceItem:
    return InvoiceItem(
        description="Widget",
        quantity=Decimal("1"),
        unit_cost=Decimal("10.00"),
        total_cost=Decimal("10.00"),
    )
