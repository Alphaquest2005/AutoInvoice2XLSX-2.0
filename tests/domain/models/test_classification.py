"""TDD tests for classification domain models."""

from __future__ import annotations

from decimal import Decimal

import pytest

from autoinvoice.domain.models.classification import (
    Classification,
    ClassificationResult,
    TariffCode,
)
from autoinvoice.domain.models.invoice import InvoiceItem


class TestTariffCode:
    def test_valid_8_digit_code(self) -> None:
        tc = TariffCode(code="33051000")
        assert tc.code == "33051000"

    def test_chapter_property(self) -> None:
        tc = TariffCode(code="33051000")
        assert tc.chapter == "33"

    def test_heading_property(self) -> None:
        tc = TariffCode(code="33051000")
        assert tc.heading == "3305"

    def test_rejects_non_digit(self) -> None:
        with pytest.raises(ValueError, match="Invalid tariff code"):
            TariffCode(code="3305ABCD")

    def test_rejects_wrong_length(self) -> None:
        with pytest.raises(ValueError, match="Invalid tariff code"):
            TariffCode(code="3305")

    def test_rejects_empty(self) -> None:
        with pytest.raises(ValueError, match="Invalid tariff code"):
            TariffCode(code="")

    def test_frozen_immutability(self) -> None:
        tc = TariffCode(code="33051000")
        with pytest.raises(AttributeError):
            tc.code = "11111111"  # type: ignore[misc]

    def test_equality(self) -> None:
        assert TariffCode(code="33051000") == TariffCode(code="33051000")

    def test_inequality(self) -> None:
        assert TariffCode(code="33051000") != TariffCode(code="33051010")

    def test_hashable(self) -> None:
        tc = TariffCode(code="33051000")
        assert tc in {tc}


class TestClassification:
    def test_create(self) -> None:
        item = InvoiceItem(
            description="Shampoo",
            quantity=Decimal("1"),
            unit_cost=Decimal("5"),
            total_cost=Decimal("5"),
        )
        tc = TariffCode(code="33051000")
        c = Classification(item=item, tariff_code=tc, confidence=0.95, source="rules")
        assert c.item.description == "Shampoo"
        assert c.tariff_code.code == "33051000"
        assert c.confidence == 0.95
        assert c.source == "rules"
        assert c.category == "PRODUCTS"

    def test_custom_category(self) -> None:
        item = InvoiceItem(
            description="X", quantity=Decimal("1"), unit_cost=Decimal("1"), total_cost=Decimal("1")
        )
        c = Classification(
            item=item,
            tariff_code=TariffCode(code="22030010"),
            confidence=0.8,
            source="asycuda",
            category="BEVERAGES",
        )
        assert c.category == "BEVERAGES"

    def test_frozen(self) -> None:
        item = InvoiceItem(
            description="X", quantity=Decimal("1"), unit_cost=Decimal("1"), total_cost=Decimal("1")
        )
        c = Classification(
            item=item, tariff_code=TariffCode(code="33051000"), confidence=0.9, source="rules"
        )
        with pytest.raises(AttributeError):
            c.confidence = 0.5  # type: ignore[misc]


class TestClassificationResult:
    def test_create_empty(self) -> None:
        result = ClassificationResult(classifications=())
        assert len(result.classifications) == 0
        assert result.unclassified_count == 0
        assert result.low_confidence_count == 0

    def test_create_with_counts(self) -> None:
        result = ClassificationResult(
            classifications=(), unclassified_count=3, low_confidence_count=2
        )
        assert result.unclassified_count == 3
        assert result.low_confidence_count == 2
