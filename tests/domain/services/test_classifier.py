"""Tests for the classifier domain service."""

from __future__ import annotations

from decimal import Decimal

from autoinvoice.domain.models.classification import (
    ClassificationResult,
    TariffCode,
)
from autoinvoice.domain.models.invoice import InvoiceItem
from autoinvoice.domain.services.classifier import classify_item, classify_items
from tests.fakes.fake_code_repo import InMemoryCodeRepository


def _make_item(description: str, sku: str = "") -> InvoiceItem:
    """Helper to build an InvoiceItem with sensible defaults."""
    return InvoiceItem(
        description=description,
        quantity=Decimal("1"),
        unit_cost=Decimal("10.00"),
        total_cost=Decimal("10.00"),
        sku=sku,
    )


# ── Single-item classification ──────────────────────────────────────────────


class TestExactKeywordMatch:
    def test_exact_keyword_match_returns_classification(self) -> None:
        # Arrange
        item = _make_item("Shampoo 500ml")
        rules = [
            {
                "id": "shampoo_001",
                "priority": 100,
                "patterns": ["shampoo"],
                "code": "33051000",
                "category": "COSMETICS",
                "confidence": 0.95,
            },
        ]
        repo = InMemoryCodeRepository(valid_codes={"33051000"})

        # Act
        result = classify_item(item, rules, repo)

        # Assert
        assert result is not None
        assert result.tariff_code == TariffCode("33051000")
        assert result.confidence == 0.95
        assert result.source == "rules"
        assert result.category == "COSMETICS"


class TestMultipleRulesHighestPriorityWins:
    def test_multiple_rules_highest_priority_wins(self) -> None:
        # Arrange
        item = _make_item("Leather handbag with strap")
        rules = [
            {
                "id": "bag_generic",
                "priority": 50,
                "patterns": ["bag"],
                "code": "42029200",
                "category": "BAGS",
                "confidence": 0.80,
            },
            {
                "id": "handbag_specific",
                "priority": 100,
                "patterns": ["handbag"],
                "code": "42022200",
                "category": "BAGS & LUGGAGE",
                "confidence": 0.95,
            },
        ]
        repo = InMemoryCodeRepository(valid_codes={"42022200", "42029200"})

        # Act
        result = classify_item(item, rules, repo)

        # Assert
        assert result is not None
        assert result.tariff_code == TariffCode("42022200")
        assert result.confidence == 0.95


class TestNoRuleMatchReturnsNone:
    def test_no_rule_match_returns_none(self) -> None:
        # Arrange
        item = _make_item("Quantum flux capacitor")
        rules = [
            {
                "id": "shampoo_001",
                "priority": 100,
                "patterns": ["shampoo"],
                "code": "33051000",
                "confidence": 0.95,
            },
        ]
        repo = InMemoryCodeRepository(valid_codes={"33051000"})

        # Act
        result = classify_item(item, rules, repo)

        # Assert
        assert result is None


class TestExclusionPatternPreventsMatch:
    def test_exclusion_pattern_prevents_match(self) -> None:
        # Arrange — "dog shampoo" should NOT match a human-shampoo rule
        item = _make_item("Dog shampoo 250ml")
        rules = [
            {
                "id": "shampoo_001",
                "priority": 100,
                "patterns": ["shampoo"],
                "exclude": ["dog", "pet", "animal"],
                "code": "33051000",
                "category": "COSMETICS",
                "confidence": 0.95,
            },
        ]
        repo = InMemoryCodeRepository(valid_codes={"33051000"})

        # Act
        result = classify_item(item, rules, repo)

        # Assert
        assert result is None


class TestCaseInsensitiveMatching:
    def test_case_insensitive_matching(self) -> None:
        # Arrange
        item = _make_item("SHAMPOO EXTRA VOLUME 500ML")
        rules = [
            {
                "id": "shampoo_001",
                "priority": 100,
                "patterns": ["shampoo"],
                "code": "33051000",
                "category": "COSMETICS",
                "confidence": 0.95,
            },
        ]
        repo = InMemoryCodeRepository(valid_codes={"33051000"})

        # Act
        result = classify_item(item, rules, repo)

        # Assert
        assert result is not None
        assert result.tariff_code == TariffCode("33051000")


class TestAssessedClassificationTakesPrecedence:
    def test_assessed_classification_takes_precedence(self) -> None:
        # Arrange — assessed DB overrides rule match
        item = _make_item("Shampoo 500ml", sku="SHP-500")
        rules = [
            {
                "id": "shampoo_001",
                "priority": 100,
                "patterns": ["shampoo"],
                "code": "33051000",
                "confidence": 0.95,
            },
        ]
        assessed = {
            "SHP-500": {"code": "33049900", "category": "ASSESSED", "confidence": 0.97},
        }
        repo = InMemoryCodeRepository(
            valid_codes={"33051000", "33049900"},
            assessed=assessed,
        )

        # Act
        result = classify_item(item, rules, repo, assessed=assessed)

        # Assert
        assert result is not None
        assert result.tariff_code == TariffCode("33049900")
        assert result.source == "assessed"
        assert result.confidence == 0.97


class TestInvalidCodeFromRuleGetsCorrected:
    def test_invalid_code_from_rule_gets_corrected(self) -> None:
        # Arrange — rule returns an invalid code, repo knows the correction
        item = _make_item("Shampoo 500ml")
        rules = [
            {
                "id": "shampoo_001",
                "priority": 100,
                "patterns": ["shampoo"],
                "code": "33051010",
                "confidence": 0.90,
            },
        ]
        repo = InMemoryCodeRepository(
            valid_codes={"33051000"},
            corrections={"33051010": "33051000"},
        )

        # Act
        result = classify_item(item, rules, repo)

        # Assert
        assert result is not None
        assert result.tariff_code == TariffCode("33051000")


# ── Batch classification ─────────────────────────────────────────────────────


class TestClassifyItemsBatch:
    def test_classify_items_batch(self) -> None:
        # Arrange
        items = (
            _make_item("Shampoo 500ml"),
            _make_item("Leather handbag"),
        )
        rules = [
            {
                "id": "shampoo_001",
                "priority": 100,
                "patterns": ["shampoo"],
                "code": "33051000",
                "category": "COSMETICS",
                "confidence": 0.95,
            },
            {
                "id": "handbag_001",
                "priority": 99,
                "patterns": ["handbag"],
                "code": "42022200",
                "category": "BAGS & LUGGAGE",
                "confidence": 0.95,
            },
        ]
        repo = InMemoryCodeRepository(valid_codes={"33051000", "42022200"})

        # Act
        result = classify_items(items, rules, repo)

        # Assert
        assert isinstance(result, ClassificationResult)
        assert len(result.classifications) == 2
        assert result.unclassified_count == 0


class TestClassifyItemsCountsUnclassified:
    def test_classify_items_counts_unclassified(self) -> None:
        # Arrange — one item has no matching rule
        items = (
            _make_item("Shampoo 500ml"),
            _make_item("Quantum flux capacitor"),
        )
        rules = [
            {
                "id": "shampoo_001",
                "priority": 100,
                "patterns": ["shampoo"],
                "code": "33051000",
                "confidence": 0.95,
            },
        ]
        repo = InMemoryCodeRepository(valid_codes={"33051000"})

        # Act
        result = classify_items(items, rules, repo)

        # Assert
        assert result.unclassified_count == 1
        assert len(result.classifications) == 1


class TestClassifyItemsCountsLowConfidence:
    def test_classify_items_counts_low_confidence(self) -> None:
        # Arrange — one item matched with confidence below 0.7
        items = (
            _make_item("Shampoo 500ml"),
            _make_item("Mystery cream tube"),
        )
        rules = [
            {
                "id": "shampoo_001",
                "priority": 100,
                "patterns": ["shampoo"],
                "code": "33051000",
                "confidence": 0.95,
            },
            {
                "id": "cream_001",
                "priority": 50,
                "patterns": ["cream"],
                "code": "33049900",
                "confidence": 0.60,
            },
        ]
        repo = InMemoryCodeRepository(valid_codes={"33051000", "33049900"})

        # Act
        result = classify_items(items, rules, repo)

        # Assert
        assert len(result.classifications) == 2
        assert result.low_confidence_count == 1
        assert result.unclassified_count == 0
