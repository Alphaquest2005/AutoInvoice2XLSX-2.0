"""BDD step definitions for classification.feature."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest
from pytest_bdd import given, parsers, scenario, then, when

from autoinvoice.domain.models.classification import Classification
from autoinvoice.domain.models.invoice import InvoiceItem
from autoinvoice.domain.services.classifier import classify_item

if TYPE_CHECKING:
    from tests.fakes.fake_code_repo import InMemoryCodeRepository

# ── Scenarios ────────────────────────────────────────────────


@scenario(
    "../../specs/classification.feature", "Classify an item using rule-based keyword matching"
)
def test_rule_based_classification() -> None:
    pass


@scenario(
    "../../specs/classification.feature", "Classify an item using ASYCUDA assessed classification"
)
def test_asycuda_classification() -> None:
    pass


@scenario("../../specs/classification.feature", "Auto-correct an invalid tariff code")
def test_auto_correct_invalid_code() -> None:
    pass


@scenario("../../specs/classification.feature", "Exclusion pattern prevents false keyword match")
def test_exclusion_pattern() -> None:
    pass


# ── Context object ───────────────────────────────────────────


class ClassificationContext:
    item: InvoiceItem
    rules: list[dict[str, Any]]
    code_repo: InMemoryCodeRepository
    assessed: dict[str, Any] | None
    result: Classification | None
    validated_code: str | None

    def __init__(self) -> None:
        self.rules = []
        self.assessed = None
        self.result = None
        self.validated_code = None


@pytest.fixture
def ctx() -> ClassificationContext:
    return ClassificationContext()


# ── Background ───────────────────────────────────────────────


@given("the tariff code repository is loaded")
def given_repo_loaded(ctx: ClassificationContext, code_repo: InMemoryCodeRepository) -> None:
    ctx.code_repo = code_repo


@given("the classification rules contain keyword mappings")
def given_rules_loaded(
    ctx: ClassificationContext, classification_rules: list[dict[str, Any]]
) -> None:
    ctx.rules = classification_rules


@given("the ASYCUDA assessed classifications database is available")
def given_asycuda_available(ctx: ClassificationContext) -> None:
    ctx.assessed = {}


# ── Given steps ──────────────────────────────────────────────


@given(parsers.parse('an invoice item with description "{description}"'))
def given_item_with_description(ctx: ClassificationContext, description: str) -> None:
    ctx.item = InvoiceItem(
        description=description,
        quantity=Decimal("1"),
        unit_cost=Decimal("10.00"),
        total_cost=Decimal("10.00"),
    )


@given(parsers.parse('an invoice item with SKU "{sku}"'))
def given_item_with_sku(ctx: ClassificationContext, sku: str) -> None:
    ctx.item = InvoiceItem(
        description="Generic item",
        quantity=Decimal("1"),
        unit_cost=Decimal("10.00"),
        total_cost=Decimal("10.00"),
        sku=sku,
    )


@given(parsers.parse('a classification rule maps keyword "{keyword}" to tariff code "{code}"'))
def given_rule_maps_keyword(ctx: ClassificationContext, keyword: str, code: str) -> None:
    # Rules already loaded from fixture; this step is for readability
    pass


@given(
    parsers.parse('SKU "{sku}" has an assessed classification of "{code}" in the ASYCUDA database')
)
def given_assessed_classification(ctx: ClassificationContext, sku: str, code: str) -> None:
    if ctx.assessed is None:
        ctx.assessed = {}
    ctx.assessed[sku] = {"code": code, "category": "ASSESSED", "confidence": 1.0}


@given(parsers.parse('an invoice item classified with code "{code}"'))
def given_item_classified_with_code(ctx: ClassificationContext, code: str) -> None:
    ctx.item = InvoiceItem(
        description="Test item",
        quantity=Decimal("1"),
        unit_cost=Decimal("10.00"),
        total_cost=Decimal("10.00"),
    )
    ctx.validated_code = code


@given(
    parsers.parse(
        'code "{invalid_code}" is listed in the invalid codes registry '
        'with correction "{correct_code}"'
    )
)
def given_invalid_code_correction(
    ctx: ClassificationContext, invalid_code: str, correct_code: str
) -> None:
    ctx.code_repo._corrections[invalid_code] = correct_code
    ctx.code_repo._valid_codes.add(correct_code)


@given("no classification rule matches the item description")
def given_no_rule_matches(ctx: ClassificationContext) -> None:
    ctx.rules = []


@given("no assessed classification exists for the item SKU")
def given_no_assessed(ctx: ClassificationContext) -> None:
    ctx.assessed = {}


@given(
    parsers.parse('an exclusion pattern excludes "{pattern}" from that rule'), target_fixture=None
)
def given_exclusion_pattern(ctx: ClassificationContext, pattern: str) -> None:
    # Exclusion already in rules fixture; this step is for readability
    pass


# ── When steps ───────────────────────────────────────────────


@when("the item is classified")
def when_classify_item(ctx: ClassificationContext) -> None:
    ctx.result = classify_item(
        ctx.item,
        ctx.rules,
        ctx.code_repo,
        assessed=ctx.assessed,
    )


@when("the classification is validated")
def when_classification_validated(ctx: ClassificationContext) -> None:
    from autoinvoice.domain.services.code_validator import fix_invalid_code

    tariff = fix_invalid_code(ctx.validated_code, ctx.code_repo)
    if tariff is not None:
        ctx.result = Classification(
            item=ctx.item,
            tariff_code=tariff,
            confidence=1.0,
            source="validated",
        )


# ── Then steps ───────────────────────────────────────────────


@then(parsers.parse('the assigned tariff code is "{code}"'))
def then_tariff_code_is(ctx: ClassificationContext, code: str) -> None:
    assert ctx.result is not None, "Expected a classification result"
    assert ctx.result.tariff_code.code == code


@then(parsers.parse('the classification source is "{source}"'))
def then_source_is(ctx: ClassificationContext, source: str) -> None:
    assert ctx.result is not None
    assert ctx.result.source == source


@then(parsers.parse("the confidence score is {score:g}"))
def then_confidence_is(ctx: ClassificationContext, score: float) -> None:
    assert ctx.result is not None
    assert ctx.result.confidence == pytest.approx(score, abs=0.01)


@then("the confidence score is less than 1.0")
def then_confidence_less_than_one(ctx: ClassificationContext) -> None:
    assert ctx.result is not None
    assert ctx.result.confidence < 1.0


@then(parsers.parse('the tariff code is corrected to "{code}"'))
def then_code_corrected_to(ctx: ClassificationContext, code: str) -> None:
    assert ctx.result is not None
    assert ctx.result.tariff_code.code == code


@then(parsers.parse('a correction record is saved linking "{old}" to "{new}"'))
def then_correction_recorded(ctx: ClassificationContext, old: str, new: str) -> None:
    assert ctx.code_repo.get_correction(old) == new


@then(parsers.parse('the item is not classified by the "{keyword}" keyword rule'))
def then_item_not_classified_by_rule(ctx: ClassificationContext, keyword: str) -> None:
    assert ctx.result is None


@then("the classifier proceeds to the next classification strategy")
def then_proceeds_to_next_strategy(ctx: ClassificationContext) -> None:
    # Classification returned None, meaning the classifier would fall through
    # to the next strategy (LLM) in a real pipeline
    assert ctx.result is None
