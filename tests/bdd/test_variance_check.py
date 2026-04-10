"""BDD step definitions for variance_check.feature."""

from __future__ import annotations

from decimal import Decimal

import pytest
from pytest_bdd import given, parsers, scenario, then, when

from autoinvoice.domain.services.variance import VarianceResult, check_variance

# ── Scenarios ────────────────────────────────────────────────


@scenario("../../specs/variance_check.feature", "Zero variance passes verification")
def test_zero_variance() -> None:
    pass


@scenario("../../specs/variance_check.feature", "Non-zero variance beyond threshold triggers error")
def test_variance_beyond_threshold() -> None:
    pass


@scenario("../../specs/variance_check.feature", "Variance within threshold passes")
def test_variance_within_threshold() -> None:
    pass


# ── Context object ───────────────────────────────────────────


class VarianceContext:
    invoice_total: Decimal
    items_total: Decimal
    threshold: Decimal
    result: VarianceResult | None

    def __init__(self) -> None:
        self.invoice_total = Decimal("0")
        self.items_total = Decimal("0")
        self.threshold = Decimal("0.50")
        self.result = None


@pytest.fixture
def ctx() -> VarianceContext:
    return VarianceContext()


# ── Background ───────────────────────────────────────────────


@given("a processed invoice with a known invoice total")
def given_processed_invoice(ctx: VarianceContext) -> None:
    pass  # invoice_total set in subsequent Given step


@given("an XLSX output has been generated from the classified items")
def given_xlsx_generated(ctx: VarianceContext) -> None:
    pass  # items_total set in subsequent Given step


# ── Given steps ──────────────────────────────────────────────


@given(parsers.parse("the invoice total is ${total:S}"))
def given_invoice_total(ctx: VarianceContext, total: str) -> None:
    ctx.invoice_total = Decimal(total.replace(",", ""))


@given(parsers.parse("the XLSX computed total is ${total:S}"))
def given_xlsx_total(ctx: VarianceContext, total: str) -> None:
    ctx.items_total = Decimal(total.replace(",", ""))


@given(parsers.parse("the variance threshold is ${threshold:S}"))
def given_threshold(ctx: VarianceContext, threshold: str) -> None:
    ctx.threshold = Decimal(threshold.replace(",", ""))


@given(parsers.parse("the variance of ${amount:S} exceeds the threshold"))
def given_variance_exceeds(ctx: VarianceContext, amount: str) -> None:
    # Informational step — variance is computed from invoice_total and items_total
    pass


@given(parsers.parse("the maximum LLM variance fix attempts is {attempts:d}"))
def given_max_attempts(ctx: VarianceContext, attempts: int) -> None:
    # Informational for scenario context
    pass


# ── When steps ───────────────────────────────────────────────


@when("the variance check runs")
def when_variance_check(ctx: VarianceContext) -> None:
    ctx.result = check_variance(ctx.invoice_total, ctx.items_total, ctx.threshold)


# ── Then steps ───────────────────────────────────────────────


@then(parsers.parse("the variance is ${amount:S}"))
def then_variance_is(ctx: VarianceContext, amount: str) -> None:
    assert ctx.result is not None
    expected = Decimal(amount.replace(",", ""))
    assert abs(ctx.result.variance) == expected


@then("the verification passes")
def then_verification_passes(ctx: VarianceContext) -> None:
    assert ctx.result is not None
    assert ctx.result.is_within_threshold is True


@then("the verification fails with a variance error")
def then_verification_fails(ctx: VarianceContext) -> None:
    assert ctx.result is not None
    assert ctx.result.is_within_threshold is False


@then("the error includes both the invoice total and the computed total")
def then_error_includes_totals(ctx: VarianceContext) -> None:
    assert ctx.result is not None
    assert ctx.result.invoice_total == ctx.invoice_total
    assert ctx.result.items_total == ctx.items_total


@then("the verification passes because the variance is within the threshold")
def then_passes_within_threshold(ctx: VarianceContext) -> None:
    assert ctx.result is not None
    assert ctx.result.is_within_threshold is True
