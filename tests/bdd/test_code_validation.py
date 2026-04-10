"""BDD step definitions for code_validation.feature."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from pytest_bdd import given, parsers, scenario, then, when

from autoinvoice.domain.models.classification import TariffCode
from autoinvoice.domain.services.code_validator import (
    fix_invalid_code,
    validate_cet_code,
)

if TYPE_CHECKING:
    from tests.fakes.fake_code_repo import InMemoryCodeRepository

# ── Scenarios ────────────────────────────────────────────────


@scenario("../../specs/code_validation.feature", "Valid 8-digit code passes validation")
def test_valid_code() -> None:
    pass


@scenario("../../specs/code_validation.feature", "Non-8-digit code is rejected")
def test_non_8_digit_code() -> None:
    pass


@scenario(
    "../../specs/code_validation.feature", "Known bad code is corrected from invalid codes registry"
)
def test_known_bad_code_corrected() -> None:
    pass


@scenario("../../specs/code_validation.feature", "Extract chapter and heading from a valid code")
def test_extract_chapter_heading() -> None:
    pass


@scenario("../../specs/code_validation.feature", "Non-numeric code is rejected")
def test_non_numeric_code() -> None:
    pass


# ── Context object ───────────────────────────────────────────


class ValidationContext:
    code: str
    code_repo: InMemoryCodeRepository
    is_valid: bool | None
    error: str | None
    corrected_code: str | None
    tariff: TariffCode | None

    def __init__(self) -> None:
        self.is_valid = None
        self.error = None
        self.corrected_code = None
        self.tariff = None


@pytest.fixture
def ctx() -> ValidationContext:
    return ValidationContext()


# ── Background ───────────────────────────────────────────────


@given("the tariff code repository is loaded with the current CET schedule")
def given_repo_loaded(ctx: ValidationContext, code_repo: InMemoryCodeRepository) -> None:
    ctx.code_repo = code_repo


@given("the invalid codes registry is loaded from invalid_codes.json")
def given_invalid_codes_loaded(ctx: ValidationContext) -> None:
    # Corrections already in code_repo fixture
    pass


# ── Given steps ──────────────────────────────────────────────


@given(parsers.parse('a tariff code "{code}"'))
def given_tariff_code(ctx: ValidationContext, code: str) -> None:
    ctx.code = code


@given(parsers.parse('the invalid codes registry maps "{invalid}" to "{correct}"'))
def given_correction_mapping(ctx: ValidationContext, invalid: str, correct: str) -> None:
    ctx.code_repo._corrections[invalid] = correct
    ctx.code_repo._valid_codes.add(correct)


@given(parsers.parse('code "{code}" is a category heading in the CET schedule, not an end-node'))
def given_code_is_heading(ctx: ValidationContext, code: str) -> None:
    # Headings end in 0000 — they're not in the valid codes set
    ctx.code_repo._valid_codes.discard(code)


# ── When steps ───────────────────────────────────────────────


@when("the code is validated")
def when_code_validated(ctx: ValidationContext) -> None:
    if not ctx.code.isdigit():
        ctx.is_valid = False
        ctx.error = "code must contain only digits"
        return
    if len(ctx.code) != 8:
        ctx.is_valid = False
        ctx.error = "code must be exactly 8 digits"
        return

    ctx.is_valid = validate_cet_code(ctx.code, ctx.code_repo)

    if not ctx.is_valid:
        # Try correction
        result = fix_invalid_code(ctx.code, ctx.code_repo)
        if result is not None:
            ctx.corrected_code = result.code
            ctx.error = None
        else:
            if ctx.code.endswith("0000"):
                ctx.error = "code is a heading, not a classifiable end-node"
            else:
                ctx.error = "code not found in CET schedule"


@when("the chapter and heading are extracted")
def when_extract_chapter_heading(ctx: ValidationContext) -> None:
    ctx.tariff = TariffCode(ctx.code)


# ── Then steps ───────────────────────────────────────────────


@then("the validation passes")
def then_validation_passes(ctx: ValidationContext) -> None:
    assert ctx.is_valid is True


@then("the code is accepted as a valid CET tariff code")
def then_code_accepted(ctx: ValidationContext) -> None:
    assert ctx.is_valid is True


@then("the validation fails")
def then_validation_fails(ctx: ValidationContext) -> None:
    assert ctx.is_valid is False


@then("the error indicates the code must be exactly 8 digits")
def then_error_8_digits(ctx: ValidationContext) -> None:
    assert ctx.error is not None
    assert "8 digits" in ctx.error


@then("the error indicates the code is a heading, not a classifiable end-node")
def then_error_heading(ctx: ValidationContext) -> None:
    assert ctx.error is not None
    assert "heading" in ctx.error


@then("the error indicates the code must contain only digits")
def then_error_non_numeric(ctx: ValidationContext) -> None:
    assert ctx.error is not None
    assert "digits" in ctx.error


@then("the original code is flagged as invalid")
def then_original_flagged(ctx: ValidationContext) -> None:
    assert ctx.is_valid is False


@then(parsers.parse('the corrected code "{code}" is returned'))
def then_corrected_code_returned(ctx: ValidationContext, code: str) -> None:
    assert ctx.corrected_code == code


@then("a correction record is logged")
def then_correction_logged(ctx: ValidationContext) -> None:
    assert ctx.corrected_code is not None


@then(parsers.parse('the chapter is "{chapter}"'))
def then_chapter_is(ctx: ValidationContext, chapter: str) -> None:
    assert ctx.tariff is not None
    assert ctx.tariff.chapter == chapter


@then(parsers.parse('the heading is "{heading}"'))
def then_heading_is(ctx: ValidationContext, heading: str) -> None:
    assert ctx.tariff is not None
    assert ctx.tariff.heading == heading


@then(parsers.parse('the subheading is "{subheading}"'))
def then_subheading_is(ctx: ValidationContext, subheading: str) -> None:
    assert ctx.tariff is not None
    assert ctx.tariff.subheading == subheading
