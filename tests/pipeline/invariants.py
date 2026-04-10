"""Reusable XLSX invariants for pipeline regression tests.

These assertions encode the rules that the variance fixer and validator
must preserve — regardless of which fix path they take.  Tests should call
these at the end of every workbook-transforming operation so that a
regression like "the VARIANCE CHECK cell got turned into a numeric 0" or
"the ADJUSTMENTS formula now has two stacked corrections" fails loudly.

The three rules below correspond one-to-one to the three bugs that caused
BL #TSCW18489131 to get stuck:

1. ``assert_is_grouped_matches`` — the validator must agree with the
   generator on whether a sheet is in grouped mode.
2. ``assert_variance_is_formula`` — VARIANCE CHECK must always be a formula
   so Excel shows a live-computed value to the broker.
3. ``assert_adjustments_has_no_stacked_corrections`` — the ADJUSTMENTS
   formula may have at most one correction term appended, so repeated fix
   runs are idempotent.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from openpyxl import Workbook
    from openpyxl.worksheet.worksheet import Worksheet

# Column indices — match ``pipeline/xlsx_validator.COL_*``.
COL_DESC = 10
COL_TOTAL_COST = 16

# A correctly-formed ADJUSTMENTS formula looks like one of:
#   =(T2+U2+V2-W2)               — baseline, no correction
#   =(T2+U2+V2-W2)+-30.84        — one appended correction term
#   =(T2+U2+V2-W2)+15.00         — one appended correction term
#
# What we explicitly reject is TWO or more ``+…`` appendages, because that
# means ``_force_adjustment`` was called repeatedly without stripping the
# previous correction first.  The regex below allows the base formula plus
# at most one trailing ``+`` or ``-`` numeric term.
_ADJUSTMENTS_BASE_RE = re.compile(r"^=\(T\d+\+U\d+\+V\d+-W\d+\)(?P<tail>.*)$")
_SINGLE_CORRECTION_RE = re.compile(r"^[+\-]\s*-?\d+(\.\d+)?$")


def _find_label_row(ws: Worksheet, label: str) -> int | None:
    """Return the row number whose description column contains ``label``.

    Search is case-insensitive and runs bottom-up so that, in a multi-invoice
    workbook, we find the final/grand totals row rather than a per-invoice
    intermediate row.
    """
    target = label.upper()
    for row in range(ws.max_row, 0, -1):
        val = ws.cell(row=row, column=COL_DESC).value
        if val and target in str(val).upper():
            return row
    return None


def assert_variance_is_formula(ws: Worksheet) -> None:
    """VARIANCE CHECK must always be a formula, never a bare number.

    Excel shows formula results live, so if anything in the pipeline ever
    writes ``0`` (or any numeric literal) into this cell, the broker loses
    visibility into the audit chain.  This was bug #2 on BL #TSCW18489131.
    """
    row = _find_label_row(ws, "VARIANCE CHECK")
    assert row is not None, "VARIANCE CHECK row not found"
    cell = ws.cell(row=row, column=COL_TOTAL_COST)
    value = cell.value
    assert isinstance(value, str) and value.startswith("="), (
        f"VARIANCE CHECK (row {row}) must be a formula, got {value!r} "
        f"(type={type(value).__name__}). Writing a numeric literal here "
        f"destroys Excel auditability."
    )


def assert_adjustments_has_no_stacked_corrections(ws: Worksheet) -> None:
    """ADJUSTMENTS formula may have at most one appended correction.

    The base formula is ``=(T{r}+U{r}+V{r}-W{r})``.  The fixer is allowed to
    append a single correction term so the variance zeroes out, but running
    the fixer twice must not produce ``=(...)+X+Y``.  This was bug #3.
    """
    row = _find_label_row(ws, "ADJUSTMENTS")
    assert row is not None, "ADJUSTMENTS row not found"
    value = ws.cell(row=row, column=COL_TOTAL_COST).value

    # ADJUSTMENTS may legally be a plain number in very simple workbooks.
    if isinstance(value, (int, float)):
        return

    assert isinstance(value, str) and value.startswith("="), (
        f"ADJUSTMENTS (row {row}) must be a formula or numeric, got {value!r}"
    )

    match = _ADJUSTMENTS_BASE_RE.match(value)
    assert match, (
        f"ADJUSTMENTS (row {row}) does not match the expected base formula "
        f"=(T..+U..+V..-W..): got {value!r}"
    )
    tail = match.group("tail")
    if not tail:
        return  # no correction appended — fine

    assert _SINGLE_CORRECTION_RE.match(tail), (
        f"ADJUSTMENTS (row {row}) has stacked corrections: tail={tail!r}. "
        f"_force_adjustment must strip previous corrections before writing "
        f"a new one so repeated runs are idempotent."
    )


def assert_is_grouped_matches(
    ws: Worksheet, *, expected_grouped: bool, detector: Callable[[Worksheet], bool]
) -> None:
    """The validator's grouped-mode detection must agree with the generator.

    Bug #1 was that ``xlsx_validator`` treated plain ``"SUBTOTAL"`` as a
    grouped-mode marker, even though ``bl_xlsx_generator`` writes that
    label in *ungrouped* mode.  This helper lets a test assert the detector
    function (injected so tests can swap it) agrees with what the factory
    built.
    """
    actual = detector(ws)
    mode = "grouped" if expected_grouped else "ungrouped"
    assert actual == expected_grouped, (
        f"is_grouped detector returned {actual} for a {mode} workbook. "
        f"A mismatch here causes sum_items=0 in the validator, which yields "
        f"a permanent non-zero variance that cannot be auto-fixed."
    )


def snapshot_workbook(wb: Workbook) -> dict[tuple[str, int, int], object]:
    """Return a ``{(sheet, row, col): value}`` dict for deep equality checks.

    Used by idempotency tests: ``snapshot(fix(wb)) == snapshot(fix(fix(wb)))``.
    Only captures cell values; formatting is intentionally out of scope
    because the variance fixer is allowed to re-colour the VARIANCE CHECK
    cell between runs (red → green when resolved).
    """
    out: dict[tuple[str, int, int], object] = {}
    for sheet in wb.worksheets:
        for row in sheet.iter_rows():
            for cell in row:
                if cell.value is not None:
                    out[(sheet.title, cell.row, cell.column)] = cell.value
    return out


def assert_fix_is_idempotent(fn: Callable[[Workbook], None], wb: Workbook) -> None:
    """Apply ``fn`` twice and assert the workbook state is unchanged.

    ``fn`` is expected to be a side-effecting function that takes a workbook
    and mutates it in place (e.g. a wrapper around ``_force_adjustment``).
    """
    fn(wb)
    once = snapshot_workbook(wb)
    fn(wb)
    twice = snapshot_workbook(wb)
    assert once == twice, "Fix function is not idempotent. Differing cells:\n" + "\n".join(
        f"  {k}: {once.get(k)!r} → {twice.get(k)!r}"
        for k in sorted(set(once) | set(twice))
        if once.get(k) != twice.get(k)
    )


def assert_all_invariants(ws: Worksheet) -> None:
    """Convenience: run every XLSX invariant that doesn't need extra context."""
    assert_variance_is_formula(ws)
    assert_adjustments_has_no_stacked_corrections(ws)


__all__: Iterable[str] = (
    "assert_variance_is_formula",
    "assert_adjustments_has_no_stacked_corrections",
    "assert_is_grouped_matches",
    "assert_fix_is_idempotent",
    "assert_all_invariants",
    "snapshot_workbook",
)
