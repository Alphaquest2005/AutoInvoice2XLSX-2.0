"""Regression test for run._extract_consignee on West-Marine-style invoices.

Pinned by shipment 451185: the buggy extractor returned the document title
"Invoice" because the multi-column regex captured the line ABOVE the
"Bill To: Ship To: Remit To:" header - and the captured value was never
routed through ``_is_valid_consignee()``. The expected consignee
(``BUDGET MARINE GRENADA``) sits on the line BELOW the header, duplicated
across the Bill-To and Ship-To columns.

All policy values (fixture name, expected consignee, doc-title rejects)
live in ``tests/fixtures/consignee_extractor/index.yaml``; this test source
contains no domain string literals.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from tests._fixtures import load_test_manifest, load_text_fixture
from tests._paths import (
    add_pipeline_to_sys_path,
    format_name_from_test_file,
)

add_pipeline_to_sys_path()

# Fixture directory name is derived from this test module's filename via
# the SSOT helper (test_<name>.py -> "<name>"), so the test source contains
# no string literal naming the fixture directory.
FIXTURE_NAME = format_name_from_test_file(__file__)
MANIFEST = load_test_manifest(FIXTURE_NAME)


def _make_args(tmp_path, fake_invoice_name):
    """Build a minimal args namespace that drives ``_extract_consignee``
    to step 5 (invoice extraction) without exercising any earlier source.

    Step 1 (manifest meta) - empty.
    Step 2 (declaration meta) - empty.
    Step 3 (BL files) - none.
    Step 4 (declaration files) - none.
    Step 5 (invoice files) - one fake PDF whose text comes from the fixture.
    """
    import run

    return SimpleNamespace(
        _declaration_metadata={},
        _classification={
            run._CLASS_INVOICE: [fake_invoice_name],
            run._CLASS_BL: [],
            run._CLASS_DECLARATION: [],
        },
        input_dir=str(tmp_path),
    )


def _run_with_stubbed_pdf_text(args, fixture_text):
    """Call ``run._extract_consignee`` while ``extract_pdf_text`` returns
    ``fixture_text`` for any path. Restores the original on exit so other
    tests in the same session see the real implementation."""
    import run
    import stages.supplier_resolver as sr

    original = sr.extract_pdf_text
    sr.extract_pdf_text = lambda _path: fixture_text
    try:
        return run._extract_consignee(args)
    finally:
        sr.extract_pdf_text = original


def test_west_marine_returns_company_not_doc_title(tmp_path):
    """The West-Marine layout must yield the company name from the line
    AFTER the Bill-To/Ship-To header, not the doc title from the line
    above."""
    fixture_text = load_text_fixture(FIXTURE_NAME, MANIFEST["ocr_fixture"])
    expected = MANIFEST["expected_consignee"]
    buggy = MANIFEST["buggy_consignee"]

    fake_pdf_name = MANIFEST["fake_pdf_filename"]
    fake_pdf_path = Path(str(tmp_path)) / fake_pdf_name
    # Touch a real file so os.path.exists() returns True inside the function.
    fake_pdf_path.touch()

    args = _make_args(tmp_path, fake_pdf_name)
    result = _run_with_stubbed_pdf_text(args, fixture_text)

    assert result == expected, (
        f"expected {expected!r} (line below 'Bill To: Ship To:'), got {result!r}"
    )
    assert result != buggy, f"extractor returned the doc title {buggy!r} - the fix has regressed"


def test_doc_title_words_in_field_labels():
    """``_is_valid_consignee`` must reject single-word doc titles like
    INVOICE/RECEIPT/STATEMENT/QUOTE/ORDER/PRINT - these are document
    headings OCR can grab when the layout confuses the line-by-line read.
    """
    import run

    for needle in MANIFEST["required_field_label_rejects"]:
        assert needle in run._BL_FIELD_LABELS, (
            f"{needle!r} must be in pipeline.consignee_normalise.field_labels "
            f"(current set: {sorted(run._BL_FIELD_LABELS)})"
        )
