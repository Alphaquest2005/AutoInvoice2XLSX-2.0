"""Regression tests for ``pipeline.consignee_resolver``.

Pinned by shipment TSCW18489131 / invoice 26006159 (Victron Energy
shipping to Budget Marine Grenada). The Victron layout uses
``INVOICE ADDRESS / INVOICE DETAILS / DELIVERY ADDRESS`` column headers
instead of the standard Bill-To/Ship-To labels, so the older
label-based extractors miss it. The rule-substring scanner introduced
by the resolver must find the literal company name anywhere in the
OCR text and route to the 7400-000 doc_type — independent of layout.

All policy values (expected consignees, doc_types, source tags) live
in ``tests/fixtures/consignee_resolver/index.yaml``; this test source
contains no domain string literals.
"""

from __future__ import annotations

from tests._fixtures import load_test_manifest, load_text_fixture
from tests._paths import (
    add_pipeline_to_sys_path,
    format_name_from_test_file,
)

add_pipeline_to_sys_path()

FIXTURE_NAME = format_name_from_test_file(__file__)
MANIFEST = load_test_manifest(FIXTURE_NAME)


def _resolve(**kwargs):
    """Import-on-call so test collection isn't sensitive to import order."""
    from pipeline.consignee_resolver import resolve_invoice_consignee

    return resolve_invoice_consignee(**kwargs)


def test_victron_invoice_resolves_to_budget_marine_via_rule_scan():
    """Victron invoice text mentions 'Budget Marine Grenada' three+ times
    in non-standard column headers. The rule-substring scanner must find
    it and resolve to 7400-000 — without any vendor-specific regex."""
    fixture_text = load_text_fixture(FIXTURE_NAME, MANIFEST["victron_ocr_fixture"])
    expected = MANIFEST["victron_expected"]

    result = _resolve(invoice_text=fixture_text)

    assert result["consignee_name"] == expected["consignee_name"], (
        f"expected consignee {expected['consignee_name']!r}, "
        f"got {result['consignee_name']!r}. "
        f"Likely cause: rule-substring scanner did not match — check "
        f"that 'budget marine' substring search in the Victron text "
        f"is still active in pipeline/consignee_resolver.py."
    )
    assert result["doc_type"] == expected["doc_type"], (
        f"expected doc_type {expected['doc_type']!r}, "
        f"got {result['doc_type']!r}. This is the 26006159 regression."
    )
    assert result["source"] == expected["source"]


def test_word_boundary_avoids_false_positive_in_longer_words():
    """Single-word aliases like 'reef' must not match inside 'reefer' or
    'submarine' — would falsely route freight invoices to a personal-
    effects consignee. The scanner uses regex word-boundary tokens."""
    text = MANIFEST["word_boundary_negative_text"]
    expected_source = MANIFEST["word_boundary_negative_expected_source"]

    result = _resolve(invoice_text=text)

    assert result["source"] == expected_source, (
        f"reefer/submarine text falsely matched a rule "
        f"(source={result['source']!r}). Word-boundary regex regressed."
    )


def test_word_boundary_positive_match_on_standalone_word():
    """When a single-word alias appears as a standalone word, it must
    match. Guards against an over-eager word-boundary fix that breaks
    legitimate matches."""
    text = MANIFEST["word_boundary_positive_text"]
    expected_dt = MANIFEST["word_boundary_positive_expected_doc_type"]
    expected_source = MANIFEST["word_boundary_positive_expected_source"]

    result = _resolve(invoice_text=text)

    assert result["doc_type"] == expected_dt
    assert result["source"] == expected_source


def test_bl_consignee_fallback_routes_through_rule_matcher():
    """When invoice text yields nothing, BL consignee must be tried and
    itself routed through the rule matcher (so 'BUDGET MARINE GRENADA'
    from a BL still resolves to 7400-000)."""
    bl = MANIFEST["bl_fallback_consignee"]
    expected_dt = MANIFEST["bl_fallback_expected_doc_type"]
    expected_source = MANIFEST["bl_fallback_expected_source"]

    result = _resolve(bl_consignee=bl)

    assert result["doc_type"] == expected_dt
    assert result["source"] == expected_source


def test_bl_free_text_keeps_name_with_default_doc_type():
    """A BL consignee that doesn't map to any rule must still be returned
    as the consignee_name (so downstream paperwork has a name) but with
    the default doc_type (no rule fired)."""
    bl = MANIFEST["bl_free_text_consignee"]
    expected_dt = MANIFEST["bl_free_text_expected_doc_type"]
    expected_source = MANIFEST["bl_free_text_expected_source"]

    result = _resolve(bl_consignee=bl)

    assert result["consignee_name"] == bl
    assert result["doc_type"] == expected_dt
    assert result["source"] == expected_source


def test_empty_inputs_yield_default_with_no_consignee():
    """All-empty inputs must produce the default doc_type with an empty
    consignee — caller surfaces this via the consignee_unrecognised
    checklist finding (block-severity, blocks email send)."""
    expected_source = MANIFEST["empty_expected_source"]
    expected_dt = MANIFEST["empty_expected_doc_type"]

    result = _resolve()

    assert result["consignee_name"] == ""
    assert result["doc_type"] == expected_dt
    assert result["source"] == expected_source
