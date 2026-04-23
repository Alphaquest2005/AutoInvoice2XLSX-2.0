"""Regression tests for consignee FREIGHT-suffix stripping.

Covers Fix B: ``ROSALIE LA GRENADE (FREIGHT 5.00 US)`` and OCR variants must
NEVER reach the email summary or XLSX. The client writes freight as a pencil
note on declarations/BLs; OCR captures it; the pipeline must strip it.

We exercise the normalizer directly (through ``_extract_consignee``'s
closure-local helper, which we re-construct here), plus the pdf_splitter
source-of-truth regex.
"""

import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "pipeline"))


# ---------------------------------------------------------------------------
# _normalize_consignee behaviour — duplicated inline here because it's a
# closure-local helper in run._extract_consignee. Any behaviour change in
# one must be mirrored in the other; this test pins the contract.
# ---------------------------------------------------------------------------

def _normalize_consignee(name: str) -> str:
    """Mirror of run._extract_consignee._normalize_consignee (closure-local)."""
    if not name:
        return name
    cleaned = re.sub(
        r'\s*[\(\[\{]\s*FREIGHT\b.*?(?:[\)\]\}]|$)',
        '', name, flags=re.IGNORECASE,
    ).strip()
    cleaned = re.sub(
        r'\s+FREIGHT\s+[\d.]+\s*(?:US|USD)?\s*$',
        '', cleaned, flags=re.IGNORECASE,
    ).strip()
    cleaned = re.sub(r'[\s,;:\-]+$', '', cleaned).strip()
    return cleaned


def test_strip_parens_freight_us_suffix():
    assert _normalize_consignee("ROSALIE LA GRENADE (FREIGHT 5.00 US)") == "ROSALIE LA GRENADE"


def test_strip_parens_freight_us_no_decimal():
    assert _normalize_consignee("Jane Doe (FREIGHT 10 US)") == "Jane Doe"


def test_strip_parens_no_us_token():
    assert _normalize_consignee("ACME CORP (FREIGHT 12.34)") == "ACME CORP"


def test_strip_ocr_squarebracket_close():
    """OCR sometimes converts ) to ] — still must strip."""
    assert _normalize_consignee("ROSALIE LA GRENADE (FREIGHT 5.00 US]") == "ROSALIE LA GRENADE"


def test_strip_ocr_squarebracket_open():
    assert _normalize_consignee("ROSALIE LA GRENADE [FREIGHT 5.00 US]") == "ROSALIE LA GRENADE"


def test_strip_missing_closer_end_of_string():
    """If OCR drops the close bracket entirely, still strip."""
    assert _normalize_consignee("ROSALIE LA GRENADE (FREIGHT 5.00 US") == "ROSALIE LA GRENADE"


def test_strip_bareless_freight():
    """OCR may drop both brackets — trailing 'FREIGHT X.XX US' still stripped."""
    assert _normalize_consignee("ROSALIE LA GRENADE FREIGHT 5.00 US") == "ROSALIE LA GRENADE"


def test_clean_name_unchanged():
    """A clean consignee must pass through untouched."""
    assert _normalize_consignee("ROSALIE LA GRENADE") == "ROSALIE LA GRENADE"


def test_empty_input_is_safe():
    assert _normalize_consignee("") == ""
    assert _normalize_consignee(None) is None


def test_trailing_punctuation_stripped():
    assert _normalize_consignee("ROSALIE LA GRENADE,") == "ROSALIE LA GRENADE"
    assert _normalize_consignee("ROSALIE LA GRENADE - ") == "ROSALIE LA GRENADE"


def test_case_insensitive_freight():
    assert _normalize_consignee("Jane Doe (freight 5.00 us)") == "Jane Doe"


# ---------------------------------------------------------------------------
# pdf_splitter source-of-truth: the consignee line extraction
# ---------------------------------------------------------------------------

def test_pdf_splitter_strips_freight_in_consignee_line():
    """Simulate the pdf_splitter declaration-metadata regex end-to-end.

    Constructs a minimal text buffer and runs the same logic
    extract_declaration_metadata uses on the Consignee: line.
    """
    # Simulate OCR declaration text with consignee line
    line_clean = "Consignee: ROSALIE LA GRENADE (FREIGHT 5.00 US)"
    consignee_part = line_clean.split(':', 1)[1].strip()

    freight_match = re.search(
        r'\(\s*FREIGHT\s+([\d.]+)\s*(?:US)?\s*[\)\}\]]',
        consignee_part, re.IGNORECASE,
    )
    assert freight_match is not None
    assert freight_match.group(1) == "5.00"

    consignee_name = re.sub(
        r'\s*[\(\[\{].*?FREIGHT.*?(?:[\)\}\]]|$)',
        '', consignee_part, flags=re.IGNORECASE,
    ).strip()
    assert consignee_name == "ROSALIE LA GRENADE"


def test_pdf_splitter_bareless_fallback():
    """When OCR drops the brackets entirely, the bareless fallback picks up."""
    consignee_part = "ROSALIE LA GRENADE FREIGHT 5.00 US"

    # Primary bracket regex fails
    freight_match = re.search(
        r'\(\s*FREIGHT\s+([\d.]+)\s*(?:US)?\s*[\)\}\]]',
        consignee_part, re.IGNORECASE,
    )
    assert freight_match is None

    # Fallback bareless regex succeeds
    bareless = re.search(
        r'(.+?)\s+FREIGHT\s+([\d.]+)\s*(?:US|USD)?\s*$',
        consignee_part, re.IGNORECASE,
    )
    assert bareless is not None
    assert bareless.group(1).strip() == "ROSALIE LA GRENADE"
    assert bareless.group(2) == "5.00"
