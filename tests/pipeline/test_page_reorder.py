"""Tests for the scanned-PDF page-reorder helpers in pdf_splitter.

These tests exercise the pure-text helpers (no PDF I/O) so they run fast
and don't depend on Tesseract / PyMuPDF.  The end-to-end flow is exercised
implicitly when run.py is invoked against a shipment with reversed pages.
"""

from __future__ import annotations

from pdf_splitter import (
    _extract_page_marker,
    detect_logical_page_order,
)

# ── _extract_page_marker ────────────────────────────────────────


def test_extract_marker_amazon_orderid_tail():
    """Amazon footer: 'orderID = 112-9925042-2903468 1 / 2'."""
    text = "...lots of body text...\nhttps://amazon.com/...&orderID = 112 - 9925042 - 2903468 1 / 2"
    n, total, ident = _extract_page_marker(text)
    assert n == 1
    assert total == 2
    assert ident == "112-9925042-2903468"


def test_extract_marker_bare_slash_tail():
    """OCR-corrupted orderID still surfaces a bare '1 / 2' at end of page."""
    text = (
        "Payment information\nVisa ending in 9318\nhttps://...orderlD = 112-9925042-2903468 2 / 2"
    )
    out = _extract_page_marker(text)
    assert out is not None
    n, total, ident = out
    assert (n, total) == (2, 2)
    # No clean orderID pattern → anonymous identity keyed on total
    assert ident == "_anon_total_2"


def test_extract_marker_page_n_of_m():
    """Generic 'Page 1 of 3' footer."""
    text = "...content...\n\nPage 2 of 3"
    n, total, ident = _extract_page_marker(text)
    assert (n, total) == (2, 3)


def test_extract_marker_ignores_body_text_total_too_large():
    """'2024 / 28' (man-reg date) must not trigger — total 28 > max_total."""
    text = "Dated this day of 20\n; : Examination Required\n2024 / 28"
    assert _extract_page_marker(text, max_total=20) is None


def test_extract_marker_returns_none_when_no_marker():
    text = "Just some invoice body text with no page footer."
    assert _extract_page_marker(text) is None


def test_extract_marker_empty_text():
    assert _extract_page_marker("") is None
    assert _extract_page_marker(None) is None  # type: ignore[arg-type]


# ── detect_logical_page_order ───────────────────────────────────


def _amazon_page(n: int, total: int, order_id: str = "112-9925042-2903468") -> str:
    return (
        f"page {n} body content here...\nhttps://amazon.com/...&orderID = {order_id} {n} / {total}"
    )


def test_detect_order_no_reorder_when_already_correct():
    pages = [_amazon_page(1, 2), _amazon_page(2, 2)]
    assert detect_logical_page_order(pages) is None


def test_detect_order_swaps_reversed_two_page_invoice():
    """Pages [2/2, 1/2] should swap to [1/2, 2/2] → new_order [1, 0]."""
    pages = [_amazon_page(2, 2), _amazon_page(1, 2)]
    assert detect_logical_page_order(pages) == [1, 0]


def test_detect_order_three_pages_reversed():
    pages = [_amazon_page(3, 3), _amazon_page(2, 3), _amazon_page(1, 3)]
    assert detect_logical_page_order(pages) == [2, 1, 0]


def test_detect_order_leaves_unmarked_pages_in_place():
    """Reversed 2-page invoice followed by 2 unmarked declaration pages."""
    pages = [
        _amazon_page(2, 2),  # → swap to slot 1
        _amazon_page(1, 2),  # → swap to slot 0
        "Simplified Declaration Form...",  # no marker — stays at slot 2
        "Another Declaration Form...",  # no marker — stays at slot 3
    ]
    assert detect_logical_page_order(pages) == [1, 0, 2, 3]


def test_detect_order_anonymous_marker_groups_with_identified():
    """A page that lost its orderID via OCR still groups with its sibling."""
    pages = [
        _amazon_page(2, 2),
        # OCR mangled orderID — only bare '1 / 2' survives
        "footer junk\nhttps://...\n1 / 2",
    ]
    assert detect_logical_page_order(pages) == [1, 0]


def test_detect_order_two_separate_invoices_not_intermixed():
    """Two distinct order IDs in one PDF must each reorder independently."""
    pages = [
        _amazon_page(2, 2, "AAA"),  # invoice A page 2
        _amazon_page(1, 2, "AAA"),  # invoice A page 1
        _amazon_page(2, 2, "BBB"),  # invoice B page 2
        _amazon_page(1, 2, "BBB"),  # invoice B page 1
    ]
    # Each contiguous group sorted independently
    assert detect_logical_page_order(pages) == [1, 0, 3, 2]


def test_detect_order_skips_group_with_duplicate_page_numbers():
    """If two pages both claim '1 of 2', don't guess — skip this group."""
    pages = [_amazon_page(1, 2), _amazon_page(1, 2)]
    assert detect_logical_page_order(pages) is None


def test_detect_order_empty_list():
    assert detect_logical_page_order([]) is None
