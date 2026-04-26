"""Tests for the scanned-PDF page-reorder helpers in pdf_splitter.

These tests exercise the pure-text helpers (no PDF I/O) so they run fast
and don't depend on Tesseract / PyMuPDF.  The end-to-end flow is exercised
implicitly when run.py is invoked against a shipment with reversed pages.
"""

from __future__ import annotations

from pdf_splitter import (
    _classify_page_position,
    _detect_reverse_scan_order,
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


# ── Reverse-scan fallback (HAWB9728292 / PUMA) ─────────────────
#
# These tests use page-text excerpts captured from the actual
# 4-page PUMA web-order scan that the PDF splitter failed to
# parse (00190.pdf, attached to the 03152025_ABOUT email).  The
# scan was fed into the scanner upside-down so the physical page
# order is exactly reversed:
#
#   phys 0 → marketing/legal footer (PUMA NORTH AMERICA, sign-up,
#            cookie settings, do not sell, IMPRINT AND LEGAL)
#   phys 1 → shipping & billing block (mid-doc)
#   phys 2 → items list (Multiflex SL V, Turin III sneakers)
#   phys 3 → order header (Order Number 00190, Date ordered,
#            Order status: SHIPPED, ORDERED ITEMS)
#
# No "Page N of M" markers appear anywhere, so detect_logical_page_order
# returns None and the marker-less fallback must do the work.

_PUMA_FOOTER_PAGE = (
    "Shipping and Detvery a . am\nTerms & Conditions\n"
    "Do Not Sell or Share My Information\nStore Locator\nBuy a Gift Card\n"
    "Service Discount\nPromotion Exclusions\nCookie Settings\nSUPPORT\nABOUT\n"
    "Company\nCorporate News\nPress Center\nInvestors\nSustainability\nCareers\n"
    "STAY UP TO DATE\nSign Up for Email\nSTAY UP TO DATE\nEXPLORE\n"
    "PUMA NORTH AMERICA, INC.\nIMPRINT AND LEGAL DATA\nWEB ID: 987 105 384\n"
)
_PUMA_SHIPPING_PAGE = (
    "SHIPPING & BILLING\n\nShipping Address:\nDawseanne Williams\n"
    "10813 Northwest 30th Street\nDoral\nFL 33172\nUS\n\n"
    "Shipment Method:\nExpedited - $20.00\n\nBilling Address:\n"
    "Dawseanne Williams\n10813 Northwest 30th Street\nDoral\nFL 33172\nUS\n\n"
    "Email: dawsiswilliams@yahoo.com\nPhone: 4734586918\n"
    "Billing Method\nVisa ***********4954\n"
    "Number Of Products 5\nTotal Amount $135.21\n"
    "BACK TO ORDER HISTORY\n"
)
_PUMA_ITEMS_PAGE = (
    "Multiflex SL V Little Kids' Sneakers\nColor: Puma White-Puma White\n"
    "Size: 1\nStyle Number: 380740_06\nQuantity: 1\n$29.99\nIN STOCK\n\n"
    "Multiflex SL V Little Kids' Sneakers\nSize: 1.5\nQuantity: 1\n$29.99\n"
    "Turin III Men's Sneakers\nSize: 13\nStyle Number: 383037_01\n"
    "Quantity: 1\n$30.99\nIN STOCK\n"
)
_PUMA_HEADER_PAGE = (
    "FREE AND EASY EASY RETURNS\n"
    "Home < My Account < Order Details\n"
    "Order details\nMy account\nLOGOUT\n"
    "Order Number 00190 - US 480772356\n"
    "Date ordered : 8 / 18 / 2024\n"
    "Order status : SHIPPED\n"
    "Tracking : Click Here\n"
    "ORDERED ITEMS\n"
    "Multiflex SL V Little Kids' Sneakers\nQuantity : 4\nNO LONGER AVAILABLE"
)


def test_classify_header_recognises_order_number_and_date():
    assert _classify_page_position(_PUMA_HEADER_PAGE) == "header"


def test_classify_footer_recognises_marketing_legal_cluster():
    assert _classify_page_position(_PUMA_FOOTER_PAGE) == "footer"


def test_classify_items_page_is_neither():
    """Items page has no header *or* footer signals → unclassified."""
    assert _classify_page_position(_PUMA_ITEMS_PAGE) is None


def test_classify_shipping_page_is_neither():
    """The mid-doc shipping/billing page must not be mistaken for a footer
    just because it contains 'BACK TO ORDER HISTORY' (single weak signal)."""
    assert _classify_page_position(_PUMA_SHIPPING_PAGE) is None


def test_classify_empty_or_short_text_is_none():
    assert _classify_page_position("") is None
    assert _classify_page_position(None) is None  # type: ignore[arg-type]
    assert _classify_page_position("short") is None


def test_classify_single_footer_phrase_is_not_enough():
    """A single 'Privacy Policy' / 'Terms & Conditions' line in site nav
    must not trigger a footer classification."""
    assert (
        _classify_page_position("Item description here. Lots of body text. Terms & Conditions")
        is None
    )


def test_reverse_scan_detects_puma_4page_reversal():
    """The actual HAWB9728292 case: footer→shipping→items→header
    must be reversed to header→items→shipping→footer."""
    pages = [
        _PUMA_FOOTER_PAGE,  # phys 0  → goes to slot 3
        _PUMA_SHIPPING_PAGE,  # phys 1  → goes to slot 2
        _PUMA_ITEMS_PAGE,  # phys 2  → goes to slot 1
        _PUMA_HEADER_PAGE,  # phys 3  → goes to slot 0
    ]
    assert _detect_reverse_scan_order(pages) == [3, 2, 1, 0]


def test_reverse_scan_returns_none_when_already_in_order():
    """Same content, but in correct logical order — no reorder."""
    pages = [
        _PUMA_HEADER_PAGE,
        _PUMA_ITEMS_PAGE,
        _PUMA_SHIPPING_PAGE,
        _PUMA_FOOTER_PAGE,
    ]
    assert _detect_reverse_scan_order(pages) is None


def test_reverse_scan_requires_at_least_3_pages():
    """A 2-page swap is too ambiguous without explicit page markers
    — leave it to detect_logical_page_order or to manual review."""
    pages = [_PUMA_FOOTER_PAGE, _PUMA_HEADER_PAGE]
    assert _detect_reverse_scan_order(pages) is None


def test_reverse_scan_requires_header_at_last_physical_page():
    """If the header page is in the middle, refuse to guess."""
    pages = [
        _PUMA_FOOTER_PAGE,
        _PUMA_HEADER_PAGE,  # header in middle, not last
        _PUMA_ITEMS_PAGE,
        _PUMA_SHIPPING_PAGE,
    ]
    assert _detect_reverse_scan_order(pages) is None


def test_reverse_scan_requires_footer_at_first_physical_page():
    """If the footer page is in the middle, refuse to guess."""
    pages = [
        _PUMA_ITEMS_PAGE,
        _PUMA_FOOTER_PAGE,  # footer in middle, not first
        _PUMA_SHIPPING_PAGE,
        _PUMA_HEADER_PAGE,
    ]
    assert _detect_reverse_scan_order(pages) is None


def test_reverse_scan_requires_exactly_one_header_and_one_footer():
    """Two pages each carrying header signals → ambiguous → None."""
    pages = [
        _PUMA_FOOTER_PAGE,
        _PUMA_HEADER_PAGE,  # header signals
        _PUMA_ITEMS_PAGE,
        _PUMA_HEADER_PAGE,  # header signals again
    ]
    assert _detect_reverse_scan_order(pages) is None


def test_reverse_scan_returns_none_for_marker_unmarked_mix():
    """If header/footer aren't both unambiguously identified, leave
    the PDF alone (do not silently reorder declarations / manifests)."""
    pages = [
        "Just a declaration form page",
        "Another declaration page",
        "Yet another generic page",
    ]
    assert _detect_reverse_scan_order(pages) is None
