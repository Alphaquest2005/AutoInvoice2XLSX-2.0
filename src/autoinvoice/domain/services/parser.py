"""Parser domain service - pure functions for extracting invoice data from text.

All functions are pure (no file I/O). The parser works on text input;
PDF extraction is a separate adapter concern.
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

from autoinvoice.domain.models.invoice import Invoice, InvoiceItem, InvoiceMetadata

if TYPE_CHECKING:
    from autoinvoice.domain.ports.config_provider import ConfigProviderPort

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def parse_invoice_text(
    text: str,
    format_spec: dict[str, Any] | None = None,
) -> Invoice:
    """Parse raw invoice text into an Invoice domain model.

    Args:
        text: Extracted invoice text (from PDF or other source).
        format_spec: Optional format specification dict to guide parsing.

    Returns:
        A frozen Invoice with metadata and line items.
    """
    metadata = extract_metadata(text)
    items = extract_items(text, format_spec)
    return Invoice(metadata=metadata, items=items, raw_text=text)


def detect_format(
    text: str,
    available_formats: list[str],
    config_provider: ConfigProviderPort | None = None,
) -> str:
    """Detect which format specification matches the given text.

    Iterates over available formats, loading each spec via the config
    provider and checking its detection rules against the text.

    Args:
        text: Invoice text to analyse.
        available_formats: List of format spec names to try.
        config_provider: Port for loading format spec definitions.

    Returns:
        The name of the matching format, or ``"default"`` if none match.
    """
    if config_provider is None:
        return "default"

    for fmt_name in available_formats:
        spec = config_provider.load_format_spec(fmt_name)
        if spec is None:
            continue
        detect_rules = spec.get("detect")
        if not detect_rules:
            continue
        if _matches_detect_rules(text, detect_rules):
            return fmt_name

    return "default"


def extract_metadata(text: str) -> InvoiceMetadata:
    """Extract invoice header information from text using regex patterns.

    Args:
        text: Raw invoice text.

    Returns:
        Frozen InvoiceMetadata with whatever fields could be extracted.
    """
    return InvoiceMetadata(
        invoice_number=_extract_invoice_number(text),
        invoice_date=_extract_invoice_date(text),
        supplier_name=_extract_supplier_name(text),
        invoice_total=_extract_total(text),
    )


def extract_items(
    text: str,
    format_spec: dict[str, Any] | None = None,
) -> tuple[InvoiceItem, ...]:
    """Extract line items from invoice text.

    Args:
        text: Raw invoice text.
        format_spec: Optional format spec to guide column extraction.

    Returns:
        Tuple of frozen InvoiceItem instances.
    """
    if not text or not text.strip():
        return ()

    if format_spec and "items" in format_spec:
        return _extract_items_with_spec(text, format_spec["items"])

    return _extract_items_generic(text)


def parse_currency(text: str) -> Decimal:
    """Parse a currency string into a Decimal value.

    Handles dollar signs, commas, euro signs, negative values
    (both ``-$50`` and ``($50)`` notation).

    Args:
        text: Currency string such as ``"$1,234.56"`` or ``"(50.00)"``.

    Returns:
        Decimal value. Returns ``Decimal("0")`` for empty/whitespace input.
    """
    if not text or not text.strip():
        return Decimal("0")

    s = text.strip()

    # Detect parenthetical negatives: ($50.00) -> negative
    is_negative = False
    if s.startswith("(") and s.endswith(")"):
        is_negative = True
        s = s[1:-1]
    elif s.startswith("-"):
        is_negative = True
        s = s[1:]

    # Strip currency symbols and whitespace
    s = re.sub(r"[$ \u20ac\u00a3\u00a5]", "", s).strip()

    # Remove thousands separators
    s = s.replace(",", "")

    if not s:
        return Decimal("0")

    try:
        value = Decimal(s)
    except InvalidOperation:
        return Decimal("0")

    return -value if is_negative else value


# ---------------------------------------------------------------------------
# Internal: detection rules
# ---------------------------------------------------------------------------


def _matches_detect_rules(text: str, rules: dict[str, Any]) -> bool:
    """Check whether text satisfies the detect rules from a format spec."""
    all_of = rules.get("all_of", [])
    any_of = rules.get("any_of", [])

    # All "all_of" strings must be present
    for pattern in all_of:
        if pattern not in text:
            return False

    # At least one "any_of" string must be present (if list is non-empty)
    return not (any_of and not any(pattern in text for pattern in any_of))


# ---------------------------------------------------------------------------
# Internal: metadata extraction helpers
# ---------------------------------------------------------------------------

_INVOICE_NUMBER_PATTERNS = [
    re.compile(r"Invoice\s*#\s*([A-Za-z0-9\-]+)", re.IGNORECASE),
    re.compile(
        r"Invoice\s*(?:No\.?|Number)\s*:?\s*([A-Za-z0-9][A-Za-z0-9\-]+)",
        re.IGNORECASE,
    ),
    re.compile(r"Order\s*(?:#|No\.?|Number|ID)\s*:?\s*([A-Za-z0-9\-]+)", re.IGNORECASE),
    re.compile(r"INV[-\s]?(\d+)", re.IGNORECASE),
]

_DATE_PATTERNS = [
    re.compile(r"(?:Invoice\s+)?Date\s*:?\s*(\d{4}-\d{2}-\d{2})", re.IGNORECASE),
    re.compile(r"(?:Invoice\s+)?Date\s*:?\s*(\d{1,2}/\d{1,2}/\d{2,4})", re.IGNORECASE),
    re.compile(r"(\d{4}-\d{2}-\d{2})"),
    re.compile(r"(\d{1,2}/\d{1,2}/\d{4})"),
]

_SUPPLIER_PATTERNS = [
    re.compile(
        r"(?:Supplier|Sold\s+by|Vendor|From|Ship\s+from)\s*:\s*(.+?)(?:\n|$)",
        re.IGNORECASE,
    ),
]

_TOTAL_PATTERNS = [
    re.compile(r"Grand\s+Total\s*:?\s*\$?([\d,]+\.\d{2})", re.IGNORECASE),
    re.compile(r"Order\s+Total\s*:?\s*\$?([\d,]+\.\d{2})", re.IGNORECASE),
    re.compile(r"Invoice\s+Total\s*:?\s*\$?([\d,]+\.\d{2})", re.IGNORECASE),
    re.compile(r"Amount\s+Due\s*:?\s*\$?([\d,]+\.\d{2})", re.IGNORECASE),
    re.compile(r"Total\s*:\s*\$?([\d,]+\.\d{2})", re.IGNORECASE),
]


def _first_match(patterns: list[re.Pattern[str]], text: str) -> str:
    """Return the first captured group from the first matching pattern."""
    for pat in patterns:
        m = pat.search(text)
        if m:
            return m.group(1).strip()
    return ""


def _extract_invoice_number(text: str) -> str:
    return _first_match(_INVOICE_NUMBER_PATTERNS, text)


def _extract_invoice_date(text: str) -> str:
    return _first_match(_DATE_PATTERNS, text)


def _extract_supplier_name(text: str) -> str:
    return _first_match(_SUPPLIER_PATTERNS, text)


def _extract_total(text: str) -> Decimal:
    raw = _first_match(_TOTAL_PATTERNS, text)
    if not raw:
        return Decimal("0")
    return parse_currency(raw)


# ---------------------------------------------------------------------------
# Internal: header-row detection
# ---------------------------------------------------------------------------

_HEADER_KEYWORDS = frozenset(
    {
        "description",
        "qty",
        "quantity",
        "unit",
        "unit price",
        "price",
        "total",
        "amount",
        "item",
        "sku",
        "uom",
        "line",
    }
)


def _is_header_row(line: str) -> bool:
    """Detect whether a line is a table header rather than a data row.

    A line is considered a header if most of its tab-delimited fields match
    known header keywords and none of them look like numbers.
    """
    stripped = line.strip()
    if not stripped:
        return False

    # Split on tab (or 2+ spaces as fallback)
    fields = stripped.split("\t") if "\t" in stripped else re.split(r"\s{2,}", stripped)
    if len(fields) < 2:
        return False

    lower_fields = [f.strip().lower() for f in fields]

    # If the majority of fields are known header words, it's a header
    matches = sum(1 for f in lower_fields if f in _HEADER_KEYWORDS)
    return matches >= len(lower_fields) // 2 + 1


# ---------------------------------------------------------------------------
# Internal: generic item extraction (tab-delimited)
# ---------------------------------------------------------------------------

# Pattern: description <tab> qty <tab> unit_price <tab> total
_TAB_LINE_4COL = re.compile(
    r"^(.+?)\t(\d+(?:\.\d+)?)\t"
    r"[\$]?([\d,]+\.?\d*)\t"
    r"[\$]?([\d,]+\.?\d*)\s*$"
)

# Pattern: description <tab> qty <tab> unit_price (3 columns, compute total)
_TAB_LINE_3COL = re.compile(
    r"^(.+?)\t(\d+(?:\.\d+)?)\t"
    r"[\$]?([\d,]+\.?\d*)\s*$"
)


def _extract_items_generic(text: str) -> tuple[InvoiceItem, ...]:
    """Extract items from text using generic tab-delimited patterns."""
    items: list[InvoiceItem] = []

    for line in text.split("\n"):
        if not line.strip():
            continue
        if _is_header_row(line):
            continue
        if _is_summary_line(line):
            continue

        item = _try_parse_item_line(line)
        if item is not None:
            items.append(item)

    return tuple(items)


def _is_summary_line(line: str) -> bool:
    """Detect subtotal/total/tax/shipping summary lines."""
    stripped = line.strip().lower()
    return bool(
        re.match(
            r"^(sub\s*total|total|grand\s+total|tax|sales\s+tax|shipping|"
            r"freight|discount|amount\s+due|invoice\s+total|order\s+total)\s*:",
            stripped,
        )
    )


def _try_parse_item_line(line: str) -> InvoiceItem | None:
    """Attempt to parse a single line as an invoice item."""
    # Try 4-column tab pattern first
    m = _TAB_LINE_4COL.match(line)
    if m:
        desc = m.group(1).strip()
        qty = Decimal(m.group(2))
        unit = parse_currency(m.group(3))
        total = parse_currency(m.group(4))
        return InvoiceItem(
            description=desc,
            quantity=qty,
            unit_cost=unit,
            total_cost=total,
        )

    # Try 3-column (no total column - compute it)
    m = _TAB_LINE_3COL.match(line)
    if m:
        desc = m.group(1).strip()
        qty = Decimal(m.group(2))
        unit = parse_currency(m.group(3))
        total = qty * unit
        return InvoiceItem(
            description=desc,
            quantity=qty,
            unit_cost=unit,
            total_cost=total,
        )

    return None


# ---------------------------------------------------------------------------
# Internal: format-spec-guided item extraction
# ---------------------------------------------------------------------------


def _extract_items_with_spec(
    text: str,
    items_spec: dict[str, Any],
) -> tuple[InvoiceItem, ...]:
    """Extract items guided by a format specification dict."""
    strategy = items_spec.get("strategy", "line")
    if strategy != "line":
        # Only line strategy implemented for now
        return _extract_items_generic(text)

    line_spec = items_spec.get("line", {})
    delimiter = line_spec.get("delimiter", "\t")
    field_map: dict[str, int] = line_spec.get("field_map", {})
    skip_patterns_raw: list[str] = line_spec.get("skip_patterns", [])

    skip_patterns = [re.compile(p) for p in skip_patterns_raw]

    items: list[InvoiceItem] = []

    for line in text.split("\n"):
        stripped = line.strip()
        if not stripped:
            continue
        if _is_header_row(line):
            continue
        if any(sp.search(stripped) for sp in skip_patterns):
            continue

        fields = stripped.split(delimiter)

        # Need at least description + quantity + one price field
        max_idx = max(field_map.values()) if field_map else 0
        if len(fields) <= max_idx:
            continue

        try:
            desc = fields[field_map["description"]].strip() if "description" in field_map else ""
            qty_raw = fields[field_map["quantity"]].strip() if "quantity" in field_map else "1"
            unit_raw = fields[field_map["unit_price"]].strip() if "unit_price" in field_map else "0"
            total_raw = fields[field_map["total_cost"]].strip() if "total_cost" in field_map else ""

            qty = Decimal(qty_raw)
            unit = parse_currency(unit_raw)
            total = parse_currency(total_raw) if total_raw else qty * unit

            items.append(
                InvoiceItem(
                    description=desc,
                    quantity=qty,
                    unit_cost=unit,
                    total_cost=total,
                )
            )
        except (InvalidOperation, KeyError, IndexError, ValueError):
            continue

    return tuple(items)
