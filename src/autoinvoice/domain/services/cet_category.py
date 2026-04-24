"""CET category resolution — SSOT for the leaf-to-chapter description walk.

This module is the canonical home for the rule that decides what human-readable
category label belongs with a given 8-digit tariff code. The prior duplicate
implementations in ``pipeline/bl_xlsx_generator.py`` and
``pipeline/stages/supplier_resolver.py`` diverged, which produced the
"polisher/sander shows Drills of all kinds" bug: the supplier_resolver copy
performed a sibling-prefix scan that returned the neighbour's description for
generic leaves like 84672900 ("Other").

The rule implemented here walks UP the HS hierarchy only — never sideways:

    leaf (8-digit) → subheading (6-digit + 00)
                   → heading (4-digit + 0000)
                   → chapter (2-digit + 000000)

If the leaf description itself is "Other" (or a generic placeholder) the result
is qualified with "Other " + parent, unless the parent itself already starts
with "Other" (avoids "Other Other appliances").

The function is pure: it takes a description lookup callable (``Mapping`` or
``Callable[[str], str | None]``) so callers can supply an in-memory cache, a
repository port, or a fake for tests without coupling to SQLite.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping

# Descriptions that carry no information on their own — the walk keeps going.
_GENERIC = frozenset({"OTHER", "OTHER:", "VIRGIN", "NONE", ""})

# Description cleaner — strips prefixes, collapses whitespace, truncates.
_WHITESPACE_RE = re.compile(r"\s+")
_NEWLINE_RE = re.compile(r"[\n\r\t]+")
_OF_SINGLE_WORD_RE = re.compile(r"^Of\s+\w+$")
_CATEGORY_PREFIX_RE = re.compile(r"^CATEGORY:\s*", re.IGNORECASE)

DescLookup = Mapping[str, str] | Callable[[str], str | None]


def _lookup(source: DescLookup, code: str) -> str:
    """Normalize Mapping/Callable lookups to a str (empty when absent)."""
    if callable(source):
        result = source(code)
        return result or ""
    return source.get(code, "") or ""


def _is_generic(desc: str) -> bool:
    """Return True for placeholder descriptions like ``Other`` / ``Other:``."""
    if not desc:
        return True
    return desc.strip().rstrip(":").strip().upper() in _GENERIC


def _is_useful(desc: str) -> bool:
    """Return True when *desc* would read meaningfully as a category label."""
    cleaned = desc.strip().rstrip(":").strip()
    if not cleaned or cleaned.upper() in _GENERIC:
        return False
    if _OF_SINGLE_WORD_RE.match(cleaned):
        return False
    if len(cleaned) < 3:
        return False
    return not cleaned.upper().startswith("INVALID:")


def _clean(desc: str) -> str:
    """Normalise *desc* for display (strip, collapse, truncate)."""
    if not desc:
        return ""
    desc = _NEWLINE_RE.sub(" ", desc).strip()
    desc = _WHITESPACE_RE.sub(" ", desc)
    desc = _CATEGORY_PREFIX_RE.sub("", desc)
    desc = desc.rstrip(":").strip()
    for sep in (" - ", "; ", " (see "):
        if sep in desc:
            desc = desc.split(sep, 1)[0].strip()
    if len(desc) > 80:
        desc = desc[:77] + "..."
    return desc


def _parent_codes(tariff_code: str) -> tuple[str, str, str]:
    """Return (subheading, heading, chapter) in walk-up order."""
    return (
        tariff_code[:6] + "00",
        tariff_code[:4] + "0000",
        tariff_code[:2] + "000000",
    )


def category_for(tariff_code: str, descriptions: DescLookup) -> str:
    """Resolve the display category for *tariff_code* using *descriptions*.

    Args:
        tariff_code: 8-digit CET end-node code. Anything else returns ``""``.
        descriptions: Mapping or callable ``code -> description`` (empty
            string / ``None`` means "not found").

    Returns:
        A human-readable category label, or ``""`` if no useful ancestor
        description exists. The caller is responsible for any further fallback
        (classifier category, raw item description, etc.).

    The function never performs a sibling/prefix scan — returning a neighbour's
    description for a generic leaf was the root cause of the polisher/sander
    "Drills of all kinds" bug.
    """
    if not tariff_code or tariff_code == "00000000" or len(tariff_code) != 8:
        return ""

    leaf_desc = _lookup(descriptions, tariff_code)
    leaf_generic = _is_generic(leaf_desc)

    # Step 1 — concrete leaf wins outright.
    if leaf_desc and not leaf_generic:
        return _clean(leaf_desc)

    # Step 2 — walk UP the hierarchy.
    seen = {tariff_code}
    for parent_code in _parent_codes(tariff_code):
        # Skip codes we've already tried — e.g. a leaf ending in "00"
        # collapses to the same 6-digit subheading code.
        if parent_code in seen:
            continue
        seen.add(parent_code)
        parent_desc = _lookup(descriptions, parent_code)
        if not parent_desc or not _is_useful(parent_desc):
            continue
        cleaned = _clean(parent_desc)
        if leaf_generic and leaf_desc:
            # Qualify the parent with "Other " unless it already starts with it.
            if cleaned.upper().startswith("OTHER"):
                return cleaned
            return f"Other {cleaned}"
        return cleaned

    # Step 3 — no useful ancestor; let the caller decide the fallback.
    return ""


__all__ = ["category_for"]
