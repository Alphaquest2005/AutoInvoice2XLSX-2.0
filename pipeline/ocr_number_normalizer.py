"""OCR number-token recovery.

Tesseract and other OCR engines reliably misread certain characters
inside number tokens. This module turns that messy string back into a
``float`` (or ``None`` when nothing recognisable is in there).

The motivating cases — all observed in production declarations:

    "7/.12"   →  71.12   (slash misread of "1" between two digits)
    "1l9.96"  →  119.96  (lowercase L misread of "1")
    "84,48"   →  84.48   (European decimal comma)
    "1,234.56"→ 1234.56  (US thousands-separator comma)
    "$15.99"  →  15.99   (currency prefix)
    "(2)1.50" →  211.50  (paren misread of "1")

All confusable maps live in ``config/ocr_corrections.yaml`` and are
read via the existing ``config_loader.load_ocr_corrections()`` loader.
This module owns no domain literals — every special character it
substitutes comes from config.
"""
from __future__ import annotations

from typing import Optional

from config_loader import load_ocr_corrections


def _strategy() -> dict:
    return load_ocr_corrections()["numeric_token_strategy"]


def _digits() -> frozenset:
    return frozenset(_strategy()["digit_chars"])


def _numeric_chars() -> dict:
    return load_ocr_corrections()["numeric_chars"]


def _decimal_chars() -> dict:
    return load_ocr_corrections()["decimal_chars"]


def _strip_token(raw: str) -> str:
    """Trim leading currency/space characters and trailing junk."""
    strategy = _strategy()
    token = raw.strip()
    prefix_chars = strategy["strip_prefix_chars"]
    suffix_chars = strategy["strip_suffix_chars"]
    while token and token[0] in prefix_chars:
        token = token[1:]
    while token and token[-1] in suffix_chars:
        token = token[:-1]
    return token


def _resolve_decimal_separator(token: str) -> str:
    """Decide whether comma is decimal or thousands separator, and
    normalise the token accordingly. See ``ocr_corrections.yaml``
    section ``decimal_chars`` and ``numeric_token_strategy``.
    """
    decimal_map = _decimal_chars()
    digits = _digits()
    thousands_n = _strategy()["thousands_separator_digit_count"]

    if "·" in decimal_map:
        token = token.replace("·", decimal_map["·"])

    if "." in token:
        return token.replace(",", "")

    if "," not in token:
        return token

    if token.count(",") == 1:
        head, tail = token.split(",", 1)
        if 1 <= len(tail) <= 2 and all(c in digits for c in tail):
            return head + decimal_map[","] + tail
        if len(tail) == thousands_n and all(c in digits for c in tail):
            return head + tail
        return head + tail

    return token.replace(",", "")


def _try_substitutions(token: str, *, max_subs: int) -> Optional[str]:
    """Replace one confusable character at a time and return the first
    fully-numeric variant, capped at ``max_subs`` substitutions.
    """
    digits = _digits()
    chars = list(token)
    confusable_map = _numeric_chars()
    subs_made = 0
    for i, c in enumerate(chars):
        if c in digits or c == ".":
            continue
        if c in confusable_map:
            chars[i] = confusable_map[c]
            subs_made += 1
            if subs_made >= max_subs:
                break
    candidate = "".join(chars)
    if all(c in digits or c == "." for c in candidate) and candidate.count(".") <= 1:
        return candidate
    return None


def normalize_ocr_number(raw: Optional[str]) -> Optional[float]:
    """Best-effort conversion of an OCR-mangled token to a float.

    Returns ``None`` when nothing usable can be recovered. Never
    raises on garbage input — silently returning ``None`` is the
    contract so callers can chain ``or fallback`` cleanly.
    """
    if raw is None:
        return None
    if not isinstance(raw, str):
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None

    strategy = _strategy()
    token = _strip_token(raw)

    if len(token) < strategy["min_token_length"]:
        return None
    if len(token) > strategy["max_token_length"]:
        return None

    token = _resolve_decimal_separator(token)

    try:
        return float(token)
    except ValueError:
        pass

    fixed = _try_substitutions(token, max_subs=strategy["max_substitutions_per_token"])
    if fixed is None:
        return None
    try:
        return float(fixed)
    except ValueError:
        return None
