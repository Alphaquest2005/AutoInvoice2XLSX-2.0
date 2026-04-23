"""Tests for the vision-cache trust predicate (Fix C).

Old v1 caches that recorded ``has_handwriting: false`` + blank customs
values are treated as stale, so the next run can re-extract under the v2
false-negative retry path.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "pipeline"))


# ---------------------------------------------------------------------------
# Predicate mirrored from pdf_splitter._cache_is_trustworthy (closure-local).
# If pdf_splitter's version changes, update this one in lockstep.
# ---------------------------------------------------------------------------

def _cache_is_trustworthy(cached):
    if not isinstance(cached, dict):
        return False
    if cached.get('_cache_version', 1) >= 2:
        return True
    hw = cached.get('handwritten') or {}
    ec = str(hw.get('customs_value_ec') or '').strip()
    usd = str(hw.get('customs_value_usd') or '').strip()
    has_hw = cached.get('has_handwriting')
    if has_hw is False and not ec and not usd:
        return False
    return True


def test_v1_false_negative_is_untrustworthy():
    cache = {
        'handwritten': {'customs_value_ec': '', 'customs_value_usd': ''},
        'has_handwriting': False,
        'printed': {'waybill': 'HAWB9603312'},
    }
    assert _cache_is_trustworthy(cache) is False


def test_v1_with_value_is_trustworthy():
    cache = {
        'handwritten': {'customs_value_ec': '80.06', 'customs_value_usd': ''},
        'has_handwriting': True,
        'printed': {'waybill': 'HAWB9600998'},
    }
    assert _cache_is_trustworthy(cache) is True


def test_v2_false_negative_is_trustworthy():
    """v2 caches went through the retry path already — trust them."""
    cache = {
        'handwritten': {'customs_value_ec': '', 'customs_value_usd': ''},
        'has_handwriting': False,
        '_cache_version': 2,
    }
    assert _cache_is_trustworthy(cache) is True


def test_v1_has_hw_true_but_blank_values_is_trustworthy():
    """If LLM said handwriting exists but couldn't extract a number, still
    trust the cache (re-extracting won't help). The gate is specifically
    for false-negative 'has_handwriting: false' results."""
    cache = {
        'handwritten': {
            'customs_value_ec': '',
            'customs_value_usd': '',
            'other_notes': 'some tariff code',
        },
        'has_handwriting': True,
    }
    assert _cache_is_trustworthy(cache) is True


def test_non_dict_is_untrustworthy():
    assert _cache_is_trustworthy(None) is False
    assert _cache_is_trustworthy([]) is False
    assert _cache_is_trustworthy('') is False


def test_v1_only_usd_value_is_trustworthy():
    cache = {
        'handwritten': {'customs_value_usd': '29.49'},
        'has_handwriting': True,
    }
    assert _cache_is_trustworthy(cache) is True
