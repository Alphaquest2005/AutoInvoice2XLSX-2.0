"""Tests for the vision-cache trust predicate.

History:
  - v1/v2 predicate invalidated only clear false-negatives.
  - v4 (current): multi-location handwriting scan; any pre-v4 positive hit
    may have missed the tax column, so we re-extract on load.  Only
    true-negatives (no handwriting seen, no values extracted) stay cached.

This test uses the real production predicate via import so it stays in
lockstep with ``pipeline/pdf_splitter.py``.
"""

from pipeline.pdf_splitter import (  # noqa: E402
    _VISION_CACHE_VERSION,
    _should_trust_vision_cache,
)


def test_current_version_trusted():
    cache = {"_cache_version": _VISION_CACHE_VERSION, "has_handwriting": True}
    assert _should_trust_vision_cache(cache) is True


def test_higher_future_version_trusted():
    cache = {"_cache_version": _VISION_CACHE_VERSION + 1, "has_handwriting": True}
    assert _should_trust_vision_cache(cache) is True


def test_v1_true_negative_trusted():
    """Old v1 cache that genuinely found no handwriting — don't waste API
    $$ re-extracting; there's nothing new for v4 to find."""
    cache = {
        "handwritten": {"customs_value_ec": "", "customs_value_usd": ""},
        "has_handwriting": False,
    }
    assert _should_trust_vision_cache(cache) is True


def test_v1_positive_hit_invalidated_for_v4():
    """Pre-v4 positive hits might have captured a margin figure while
    missing the authoritative tax-column figure (e.g. HAWB9590375 cached
    136.57 while the real 155.04 sits in the tax column).  Re-extract."""
    cache = {
        "handwritten": {"customs_value_ec": "136.57"},
        "has_handwriting": True,
    }
    assert _should_trust_vision_cache(cache) is False


def test_v3_positive_hit_invalidated_for_v4():
    """Even v3 positives need re-extraction because v3 prompt restricted
    customs_value_ec search to the left margin only."""
    cache = {
        "_cache_version": 3,
        "handwritten": {"customs_value_ec": "136.57"},
        "has_handwriting": True,
    }
    assert _should_trust_vision_cache(cache) is False


def test_v3_true_negative_trusted():
    cache = {
        "_cache_version": 3,
        "handwritten": {"customs_value_ec": "", "customs_value_usd": ""},
        "has_handwriting": False,
    }
    assert _should_trust_vision_cache(cache) is True


def test_v1_has_hw_true_but_blank_values_invalidated():
    """Pre-v4 cache that flagged handwriting but couldn't extract a value
    — under v4 the broader scan might pick up what v2/v3 missed."""
    cache = {
        "handwritten": {
            "customs_value_ec": "",
            "customs_value_usd": "",
            "other_notes": "some tariff code",
        },
        "has_handwriting": True,
    }
    assert _should_trust_vision_cache(cache) is False


def test_v1_only_tariff_code_invalidated():
    """Only a tariff code in a pre-v4 cache — the new prompt also targets
    tax-column figures + right-margin numbers, so re-extract."""
    cache = {
        "handwritten": {"tariff_code": "62034290"},
        "has_handwriting": True,
    }
    assert _should_trust_vision_cache(cache) is False


def test_v1_only_usd_value_invalidated():
    cache = {
        "handwritten": {"customs_value_usd": "29.49"},
        "has_handwriting": True,
    }
    assert _should_trust_vision_cache(cache) is False


def test_non_dict_untrustworthy():
    assert _should_trust_vision_cache(None) is False
    assert _should_trust_vision_cache([]) is False
    assert _should_trust_vision_cache("") is False
