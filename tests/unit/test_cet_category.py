"""Unit tests for src/autoinvoice/domain/services/cet_category.py.

These assert the SSOT rule: walk UP the hierarchy only, never sideways.
The regression case is 84672900 ("Other") which previously returned
"Drills of all kinds" due to a sibling-prefix scan.
"""

from __future__ import annotations

import pytest

from autoinvoice.domain.services.cet_category import category_for


# ── Fixture descriptions — a trimmed CET slice covering the regression cases ──

DESCRIPTIONS = {
    # Polisher/sander chain — the bug case
    "84672900": "Other",                          # generic leaf
    "84672000": "Electro-mechanical hand tools:", # subheading (has trailing colon)
    "84670000": "Tools for working in the hand",  # heading
    # Sibling that SHOULD NEVER be returned
    "84672100": "Drills of all kinds",
    # Anchors — remapped by rules to leaf 73160000
    "73160000": "Anchors, grapnels and parts thereof",
    # Fish-hooks
    "95072000": "Fish-hooks",
    # A chain that forces a walk to chapter
    "99999999": "Other",
    "99999900": "Other:",
    "99990000": "Other",
    "99000000": "Miscellaneous manufactured articles",
    # An "Of metal" short phrase that should be skipped by _is_useful
    "12341234": "Other",
    "12341200": "Of metal",
    "12340000": "Cooking utensils",
    # A leaf whose parent starts with "Other" → must NOT double-prefix
    "55556789": "Other",
    "55556700": "Other appliances",
    "55550000": "Appliances",
}


def test_leaf_wins_when_concrete() -> None:
    assert category_for("95072000", DESCRIPTIONS) == "Fish-hooks"
    assert category_for("73160000", DESCRIPTIONS) == "Anchors, grapnels and parts thereof"


def test_generic_leaf_walks_to_useful_parent() -> None:
    # The polisher/sander regression — must NOT return "Drills of all kinds".
    # For 84672900 the 6-digit subheading prefix collapses to the leaf
    # itself, so the walk moves on to the 4-digit heading (84670000).
    result = category_for("84672900", DESCRIPTIONS)
    assert "Drills" not in result
    assert result == "Other Tools for working in the hand"


def test_generic_leaf_uses_six_digit_subheading_when_distinct() -> None:
    # For a code whose 7th–8th digits are non-zero, the subheading code is
    # distinct from the leaf and should be picked up first.
    descriptions = {
        "84672910": "Other",                          # leaf (generic)
        "84672900": "Electro-mechanical hand tools",  # 6-digit subheading
        "84670000": "Tools for working in the hand",  # heading
    }
    result = category_for("84672910", descriptions)
    assert result == "Other Electro-mechanical hand tools"


def test_generic_leaf_does_not_double_prefix_other() -> None:
    # Parent already starts with "Other" — avoid "Other Other appliances".
    assert category_for("55556789", DESCRIPTIONS) == "Other appliances"


def test_short_of_phrase_is_skipped() -> None:
    # "Of metal" is too generic; walk continues to "Cooking utensils".
    result = category_for("12341234", DESCRIPTIONS)
    assert result == "Other Cooking utensils"


def test_walks_all_the_way_to_chapter() -> None:
    # Whole chain is "Other" / "Other:" until the chapter.
    result = category_for("99999999", DESCRIPTIONS)
    assert result == "Other Miscellaneous manufactured articles"


def test_empty_or_invalid_code_returns_empty() -> None:
    assert category_for("", DESCRIPTIONS) == ""
    assert category_for("00000000", DESCRIPTIONS) == ""
    assert category_for("ABC", DESCRIPTIONS) == ""
    assert category_for("1234567", DESCRIPTIONS) == ""      # 7 digits
    assert category_for("123456789", DESCRIPTIONS) == ""    # 9 digits


def test_accepts_callable_lookup() -> None:
    calls: list[str] = []

    def lookup(code: str) -> str:
        calls.append(code)
        return DESCRIPTIONS.get(code, "")

    assert category_for("95072000", lookup) == "Fish-hooks"
    assert calls == ["95072000"]


def test_missing_code_returns_empty() -> None:
    # No leaf, no parents — nothing to say.
    assert category_for("88888888", DESCRIPTIONS) == ""


def test_trailing_colon_is_stripped() -> None:
    # Parent description "Electro-mechanical hand tools:" — colon removed.
    result = category_for("84672900", DESCRIPTIONS)
    assert not result.endswith(":")


def test_no_sibling_scan() -> None:
    """The invariant: a generic leaf NEVER picks up a sibling's description.

    Previously, a flat prefix scan against _cet_desc_cache would return
    84672100's "Drills of all kinds" for any 8467xxxx generic leaf. This
    test pins that regression shut.
    """
    descriptions = {
        # Sibling leaves — one of them has a tempting description.
        "84672100": "Drills of all kinds",
        "84672200": "Saws",
        "84672900": "Other",
        # NO 6-digit subheading, heading, or chapter — forces the function
        # to either return empty OR (incorrectly) pick a sibling.
    }
    result = category_for("84672900", descriptions)
    assert result == "", f"Expected empty (no ancestor), got {result!r}"
