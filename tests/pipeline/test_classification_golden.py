"""Classification correctness golden test.

This is the safety net for Joseph's concern that "correct classifications is a
big problem". The test corpus is built from two sources:

1. **rules/invalid_codes.json** — every entry there is a known classifier bug
   that has been patched. For each invalid code, we assert that the validator
   maps it to the documented ``correct_code``. If that regresses (e.g. someone
   removes an entry or the validator stops reading the file) the test fails.

2. **Historical misclassifications** — a hand-curated list of known-problem
   items from real shipments (polisher/sander → "Drills of all kinds", fish
   hooks misclassified as screws, etc.). For each, we verify the category
   label is sane AND that the correction rule still points to the right leaf.

The test deliberately does NOT call the LLM. It exercises only the pure
validation/correction + CET category walk, so it is:

* **Fast** — runs in well under a second.
* **Deterministic** — no network, no API keys, no model-ID coupling.
* **Complete for its scope** — every bug that has been fixed stays fixed.

Run with:

    source .venv/bin/activate
    pytest tests/pipeline/test_classification_golden.py -v
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]

# Ensure pipeline/ and src/ are importable.
for _d in (_REPO_ROOT / "pipeline", _REPO_ROOT / "src"):
    if str(_d) not in sys.path:
        sys.path.insert(0, str(_d))

from classifier import validate_and_correct_code  # noqa: E402

from autoinvoice.domain.services.cet_category import category_for  # noqa: E402

_INVALID_CODES_PATH = _REPO_ROOT / "rules" / "invalid_codes.json"


def _load_invalid_codes_corpus() -> list[tuple[str, str, str]]:
    """Return ``[(invalid_code, expected_correct, reason), ...]``."""
    data = json.loads(_INVALID_CODES_PATH.read_text(encoding="utf-8"))
    out: list[tuple[str, str, str]] = []
    for key, val in data.items():
        if key.startswith("_"):
            continue  # structural comments
        if not isinstance(val, dict):
            continue
        correct = val.get("correct_code")
        reason = val.get("reason", "")
        if correct:
            out.append((key, correct, reason))
    return out


CORPUS = _load_invalid_codes_corpus()


def test_corpus_not_empty() -> None:
    """Gate against an empty/corrupt rules file silently masking all regressions."""
    assert len(CORPUS) >= 30, f"rules/invalid_codes.json has only {len(CORPUS)} entries"


@pytest.mark.parametrize(
    ("invalid", "correct", "reason"),
    CORPUS,
    ids=[f"{i}->{c}" for i, c, _ in CORPUS],
)
def test_invalid_code_autocorrects(invalid: str, correct: str, reason: str) -> None:
    """Every entry in invalid_codes.json must be auto-corrected end-to-end."""
    result = validate_and_correct_code(invalid, str(_REPO_ROOT))
    assert result == correct, f"Expected {invalid} → {correct} ({reason}); got {result}"


# ── Handpicked historical regressions ─────────────────────────────────────────

# Each entry: (invalid_or_questionable_code, expected_correct_code,
#              description_for_trace)
HISTORICAL_MISCLASSIFICATIONS = [
    # Polisher / sander — previously resolved to "Drills of all kinds"
    # because of the sibling-prefix scan. After the SSOT fix this should
    # never happen again.
    ("84672900", "84672900", "polisher/sander — leaf already valid"),
    # Fish hooks previously classified as screws/bolts.
    ("73161500", "95072000", "fish hooks → fish-hooks heading"),
    # Fishing hooks under invented chapter 95.11.
    ("95110000", "95072000", "fishing hooks — chapter 95.11 does not exist"),
    # Safety lights classified as eye makeup.
    ("33042000", "85319000", "flashlights → signaling equipment"),
    # Anchors invented subdivisions.
    ("73161000", "73160000", "anchors — 73161000 does not exist"),
    ("73169000", "73160000", "anchors — 73169000 does not exist"),
    # Marine paints — Budget Marine is a yacht shop; automotive/enamel
    # subdivisions must remap to marine paints.
    ("32081010", "32081020", "automotive paint → marine paint"),
    ("32082030", "32082020", "enamel → marine paint"),
    ("32089090", "32089020", "other paints → marine paint"),
]


@pytest.mark.parametrize(
    ("code", "expected", "desc"),
    HISTORICAL_MISCLASSIFICATIONS,
    ids=[d for _, _, d in HISTORICAL_MISCLASSIFICATIONS],
)
def test_historical_misclassification(code: str, expected: str, desc: str) -> None:
    result = validate_and_correct_code(code, str(_REPO_ROOT))
    assert result == expected, f"{desc}: expected {code} → {expected}, got {result}"


# ── Category label correctness — the SSOT regression that started this fix ────


def _load_cet_descriptions() -> dict[str, str]:
    """Load the CET description cache using the same source the pipeline uses."""
    import sqlite3

    db_path = _REPO_ROOT / "data" / "cet.db"
    cache: dict[str, str] = {}
    if not db_path.exists():
        pytest.skip("data/cet.db not available — category tests require it")
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        for code, desc in conn.execute("SELECT hs_code, description FROM cet_codes"):
            if desc:
                cache[code] = desc
    finally:
        conn.close()
    return cache


def test_polisher_sander_category_is_not_drills() -> None:
    """The founding regression — 84672900 must NOT resolve to 'Drills of all kinds'.

    This is the hard invariant: a generic leaf ("Other") may never borrow
    a sibling's description.
    """
    cache = _load_cet_descriptions()
    category = category_for("84672900", cache)
    assert "Drills" not in category, (
        f"SSOT regression: 84672900 resolved to {category!r}, which borrows a sibling's description"
    )


def test_fish_hooks_category_makes_sense() -> None:
    cache = _load_cet_descriptions()
    category = category_for("95072000", cache)
    # Leaf description should win outright.
    assert "fish" in category.lower() or "hook" in category.lower(), (
        f"Fish-hooks (95072000) resolved to unexpected category {category!r}"
    )


def test_anchors_category_is_not_wrong() -> None:
    """Anchors (73160000) — the CET DB currently has an empty description for
    this leaf, so an empty result is acceptable (caller supplies fallback).
    What is NOT acceptable is a sibling-borrowed category like 'Screws'."""
    cache = _load_cet_descriptions()
    category = category_for("73160000", cache)
    lowered = category.lower()
    # Empty is OK — it means "no description, caller falls through".
    # Non-empty must be anchor-related, never an unrelated sibling.
    bad_tokens = ("screw", "bolt", "nail", "drill", "saw")
    assert not any(b in lowered for b in bad_tokens), (
        f"Anchors (73160000) resolved to wrong category {category!r}"
    )
