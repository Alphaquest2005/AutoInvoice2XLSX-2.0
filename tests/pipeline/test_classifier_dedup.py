"""
WS-B2: TDD guard that batch classification dedupes identical descriptions
before calling the LLM.

Given 100 items where 40 share the exact same description, the LLM should
receive at most 60 item-slots (one per unique description), not 100.

This test is RED until WS-B3(a) implements dedup in classify_items_batch().
"""

from __future__ import annotations

import os
import sys

import pytest

# Put pipeline/ on sys.path so `import classifier_batch` works
_PIPELINE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "pipeline"))
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)


class _CountingStub:
    """Stand-in for core.llm_client.LLMClient that tracks item-slot counts.

    Each call to call_json receives a batch prompt with numbered items:
        "1. widget-a\n2. widget-b\n…"
    We parse the numbers to count how many *items* the batch classifier
    actually sent to the LLM layer (not how many HTTP calls it made).
    """

    def __init__(self) -> None:
        self.calls: int = 0
        self.item_slots: int = 0
        self.last_prompts: list[str] = []

    def call_json(
        self,
        user_message: str,
        system_prompt: str = "",
        max_tokens: int = 4096,
        use_cache: bool = True,
        cache_key_extra: str = "",
    ) -> dict:
        import re as _re

        self.calls += 1
        self.last_prompts.append(user_message)
        nums = _re.findall(r"^\s*(\d+)\.\s", user_message, flags=_re.MULTILINE)
        self.item_slots += len(nums)
        # Return a valid classification for every numbered item so the
        # batch parser accepts the response and doesn't fall through to
        # the urllib fallback.
        return {
            n: {
                "code": "39269090",  # valid CET 8-digit code (other plastics)
                "category": "PLASTICS",
                "confidence": 0.9,
                "reasoning": "stub",
            }
            for n in nums
        }

    # Matching signature for LLMClient.call (rarely used, but keep it safe).
    def call(self, *args, **kwargs) -> str:
        return ""


@pytest.fixture
def counting_stub(monkeypatch):
    """Install the counting stub in place of the real LLM client."""
    import core.llm_client as _llm_mod  # type: ignore

    stub = _CountingStub()
    monkeypatch.setattr(_llm_mod, "_client", stub, raising=False)
    monkeypatch.setattr(_llm_mod, "get_llm_client", lambda: stub)

    # Neutralise web-context gathering so the test doesn't touch the network
    import classifier as _cls  # type: ignore
    import classifier_batch as _clsb  # type: ignore

    monkeypatch.setattr(_cls, "_gather_web_context", lambda d, cfg=None: "")
    monkeypatch.setattr(_clsb, "_gather_web_context", lambda d, cfg=None: "")

    return stub


def test_batch_dedups_identical_descriptions(counting_stub, tmp_path):
    """60 unique items + 40 duplicates → ≤60 LLM item-slots, all 100 classified."""
    from classifier_batch import classify_items_batch  # type: ignore

    # Use descriptions that are NOT in the assessed DB, NOT in any rules,
    # and NOT in the JSON cache, so every single one falls through to LLM.
    # "qwzxyy-unique-<i>" is obviously synthetic.
    items: list[dict] = []
    for i in range(60):
        items.append({"description": f"qwzxyy-unique-widget-{i:03d}"})
    duplicate_desc = "qwzxyy-shared-widget"
    for _ in range(40):
        items.append({"description": duplicate_desc})

    assert len(items) == 100

    results = classify_items_batch(
        items,
        rules=[],
        noise_words=set(),
        config={"base_dir": str(tmp_path)},  # empty base_dir → no assessed data
        gather_web_context=False,
    )

    # Every item must have a classification entry
    assert len(results) == 100, f"Expected 100 results, got {len(results)}"

    # The 40 duplicate items must all resolve to the same code as the
    # representative that was sent to the LLM. The dedup implementation is
    # expected to fan the result back out.
    dup_codes = {results[i].get("code") for i in range(60, 100) if results[i] is not None}
    assert len(dup_codes) == 1, (
        f"All 40 duplicates must share the same classification; got {dup_codes}"
    )
    dup_code = dup_codes.pop()
    assert dup_code and dup_code != "UNKNOWN", (
        f"Duplicate items were not classified (code={dup_code!r})"
    )

    # The critical assertion: no more than 60 item-slots should reach the LLM.
    # Currently the implementation sends all 100, so this assertion fails.
    assert counting_stub.item_slots <= 60, (
        f"Expected ≤60 LLM item-slots for 60 unique descriptions; "
        f"got {counting_stub.item_slots} (dedup is missing)"
    )


def test_batch_resolves_category_lookup_without_llm(counting_stub, tmp_path):
    """Parity with single path: batch Layer 2b must catch brand/category hits.

    Items like "Polo Shirt, Male Uniform Polyester (Large, Navy)" don't appear
    in the assessed DB, don't match any rule, and aren't in the JSON cache, but
    they DO match CATEGORY_HS_CODES via keyword scoring. The single path picks
    them up via lookup_hs_code_web's brand/category layers; the batch path must
    do the same before resorting to an LLM call.

    Regression guard: before WS-B3(d), these items dropped straight to the LLM
    (wasting a call) and in mock mode ended up as UNKNOWN.
    """
    from classifier_batch import classify_items_batch  # type: ignore

    items = [
        {"description": "Polo Shirt, Male Uniform Polyester (Large, Navy)"},
        {"description": "Polo Shirt, Female Uniform Polyester (Large, Navy)"},
    ]

    results = classify_items_batch(
        items,
        rules=[],
        noise_words=set(),
        config={"base_dir": str(tmp_path)},
        gather_web_context=False,
    )

    assert len(results) == 2
    for r in results:
        assert r is not None
        assert r.get("code") and r["code"] != "UNKNOWN", (
            f"batch failed to classify category_lookup item: {r!r}"
        )
        assert r.get("source") in {"category_lookup", "brand_lookup"}, (
            f"expected category/brand source, got {r.get('source')!r}"
        )

    # Critical: these items must NEVER reach the LLM — they're local-layer hits.
    assert counting_stub.item_slots == 0, (
        f"category_lookup items should resolve locally; "
        f"got {counting_stub.item_slots} LLM item-slots"
    )
