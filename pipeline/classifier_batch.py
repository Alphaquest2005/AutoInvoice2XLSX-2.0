#!/usr/bin/env python3
# ruff: noqa: E501
"""
Batch Classification Strategy for CARICOM tariff codes.

Drop-in alternative to the single-item classify_with_llm() calls.
Instead of 1 LLM call per item (or group), this sends batches of ~20-30
items per LLM call, dramatically reducing API round-trips.

Uses the SAME classification rules, prompts, and CET validation as
pipeline/classifier.py — the only difference is batching.

Usage:
    from classifier_batch import BatchClassifier

    bc = BatchClassifier(config={'base_dir': '.'})
    results = bc.classify_batch(items)
    # results is a list of dicts in the same format as classify_with_llm()
"""

import json
import logging
import os
import sys

logger = logging.getLogger(__name__)

# Ensure pipeline dir is on path for sibling imports
_pipeline_dir = os.path.dirname(os.path.abspath(__file__))
if _pipeline_dir not in sys.path:
    sys.path.insert(0, _pipeline_dir)

from classification_db import normalize_description  # noqa: E402
from classifier import (  # noqa: E402
    _check_lookup_cache,
    _extract_search_terms,
    _gather_web_context,
    _load_llm_settings,
    _save_to_cache,
    classify_item,
    lookup_assessed_classification,
    validate_and_correct_code,
)

# ── The batch classification prompt ─────────────────────────────────────────
# Mirrors _build_classification_prompt() but for multiple items at once.

_BATCH_SYSTEM_PROMPT = """You are a CARICOM customs tariff classification expert. You will classify multiple products at once.

CLASSIFICATION RULES (follow strictly):

1. CLASSIFY BY PRIMARY MATERIAL when the item is a simple article made of one material:
   - Zinc articles (anodes, plates, bars, weld-on sacrificial anodes) → Chapter 79 (e.g. 79070090)
   - Aluminum articles → Chapter 76
   - Iron/steel articles → Chapter 73
   - Copper articles → Chapter 74
   - Plastic articles → Chapter 39
   - Rubber articles → Chapter 40
   "Zn" = Zinc. Items described as "Zn", "Zinc", "Anode", or "Sacrificial" in a marine context are zinc cathodic protection anodes → 79070090

2. CLASSIFY BY FUNCTION when the item is a machine, apparatus, or complex part:
   - Pumps, macerators, water pumps → Chapter 84
   - Electrical equipment, motors, switches → Chapter 85
   - Valves (check, relief, safety) → 8481
   - Hand tools, multi-tools, pliers → Chapter 82

3. COMMON MARINE/BOATING PRODUCTS (this is a Caribbean marine trade context):
   - Zinc anodes (any shape: teardrop, collar, plate, disc, hull, shaft, rudder) → 79070090
   - Zinc galvanizing spray/coating → 32091000
   - Marine paint, antifouling → Chapter 32
   - Diving masks, swim goggles → 90049000
   - Snorkels, fins → 95062900
   - Spear guns, spear heads → 95079010
   - Wetsuits → 40159000
   - Multi-tools (Leatherman, etc.) → 82055100
   - Glasses/sunglass straps, retainers → 63079090

4. NEVER classify a simple zinc/metal article as furniture hardware (Chapter 83) or machinery parts (Chapter 84) unless it is genuinely a machine component with moving parts.

5. Codes MUST be exactly 8 digits. Use the CARICOM CET national subdivision (last 2 digits), not US HTS subdivisions.

For EACH item, provide an 8-digit HS code, category label, confidence (0.0-1.0), and brief reasoning."""


def _build_batch_user_message(items_with_context: list[tuple[int, str, str]]) -> str:
    """
    Build the user message for a batch classification call.

    Args:
        items_with_context: List of (item_number, description, web_context) tuples.

    Returns:
        Formatted user message string.
    """
    lines = []
    for num, desc, web_ctx in items_with_context:
        ctx_str = f"\n   Web context: {web_ctx[:200]}" if web_ctx else ""
        lines.append(f"{num}. {desc}{ctx_str}")

    item_list = "\n".join(lines)

    return f"""Classify these {len(items_with_context)} products with 8-digit CARICOM CET tariff codes.

{item_list}

Respond with ONLY a JSON object mapping item numbers to classification objects.
Example format:
{{"1": {{"code": "79070090", "category": "MARINE HARDWARE", "confidence": 0.9, "reasoning": "Zinc anode for marine use"}}, "2": {{"code": "73181500", "category": "FASTENERS", "confidence": 0.85, "reasoning": "Steel bolts"}}}}"""


class BatchClassifier:
    """
    Batch classification strategy — sends multiple items per LLM call.

    Maintains the same interface and validation as the single-item classifier
    so it can be used as a drop-in replacement.
    """

    # Default batch size — balances context window usage vs API call count.
    # WS-B3(c): raised from 25 → 60. Claude Haiku / GLM-5 have >=200K context
    # windows, and one classification line is ~60 tokens, so 60 items fits
    # comfortably with headroom for web-context snippets. Halving the call
    # count roughly halves per-invoice LLM latency when many items fall
    # through to the LLM layer.
    DEFAULT_BATCH_SIZE = 60

    def __init__(self, config: dict = None, batch_size: int = None):
        """
        Args:
            config: Pipeline config dict (needs 'base_dir' at minimum).
            batch_size: Number of items per LLM call (default 60).
        """
        self.config = config or {}
        self.base_dir = self.config.get('base_dir', '.')
        self.batch_size = batch_size or self.DEFAULT_BATCH_SIZE

    def classify_batch(
        self,
        items: list[dict],
        rules: list[dict] = None,
        noise_words: set = None,
        gather_web_context: bool = True,
    ) -> list[dict]:
        """
        Classify a list of items using batched LLM calls.

        This follows the same multi-layer approach as the single-item pipeline:
          Layer 0: Assessed classifications (customs-verified)
          Layer 1: Rule-based classification
          Layer 2: Cache lookup
          Layer 3: Batch LLM classification (the key difference — batched!)

        Args:
            items: List of dicts, each with at least 'description' key.
                   Can also have 'po_item_desc', 'supplier_item_desc'.
            rules: Classification rules (loaded from rules/classification_rules.json).
            noise_words: Set of noise words for rule matching.
            gather_web_context: Whether to gather web context for LLM items.

        Returns:
            List of classification result dicts (same length as items),
            each in the standard format:
              {'code': '79070090', 'category': 'MARINE', 'confidence': 0.9,
               'source': 'batch_llm_classification', 'notes': '...'}
            Items that could not be classified get code='UNKNOWN'.
        """
        # WS-B4: lazily seed classifications.db on first batch classification.
        # Cheap no-op after the first call per process.
        try:
            from classification_db import ensure_db_seeded
            ensure_db_seeded(self.base_dir)
        except Exception as _e:
            logger.debug(f"[DB-SEED] ensure_db_seeded failed: {_e}")

        results = [None] * len(items)
        needs_llm = []  # (original_index, description) tuples

        # ── Layers 0-2: local classification (no LLM needed) ──
        for i, item in enumerate(items):
            desc = self._get_description(item)
            if not desc:
                results[i] = self._unknown_result()
                continue

            # Layer 0: Assessed
            assessed = lookup_assessed_classification(desc, self.base_dir)
            if assessed and assessed.get('code') and assessed['code'] != 'UNKNOWN':
                results[i] = assessed
                continue

            # Layer 1: Rule-based
            if rules is not None:
                rule_result = classify_item(
                    desc, rules, noise_words or set(), self.base_dir
                )
                if rule_result and rule_result.get('code') and rule_result['code'] != 'UNKNOWN':
                    results[i] = rule_result
                    continue

            # Layer 2: Cache
            search_terms = _extract_search_terms(desc)
            if search_terms:
                cache_result = _check_lookup_cache(search_terms, self.base_dir)
                if cache_result and cache_result.get('code') and cache_result['code'] != 'UNKNOWN':
                    cache_result['code'] = validate_and_correct_code(
                        cache_result['code'], self.base_dir
                    )
                    results[i] = cache_result
                    continue

            # Needs LLM
            needs_llm.append((i, desc))

        if not needs_llm:
            # Everything was classified locally
            return [r if r else self._unknown_result() for r in results]

        logger.info(
            f"[BATCH] {len(items)} items: "
            f"{len(items) - len(needs_llm)} classified locally, "
            f"{len(needs_llm)} need LLM"
        )

        # ── Layer 3: Batch LLM classification ──
        # WS-B3(a): dedup by normalized description before sending to LLM.
        # We use classification_db.normalize_description — the same canonical
        # normalization the SQLite cache layer uses. Collapsing exact
        # duplicates before the LLM pays off on invoices with many similar
        # SKUs (e.g. "Shirt Blue Small", "Shirt Blue Medium" … all normalize
        # to "shirt blue"). Representatives are chosen as the first item in
        # each group, and the classification is fanned back out to siblings.
        groups: dict[str, list[tuple[int, str]]] = {}
        for orig_idx, desc in needs_llm:
            key = normalize_description(desc) or desc.strip().lower()
            groups.setdefault(key, []).append((orig_idx, desc))

        dedup_representatives: list[tuple[int, str]] = [
            members[0] for members in groups.values()
        ]

        dedup_savings = len(needs_llm) - len(dedup_representatives)
        if dedup_savings > 0:
            logger.info(
                f"[BATCH] Dedup: {len(needs_llm)} items → "
                f"{len(dedup_representatives)} unique ({dedup_savings} duplicates skipped)"
            )

        # WS-B3(b): gather web context only for items that will actually
        # hit the LLM (post-dedup). Items resolved by assessed/rules/cache
        # never need a web fetch. Gather per-representative, not per-item,
        # so duplicates inherit the context for free.
        web_contexts: dict[int, str] = {}
        if gather_web_context:
            for idx, desc in dedup_representatives:
                try:
                    ctx = _gather_web_context(desc, self.config)
                    if ctx:
                        web_contexts[idx] = ctx
                except Exception as e:
                    logger.debug(f"Web context failed for item {idx}: {e}")

        # Split the deduped representatives into batches
        batches = []
        for batch_start in range(0, len(dedup_representatives), self.batch_size):
            batch = dedup_representatives[batch_start:batch_start + self.batch_size]
            batches.append(batch)

        total_calls = len(batches)
        logger.info(
            f"[BATCH] Sending {len(dedup_representatives)} unique items "
            f"in {total_calls} LLM calls (batch_size={self.batch_size})"
        )

        for batch_num, batch in enumerate(batches, 1):
            batch_results = self._classify_batch_llm(batch, web_contexts)

            for orig_idx, classification in batch_results.items():
                results[orig_idx] = classification

            logger.info(
                f"[BATCH] Batch {batch_num}/{total_calls}: "
                f"classified {len(batch_results)}/{len(batch)} items"
            )

        # Fan out dedup'd classifications to all members of each group.
        # Representatives whose LLM call succeeded have results[rep_idx] set;
        # their dup siblings still have None. Copy the representative's
        # result into each sibling so the final list is fully populated.
        for _key, members in groups.items():
            if len(members) < 2:
                continue
            rep_idx = members[0][0]
            rep_result = results[rep_idx]
            if rep_result is None:
                continue  # representative failed; leave siblings as UNKNOWN
            for sibling_idx, _sibling_desc in members[1:]:
                # shallow-copy so downstream mutations don't bleed
                results[sibling_idx] = dict(rep_result)

        # Fill any remaining None entries with UNKNOWN
        return [r if r else self._unknown_result() for r in results]

    def _classify_batch_llm(
        self,
        batch: list[tuple[int, str]],
        web_contexts: dict[int, str],
    ) -> dict[int, dict]:
        """
        Send a single batch of items to the LLM for classification.

        Args:
            batch: List of (original_index, description) tuples.
            web_contexts: Dict mapping original_index -> web context string.

        Returns:
            Dict mapping original_index -> classification result dict.
        """
        # Build numbered item list for the prompt
        # Use 1-based numbering in the prompt, track mapping back to original indices
        prompt_items = []
        num_to_orig = {}  # prompt_number -> original_index

        for prompt_num, (orig_idx, desc) in enumerate(batch, 1):
            web_ctx = web_contexts.get(orig_idx, "")
            prompt_items.append((prompt_num, desc, web_ctx))
            num_to_orig[prompt_num] = orig_idx

        user_message = _build_batch_user_message(prompt_items)

        # Call LLM
        raw_result = self._call_llm(user_message)
        if not raw_result:
            return {}

        # Parse results and validate
        classified = {}
        for key, value in raw_result.items():
            try:
                prompt_num = int(key)
            except (ValueError, TypeError):
                continue

            orig_idx = num_to_orig.get(prompt_num)
            if orig_idx is None:
                continue

            if not isinstance(value, dict):
                continue

            code = str(value.get('code', '')).replace('.', '').replace(' ', '')
            if len(code) != 8 or not code.isdigit():
                logger.warning(
                    f"[BATCH] Invalid code '{code}' for item {prompt_num}, skipping"
                )
                continue

            # Validate against CET
            code = validate_and_correct_code(code, self.base_dir)

            confidence = value.get('confidence', 0.75)
            if isinstance(confidence, str):
                try:
                    confidence = float(confidence)
                except ValueError:
                    confidence = 0.75

            # Reject low-confidence
            if confidence < 0.4:
                logger.warning(
                    f"[BATCH] Rejecting low-confidence ({confidence}) "
                    f"for item {prompt_num}"
                )
                continue

            classification = {
                'code': code,
                'category': value.get('category', 'LLM_CLASSIFIED'),
                'confidence': confidence,
                'source': 'batch_llm_classification',
                'notes': value.get('reasoning', 'Classified by batch LLM'),
            }
            classified[orig_idx] = classification

            # Cache result for future runs
            desc = dict(batch).get(orig_idx, "")
            if desc:
                search_terms = _extract_search_terms(desc)
                if search_terms:
                    _save_to_cache(search_terms, desc, classification, self.base_dir)

        return classified

    def _call_llm(self, user_message: str) -> dict | None:
        """
        Call the LLM API with the batch classification prompt.

        Tries core.llm_client first (shared singleton with caching),
        falls back to direct urllib call.
        """
        # Try the shared LLM client first (has caching, retry, etc.)
        try:
            from core.llm_client import get_llm_client
            llm = get_llm_client()
            result = llm.call_json(
                user_message=user_message,
                system_prompt=_BATCH_SYSTEM_PROMPT,
                max_tokens=8192,  # Larger for batch responses
                use_cache=True,
                cache_key_extra="batch_classify_v1",
            )
            if result and isinstance(result, dict):
                return result
        except Exception as e:
            logger.debug(f"LLM client call failed, trying urllib fallback: {e}")

        # Fallback: direct urllib call
        return self._call_llm_urllib(user_message)

    def _call_llm_urllib(self, user_message: str) -> dict | None:
        """Direct urllib LLM call (fallback when core.llm_client unavailable)."""
        try:
            from urllib.request import Request, urlopen
        except ImportError:
            logger.warning("urllib not available for batch LLM call")
            return None

        llm_settings = _load_llm_settings(self.base_dir)
        api_key = llm_settings.get('api_key', '')
        base_url = llm_settings.get('base_url', 'https://api.z.ai/api/anthropic')
        model = llm_settings.get('model', 'glm-5')  # SSOT: src/autoinvoice/domain/models/settings.py

        if not api_key:
            logger.warning("No API key for batch LLM classification")
            return None

        payload = {
            'model': model,
            'max_tokens': 8192,
            'temperature': 0,
            'system': _BATCH_SYSTEM_PROMPT,
            'messages': [{'role': 'user', 'content': user_message}],
        }

        api_endpoint = f"{base_url.rstrip('/')}/v1/messages"
        body = json.dumps(payload).encode('utf-8')

        req = Request(
            api_endpoint,
            data=body,
            headers={
                'Content-Type': 'application/json',
                'x-api-key': api_key,
                'anthropic-version': '2023-06-01',
            },
        )

        try:
            with urlopen(req, timeout=120) as response:
                data = json.loads(response.read().decode('utf-8'))

            text = data.get('content', [{}])[0].get('text', '')
            if not text:
                return None

            # Extract JSON from response
            json_start = text.find('{')
            json_end = text.rfind('}') + 1
            if json_start >= 0 and json_end > json_start:
                return json.loads(text[json_start:json_end])

        except Exception as e:
            logger.warning(f"Batch LLM urllib call failed: {e}")

        return None

    @staticmethod
    def _get_description(item: dict) -> str:
        """Extract the best description from an item dict."""
        return (
            item.get('description', '')
            or item.get('po_item_desc', '')
            or item.get('supplier_item_desc', '')
        )

    @staticmethod
    def _unknown_result() -> dict:
        """Return a standard UNKNOWN classification result."""
        return {
            'code': 'UNKNOWN',
            'category': 'UNCLASSIFIED',
            'confidence': 0,
            'source': 'none',
            'notes': 'Could not classify',
        }


# ── Convenience function matching classify_with_llm() signature ─────────────

def classify_items_batch(
    items: list[dict],
    rules: list[dict] = None,
    noise_words: set = None,
    config: dict = None,
    batch_size: int = None,
    gather_web_context: bool = True,
) -> list[dict]:
    """
    Convenience function: classify a list of items using the batch strategy.

    This is the main entry point for batch classification — a drop-in
    replacement for calling classify_with_llm() in a loop.

    Args:
        items: List of item dicts with 'description' (or 'po_item_desc'/'supplier_item_desc').
        rules: Classification rules list (from load_classification_rules()).
        noise_words: Noise words set (from load_classification_rules()).
        config: Pipeline config dict.
        batch_size: Items per LLM call (default BatchClassifier.DEFAULT_BATCH_SIZE = 60).
        gather_web_context: Whether to gather web search context first.

    Returns:
        List of classification result dicts (same length as items).
    """
    bc = BatchClassifier(config=config, batch_size=batch_size)
    return bc.classify_batch(
        items, rules=rules, noise_words=noise_words,
        gather_web_context=gather_web_context,
    )
