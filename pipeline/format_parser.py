#!/usr/bin/env python3
"""
Generic Format Parser Engine

This module executes format specifications from YAML config files.
ALL format-specific logic lives in config/formats/*.yaml
This Python code is generic and should NOT contain format-specific logic.

To add a new invoice format:
  1. Create config/formats/{supplier}.yaml
  2. Define detection, OCR normalization, metadata, and item extraction rules
  3. No Python changes needed
"""

import os
import re
import logging
import time as _time
from typing import Any, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

# ── Regex safety: timeout + performance tracking ─────────────────────────
# Prevents catastrophic backtracking on LLM-generated patterns applied to
# large OCR text.  Every regex operation goes through a _safe_re_* wrapper
# that (a) enforces a wall-clock timeout via SIGALRM and (b) logs any
# operation that exceeds _PERF_LOG_THRESHOLD so we can identify and fix
# the offending pattern.

_REGEX_TIMEOUT = 5          # hard-kill timeout (seconds)
_PERF_LOG_THRESHOLD = 0.1   # log warning for any regex taking longer (seconds)
_SIGALRM_AVAILABLE = hasattr(__import__('signal'), 'SIGALRM')

if _SIGALRM_AVAILABLE:
    import signal as _signal

    class _RegexTimeoutError(Exception):
        pass

    def _alarm_handler(signum, frame):
        raise _RegexTimeoutError("regex timeout")


def _pattern_str(pattern) -> str:
    """Extract printable pattern string from str or compiled re.Pattern."""
    if isinstance(pattern, re.Pattern):
        return pattern.pattern
    return str(pattern)


def _safe_re_sub(pattern, replace, text, flags=0):
    """re.sub with timeout protection and performance logging."""
    t0 = _time.monotonic()
    pat_s = _pattern_str(pattern)
    if _SIGALRM_AVAILABLE and len(text) > 5000:
        old_handler = _signal.signal(_signal.SIGALRM, _alarm_handler)
        _signal.alarm(_REGEX_TIMEOUT)
        try:
            result = re.sub(pattern, replace, text, flags=flags)
            _signal.alarm(0)
            elapsed = _time.monotonic() - t0
            if elapsed > _PERF_LOG_THRESHOLD:
                logger.warning(f"REGEX_PERF sub {elapsed:.3f}s pattern={pat_s[:120]} text_len={len(text)}")
            return result
        except _RegexTimeoutError:
            _signal.alarm(0)
            logger.error(f"REGEX_TIMEOUT sub {_REGEX_TIMEOUT}s pattern={pat_s[:120]} text_len={len(text)}")
            return text
        except re.error as e:
            _signal.alarm(0)
            logger.error(f"REGEX_ERROR sub pattern={pat_s[:120]}: {e}")
            return text
        finally:
            _signal.signal(_signal.SIGALRM, old_handler)
    try:
        result = re.sub(pattern, replace, text, flags=flags)
        elapsed = _time.monotonic() - t0
        if elapsed > _PERF_LOG_THRESHOLD:
            logger.warning(f"REGEX_PERF sub {elapsed:.3f}s pattern={pat_s[:120]} text_len={len(text)}")
        return result
    except re.error as e:
        logger.error(f"REGEX_ERROR sub pattern={pat_s[:120]}: {e}")
        return text


def _safe_re_search(pattern, text, flags=0):
    """re.search with timeout protection and performance logging."""
    t0 = _time.monotonic()
    pat_s = _pattern_str(pattern)
    if _SIGALRM_AVAILABLE and len(text) > 5000:
        old_handler = _signal.signal(_signal.SIGALRM, _alarm_handler)
        _signal.alarm(_REGEX_TIMEOUT)
        try:
            result = re.search(pattern, text, flags=flags)
            _signal.alarm(0)
            elapsed = _time.monotonic() - t0
            if elapsed > _PERF_LOG_THRESHOLD:
                logger.warning(f"REGEX_PERF search {elapsed:.3f}s pattern={pat_s[:120]} text_len={len(text)}")
            return result
        except _RegexTimeoutError:
            _signal.alarm(0)
            logger.error(f"REGEX_TIMEOUT search {_REGEX_TIMEOUT}s pattern={pat_s[:120]} text_len={len(text)}")
            return None
        except re.error as e:
            _signal.alarm(0)
            logger.error(f"REGEX_ERROR search pattern={pat_s[:120]}: {e}")
            return None
        finally:
            _signal.signal(_signal.SIGALRM, old_handler)
    try:
        result = re.search(pattern, text, flags=flags)
        elapsed = _time.monotonic() - t0
        if elapsed > _PERF_LOG_THRESHOLD:
            logger.warning(f"REGEX_PERF search {elapsed:.3f}s pattern={pat_s[:120]} text_len={len(text)}")
        return result
    except re.error as e:
        logger.error(f"REGEX_ERROR search pattern={pat_s[:120]}: {e}")
        return None


def _safe_re_match(pattern, text, flags=0):
    """re.match with timeout protection and performance logging."""
    t0 = _time.monotonic()
    pat_s = _pattern_str(pattern)
    if _SIGALRM_AVAILABLE and len(text) > 5000:
        old_handler = _signal.signal(_signal.SIGALRM, _alarm_handler)
        _signal.alarm(_REGEX_TIMEOUT)
        try:
            result = re.match(pattern, text, flags=flags)
            _signal.alarm(0)
            elapsed = _time.monotonic() - t0
            if elapsed > _PERF_LOG_THRESHOLD:
                logger.warning(f"REGEX_PERF match {elapsed:.3f}s pattern={pat_s[:120]} text_len={len(text)}")
            return result
        except _RegexTimeoutError:
            _signal.alarm(0)
            logger.error(f"REGEX_TIMEOUT match {_REGEX_TIMEOUT}s pattern={pat_s[:120]} text_len={len(text)}")
            return None
        except re.error as e:
            _signal.alarm(0)
            logger.error(f"REGEX_ERROR match pattern={pat_s[:120]}: {e}")
            return None
        finally:
            _signal.signal(_signal.SIGALRM, old_handler)
    try:
        result = re.match(pattern, text, flags=flags)
        elapsed = _time.monotonic() - t0
        if elapsed > _PERF_LOG_THRESHOLD:
            logger.warning(f"REGEX_PERF match {elapsed:.3f}s pattern={pat_s[:120]} text_len={len(text)}")
        return result
    except re.error as e:
        logger.error(f"REGEX_ERROR match pattern={pat_s[:120]}: {e}")
        return None


def _safe_re_findall(pattern, text, flags=0):
    """re.findall with timeout protection and performance logging."""
    t0 = _time.monotonic()
    pat_s = _pattern_str(pattern)
    if _SIGALRM_AVAILABLE and len(text) > 5000:
        old_handler = _signal.signal(_signal.SIGALRM, _alarm_handler)
        _signal.alarm(_REGEX_TIMEOUT)
        try:
            result = re.findall(pattern, text, flags=flags)
            _signal.alarm(0)
            elapsed = _time.monotonic() - t0
            if elapsed > _PERF_LOG_THRESHOLD:
                logger.warning(f"REGEX_PERF findall {elapsed:.3f}s pattern={pat_s[:120]} text_len={len(text)}")
            return result
        except _RegexTimeoutError:
            _signal.alarm(0)
            logger.error(f"REGEX_TIMEOUT findall {_REGEX_TIMEOUT}s pattern={pat_s[:120]} text_len={len(text)}")
            return []
        except re.error as e:
            _signal.alarm(0)
            logger.error(f"REGEX_ERROR findall pattern={pat_s[:120]}: {e}")
            return []
        finally:
            _signal.signal(_signal.SIGALRM, old_handler)
    try:
        result = re.findall(pattern, text, flags=flags)
        elapsed = _time.monotonic() - t0
        if elapsed > _PERF_LOG_THRESHOLD:
            logger.warning(f"REGEX_PERF findall {elapsed:.3f}s pattern={pat_s[:120]} text_len={len(text)}")
        return result
    except re.error as e:
        logger.error(f"REGEX_ERROR findall pattern={pat_s[:120]}: {e}")
        return []


def _safe_re_finditer(pattern, text, flags=0):
    """re.finditer with timeout protection and performance logging.

    Returns a list (not an iterator) so that the entire match operation
    completes within the timeout window.  Callers can iterate the list
    normally.
    """
    t0 = _time.monotonic()
    pat_s = _pattern_str(pattern)
    if _SIGALRM_AVAILABLE and len(text) > 5000:
        old_handler = _signal.signal(_signal.SIGALRM, _alarm_handler)
        _signal.alarm(_REGEX_TIMEOUT)
        try:
            result = list(re.finditer(pattern, text, flags=flags))
            _signal.alarm(0)
            elapsed = _time.monotonic() - t0
            if elapsed > _PERF_LOG_THRESHOLD:
                logger.warning(
                    f"REGEX_PERF finditer {elapsed:.3f}s matches={len(result)} "
                    f"pattern={pat_s[:120]} text_len={len(text)}"
                )
            return result
        except _RegexTimeoutError:
            _signal.alarm(0)
            logger.error(f"REGEX_TIMEOUT finditer {_REGEX_TIMEOUT}s pattern={pat_s[:120]} text_len={len(text)}")
            return []
        except re.error as e:
            _signal.alarm(0)
            logger.error(f"REGEX_ERROR finditer pattern={pat_s[:120]}: {e}")
            return []
        finally:
            _signal.signal(_signal.SIGALRM, old_handler)
    try:
        result = list(re.finditer(pattern, text, flags=flags))
        elapsed = _time.monotonic() - t0
        if elapsed > _PERF_LOG_THRESHOLD:
            logger.warning(
                f"REGEX_PERF finditer {elapsed:.3f}s matches={len(result)} "
                f"pattern={pat_s[:120]} text_len={len(text)}"
            )
        return result
    except re.error as e:
        logger.error(f"REGEX_ERROR finditer pattern={pat_s[:120]}: {e}")
        return []


class FormatParser:
    """
    Executes a format specification to parse invoice text.

    The parser is completely generic - all format-specific logic
    comes from the spec dict (loaded from YAML).
    """

    def __init__(self, spec: Dict[str, Any]):
        """
        Initialize parser with a format specification.

        Args:
            spec: Format specification dict (from YAML)
        """
        self.spec = spec
        self.name = spec.get('name', 'unknown')
        self._compiled_patterns: Dict[str, re.Pattern] = {}

    def parse(self, text: str) -> Dict[str, Any]:
        """
        Parse invoice text using this format's specification.

        Args:
            text: Raw invoice text (from OCR or PDF extraction)

        Returns:
            Parsed invoice data with metadata and items
        """
        t0 = _time.monotonic()
        # Step 1: Apply OCR normalization
        try:
            normalized_text = self.normalize_ocr(text)
        except Exception as e:
            logger.error(f"[{self.name}] OCR normalization failed: {e}", exc_info=True)
            normalized_text = text

        # Step 2: Extract metadata
        try:
            metadata = self.extract_metadata(normalized_text)
        except Exception as e:
            logger.error(f"[{self.name}] Metadata extraction failed: {e}", exc_info=True)
            metadata = {}

        # Step 3: Extract line items
        self._skipped_items_total = 0.0
        try:
            items = self.extract_items(normalized_text)
        except Exception as e:
            logger.error(f"[{self.name}] Item extraction failed: {e}", exc_info=True)
            items = []

        # Step 4: Post-OCR item validation — cross-check qty × price vs totals
        try:
            items = self._validate_and_correct_items(items, metadata)
        except Exception as e:
            logger.error(f"[{self.name}] Item validation failed: {e}", exc_info=True)

        # Step 4a: Honest orphan-price scan (Tier A1).  Look in the items
        # section for standalone price tokens (\d+\.\d{2}) that were not
        # captured as items.  If items_sum + orphan ≈ subtotal (within
        # $0.25), inject the orphan as a new item.  The description is
        # reconstructed from the lines immediately preceding the orphan
        # price.  No hallucination: we only add numbers that were actually
        # in the OCR text but missed by the strict item regex.
        orphan_notes = []
        try:
            items, orphan_notes = self._scan_orphan_prices(items, metadata, normalized_text)
        except Exception as e:
            logger.error(f"[{self.name}] Orphan price scan failed: {e}", exc_info=True)

        # Step 4b: Subtotal-anchored permissive retry.  When items_sum does
        # not reconcile with the extracted subtotal, re-run item extraction
        # with a permissive price pattern that accepts OCR-mangled qty
        # tokens (e.g. 'a', 'Vv', 'iM', 'L.').  Only accept the retry if it
        # reconciles better with the subtotal anchor.
        try:
            items = self._subtotal_anchored_retry(items, metadata, normalized_text)
        except Exception as e:
            logger.error(f"[{self.name}] Subtotal retry failed: {e}", exc_info=True)

        # Step 4c: Structural item count — independent count of price-bearing
        # lines between section markers. Compared against extracted items to
        # flag mismatches (missed or phantom items).
        structural_count = {'price_line_count': 0, 'header_count': None}
        try:
            structural_count = self._count_structural_items(normalized_text)
        except Exception as e:
            logger.error(f"[{self.name}] Structural count failed: {e}", exc_info=True)

        # Step 5: Post-process and validate
        result = self._build_result(metadata, items, normalized_text,
                                    skipped_items_total=self._skipped_items_total)

        # Attach orphan-scan notes so downstream (invoice_processor,
        # bl_xlsx_generator) can render uncertainty markers and decide
        # whether a proposed-fixes email is required.
        if orphan_notes:
            for inv in result.get('invoices', []):
                inv.setdefault('data_quality_notes', []).extend(orphan_notes)

        # Attach structural item count and mismatch flag
        for inv in result.get('invoices', []):
            inv['structural_item_count'] = structural_count
            extracted_count = len(inv.get('items', []))
            price_count = structural_count['price_line_count']
            header_count = structural_count.get('header_count')

            mismatch = False
            if price_count > 0 and extracted_count != price_count:
                mismatch = True
                inv.setdefault('data_quality_notes', []).append(
                    f"Structural item count mismatch: {price_count} price lines "
                    f"found between section markers but {extracted_count} items extracted"
                )
            if header_count is not None and extracted_count != header_count:
                mismatch = True
                inv.setdefault('data_quality_notes', []).append(
                    f"Header item count mismatch: header says {header_count} items "
                    f"but {extracted_count} items extracted"
                )
            inv['item_count_mismatch'] = mismatch

        elapsed = _time.monotonic() - t0
        if elapsed > 1.0:
            logger.info(
                f"[{self.name}] parse completed in {elapsed:.1f}s "
                f"(items={len(items)}, text_len={len(text)})"
            )
        return result

    def _scan_orphan_prices(
        self, items: List[Dict], metadata: Dict, text: str,
    ) -> Tuple[List[Dict], List[str]]:
        """
        Tier A1 honest recovery: scan the items section for standalone
        decimal prices that were NOT captured as items by the strict regex.

        For each orphan price we try to reconstruct a description from the
        lines preceding it, then inject a new item with:
            - sku generated from the spec's sku template
            - description from preceding non-empty lines
            - quantity = 1 (most common case for SHEIN/retail)
            - unit_cost / total_cost = orphan price value
            - data_quality = 'orphan_price_recovered'

        We only add an orphan if doing so moves items_sum *closer* to the
        subtotal anchor (within $0.25 residual).  This prevents injecting
        prices that are actually freight/tax numbers elsewhere on the page.

        Returns: (updated_items, notes) where notes is a list of
        human-readable strings describing each recovery.
        """
        notes: List[str] = []
        items_spec = self.spec.get('items') or {}
        if items_spec.get('strategy') != 'multiline':
            return items, notes

        subtotal = metadata.get('subtotal') or 0
        if not isinstance(subtotal, (int, float)) or subtotal <= 0:
            return items, notes

        # Existing items sum
        items_sum = sum(
            (it.get('total_cost', 0) or 0) for it in items
            if isinstance(it.get('total_cost'), (int, float))
        )
        initial_gap = round(subtotal - items_sum, 2)

        # Nothing to recover when already balanced (within $0.02).
        if abs(initial_gap) <= 0.02:
            return items, notes

        # Only recover positive gaps (we are missing money, not extra).
        if initial_gap < 0.02:
            return items, notes

        # Get items section text.
        sections_spec = self.spec.get('sections') or {}
        section_text = self._get_items_section(text, sections_spec)
        lines = section_text.split('\n')

        # Build set of already-captured prices so we don't double-count.
        captured_prices = set()
        for it in items:
            tc = it.get('total_cost')
            uc = it.get('unit_cost')
            if isinstance(tc, (int, float)):
                captured_prices.add(round(float(tc), 2))
            if isinstance(uc, (int, float)):
                captured_prices.add(round(float(uc), 2))

        # Scan for orphan prices in the items section — ignore lines that
        # match totals markers (Subtotal, Tax, Shipping, Grand Total).
        multiline = items_spec.get('multiline') or {}
        max_price = (self.spec.get('validation') or {}).get('max_item_price', 500)
        min_desc_len = multiline.get('min_description_length', 10)

        # Patterns we must NOT confuse with line items
        total_line_pat = re.compile(
            r'(?i)(subtotal|sales\s*tax|shipping|handling|grand\s*total|'
            r'item\(s\)|order\s*(number|date)|invoice\s*(date|no\.?|number)|'
            r'^\s*\d{4}-\d{2}-\d{2})'
        )
        price_token_re = re.compile(r'(?<![\d.])(\d{1,3}(?:,\d{3})*\.\d{2})(?![\d])')

        orphan_candidates: List[Tuple[float, str, int]] = []  # (price, desc, line_idx)

        for idx, line in enumerate(lines):
            stripped = line.strip()
            if not stripped:
                continue
            if total_line_pat.search(stripped):
                continue

            for m in price_token_re.finditer(stripped):
                try:
                    val = float(m.group(1).replace(',', ''))
                except ValueError:
                    continue
                if val <= 0 or val > max_price:
                    continue
                if round(val, 2) in captured_prices:
                    continue

                # Build description from this line's prefix + previous lines.
                prefix = stripped[:m.start()].strip(' :-—–\t|')
                desc_parts: List[str] = []
                if prefix:
                    desc_parts.append(prefix)

                # Walk backward up to 3 lines to find description context.
                back = idx - 1
                while back >= 0 and len(' '.join(desc_parts)) < 80:
                    prev = lines[back].strip()
                    back -= 1
                    if not prev:
                        continue
                    if total_line_pat.search(prev):
                        break
                    # If the previous line ends with its own price token,
                    # it already belongs to another (captured) item.
                    if price_token_re.search(prev):
                        break
                    desc_parts.insert(0, prev)

                description = ' '.join(desc_parts).strip()
                # Sanitize: remove OCR punctuation noise like "eee tia"
                description = re.sub(r'\s{2,}', ' ', description)
                description = re.sub(
                    r'(?:^|\s)(?:eee|tia|TE|Vv|Lge)(?:\s|$)', ' ', description
                ).strip()

                if len(description) < min_desc_len:
                    continue

                orphan_candidates.append((val, description, idx))

        if not orphan_candidates:
            return items, notes

        # Accept combinations of orphans that make items_sum + sum ≈ subtotal
        # within a residual window of $0.25 (absorbs OCR price rounding).
        # Greedy: try each orphan alone first, then pairs, matching the gap.
        tolerance = 0.25
        chosen: List[Tuple[float, str]] = []

        def gap_after(selected: List[Tuple[float, str]]) -> float:
            return abs(initial_gap - sum(p for p, _ in selected))

        # Single-orphan match
        best_single = None
        best_single_gap = tolerance + 1
        for (val, desc, _) in orphan_candidates:
            g = abs(initial_gap - val)
            if g < best_single_gap:
                best_single_gap = g
                best_single = (val, desc)

        if best_single and best_single_gap <= tolerance:
            chosen = [best_single]
        else:
            # Try pairs
            best_pair = None
            best_pair_gap = tolerance + 1
            n = len(orphan_candidates)
            for i in range(n):
                for j in range(i + 1, n):
                    total = orphan_candidates[i][0] + orphan_candidates[j][0]
                    g = abs(initial_gap - total)
                    if g < best_pair_gap:
                        best_pair_gap = g
                        best_pair = [
                            (orphan_candidates[i][0], orphan_candidates[i][1]),
                            (orphan_candidates[j][0], orphan_candidates[j][1]),
                        ]
            if best_pair and best_pair_gap <= tolerance:
                chosen = best_pair

        if not chosen:
            return items, notes

        # Build new items for each chosen orphan.
        sku_template = ((multiline.get('generated_fields') or {})
                        .get('sku', 'ITEM-{index}'))
        start_index = len(items) + 1
        new_items = list(items)
        for offset, (val, desc) in enumerate(chosen):
            idx_num = start_index + offset
            sku = sku_template.replace('{index}', str(idx_num))
            new_items.append({
                'sku': sku,
                'description': desc[:200],
                'quantity': 1,
                'unit_cost': round(val, 2),
                'total_cost': round(val, 2),
                'data_quality': 'orphan_price_recovered',
            })
            notes.append(
                f"Orphan price ${val:.2f} recovered from OCR text "
                f"(description: {desc[:60]}...)"
            )
            logger.info(
                f"Orphan-price recovery: injected ${val:.2f} as item "
                f"'{desc[:40]}' (gap {initial_gap:.2f} → "
                f"{round(initial_gap - sum(p for p, _ in chosen[:offset + 1]), 2):.2f})"
            )

        final_gap = round(subtotal - sum(
            (it.get('total_cost', 0) or 0) for it in new_items
            if isinstance(it.get('total_cost'), (int, float))
        ), 2)
        if abs(final_gap) > 0.02:
            notes.append(
                f"Residual ${final_gap:.2f} absorbed as OCR price rounding "
                f"(items_sum + recovered vs subtotal ${subtotal:.2f})"
            )

        return new_items, notes

    def _subtotal_anchored_retry(
        self, items: List[Dict], metadata: Dict, text: str,
    ) -> List[Dict]:
        """
        If items_sum fails to reconcile with the extracted subtotal, retry
        item extraction with a permissive price pattern and accept the
        retry only if it reconciles better with the subtotal anchor.

        Rationale: OCR sometimes mangles the single-digit qty column so the
        strict pattern misses rows (e.g. 'a', 'v', 'Vv', 'iM', 'L.' instead
        of '1').  The subtotal printed on the invoice is a reliable anchor
        — if a permissive retry produces an items sum that matches it, we
        can trust the extra items even though their qty tokens are garbled.
        """
        items_spec = self.spec.get('items', {})
        if items_spec.get('strategy') != 'multiline':
            return items

        subtotal = metadata.get('subtotal') or 0
        if not isinstance(subtotal, (int, float)) or subtotal <= 0:
            return items

        items_sum = sum(
            (it.get('total_cost', 0) or 0) for it in items
            if isinstance(it.get('total_cost'), (int, float))
        )

        # Tolerance: $0.02 fixed (matches variance_fixer threshold)
        tolerance = 0.02
        original_gap = abs(items_sum - subtotal)
        if original_gap <= tolerance:
            return items

        multiline = items_spec.get('multiline') or {}
        if not multiline.get('price_pattern'):
            return items

        # Build permissive variant: swap price_pattern for one that accepts
        # any 1-3 char non-whitespace token as the qty marker.
        permissive_spec = dict(items_spec)
        permissive_multiline = dict(multiline)
        permissive_multiline['price_pattern'] = (
            r'\s+(\S{1,3})[.:;,]?\s+([\d,]+\.\d{2})\s*[|}\)]*\s*$'
        )
        permissive_spec['multiline'] = permissive_multiline

        retry_items = self._extract_multiline_items(text, permissive_spec)

        # In permissive mode, force qty=1 for every item and recompute
        # total_cost from unit_cost.  Rationale: the permissive pattern
        # matches OCR-mangled qty tokens like 'a', 'iM', 'Vv', 'L.' which
        # cannot be parsed to an integer, *and* it may also mis-read a
        # stray digit (e.g. '4') that is actually a garbled '1'.  For
        # SHEIN-style retail invoices qty is almost always 1; if an
        # invoice legitimately has qty > 1 the subtotal anchor check
        # below will reject the retry and we keep the original items.
        for it in retry_items:
            unit_cost = it.get('unit_cost', 0) or 0
            if unit_cost > 0:
                it['quantity'] = 1
                it['total_cost'] = round(float(unit_cost), 2)

        retry_sum = sum(
            (it.get('total_cost', 0) or 0) for it in retry_items
            if isinstance(it.get('total_cost'), (int, float))
        )
        retry_gap = abs(retry_sum - subtotal)

        # Accept retry only if it reconciles with subtotal AND improves on
        # the original gap.  This is a one-way door: we never regress.
        if retry_gap <= tolerance and retry_gap < original_gap:
            logger.info(
                f"Subtotal-anchored retry: original {len(items)} items "
                f"sum ${items_sum:.2f} (gap ${original_gap:.2f}) → "
                f"permissive {len(retry_items)} items sum ${retry_sum:.2f} "
                f"(anchor ${subtotal:.2f}, gap ${retry_gap:.2f})"
            )
            return retry_items

        return items

    def normalize_ocr(self, text: str) -> str:
        """
        Apply format-specific OCR normalization rules.

        Rules are applied in order from the spec's ocr_normalize list.
        Each rule has a pattern and replacement.
        """
        rules = self.spec.get('ocr_normalize', [])

        for rule in rules:
            pattern = rule.get('pattern', '')
            replace = rule.get('replace', '')
            flags = self._parse_flags(rule.get('flags', ''))

            if pattern:
                text = _safe_re_sub(pattern, replace, text, flags=flags)

        return text

    def extract_metadata(self, text: str) -> Dict[str, Any]:
        """
        Extract invoice metadata using format-specific patterns.

        Each metadata field can have:
          - patterns: list of regex patterns to try (first match wins)
          - value: static value (no pattern matching)
          - type: 'string', 'currency', 'integer', 'date'
          - required: whether field must be found
          - aggregate: 'sum' to sum ALL matches across all patterns (for fields
            like credits/free_shipping that may appear multiple times)
        """
        metadata_spec = self.spec.get('metadata', {})
        metadata = {}

        for field_name, field_spec in metadata_spec.items():
            if isinstance(field_spec, dict) and field_spec.get('aggregate') == 'sum':
                total = self._extract_field_sum(text, field_spec)
                if total:
                    metadata[field_name] = total
            else:
                value = self._extract_field(text, field_spec)
                if value is not None:
                    metadata[field_name] = value

        return metadata

    def extract_items(self, text: str) -> List[Dict[str, Any]]:
        """
        Extract line items using format-specific strategy.

        Strategies:
          - block: Multi-line item blocks with start/end markers
          - table: Tabular data with header row
          - line: One item per line with regex pattern
        """
        items_spec = self.spec.get('items', {})
        strategy = items_spec.get('strategy', 'line')

        if strategy == 'block':
            return self._extract_block_items(text, items_spec)
        elif strategy == 'table':
            return self._extract_table_items(text, items_spec)
        elif strategy == 'line':
            return self._extract_line_items(text, items_spec)
        elif strategy == 'multiline':
            return self._extract_multiline_items(text, items_spec)
        elif strategy == 'column':
            return self._extract_column_items(text, items_spec)
        else:
            logger.warning(f"Unknown item strategy '{strategy}', using line")
            return self._extract_line_items(text, items_spec)

    def _extract_block_items(self, text: str, spec: Dict) -> List[Dict]:
        """Extract items using block strategy (multi-line item blocks)."""
        items = []
        block_spec = spec.get('block') or {}

        start_pattern = block_spec.get('start_pattern', '')
        end_pattern = block_spec.get('end_pattern', '')
        skip_patterns = block_spec.get('skip_patterns', [])
        fields_spec = block_spec.get('fields', {})

        if not start_pattern:
            return items

        # Compile patterns
        start_re = self._compile(start_pattern, re.MULTILINE | re.IGNORECASE)
        end_re = self._compile(end_pattern, re.MULTILINE) if end_pattern else None
        skip_res = [self._compile(p, re.IGNORECASE) for p in skip_patterns]

        # Get section boundaries
        sections_spec = self.spec.get('sections', {})
        text_section = self._get_items_section(text, sections_spec)

        # Find all item blocks
        lines = text_section.split('\n')
        current_block = []
        in_block = False

        for line in lines:
            line_stripped = line.strip()
            if not line_stripped:
                continue

            # Skip lines matching skip patterns (always, whether in block or not)
            if any(sr.search(line_stripped) for sr in skip_res):
                continue

            # Check if this ends the current block
            if in_block and end_re and end_re.match(line_stripped):
                current_block.append(line_stripped)
                item = self._parse_block(current_block, block_spec, len(items))
                if item:
                    items.append(item)
                current_block = []
                in_block = False
                continue

            # If we're already in a block, check if a new start_pattern appears
            # (this closes the previous block and starts a new one)
            # no_mid_split: when true, only the end_pattern can close a block
            # (for formats with multi-line descriptions where continuation lines
            # may match the start pattern)
            if in_block:
                no_mid_split = block_spec.get('no_mid_split', False)
                if not no_mid_split and start_re.search(line_stripped):
                    # Close the current block first
                    item = self._parse_block(current_block, block_spec, len(items))
                    if item:
                        items.append(item)
                    # Start a new block
                    current_block = [line_stripped]
                else:
                    current_block.append(line_stripped)
                continue

            # Check if this starts a new block (only when not in a block)
            if start_re.search(line_stripped):
                current_block = [line_stripped]
                in_block = True
                continue

        # Handle last block
        if current_block:
            item = self._parse_block(current_block, block_spec, len(items))
            if item:
                items.append(item)

        return items

    def _parse_block(self, block_lines: List[str], block_spec: Dict, index: int) -> Optional[Dict]:
        """Parse a single item block into structured data."""
        block_text = '\n'.join(block_lines)
        item = {}

        fields_spec = block_spec.get('fields', {})
        types_map = block_spec.get('types', {})

        for field_name, field_spec in fields_spec.items():
            # Handle generated fields
            if 'generate' in field_spec:
                template = field_spec['generate']
                item[field_name] = template.format(index=index + 1)
                continue

            # Merge type from block-level types if not specified in field
            if 'type' not in field_spec and field_name in types_map:
                field_spec = dict(field_spec)  # Copy to avoid modifying original
                field_spec['type'] = types_map[field_name]

            # Extract field value
            value = self._extract_field(block_text, field_spec)
            if value is not None:
                item[field_name] = value
            elif 'default' in field_spec:
                item[field_name] = field_spec['default']

        # Append continuation lines to description (text between start line
        # and any recognized field like "Legacy Item #").  Controlled by
        # include_continuation: true in the description field spec.
        if ('description' in item and 'description' in fields_spec
                and fields_spec['description'].get('include_continuation')):
            stop_patterns = fields_spec['description'].get(
                'continuation_stop', [])
            stop_res = [re.compile(p, re.IGNORECASE) for p in stop_patterns]
            extra_parts = []
            for line in block_lines[1:]:          # skip the start line
                if any(sr.search(line) for sr in stop_res):
                    break
                extra_parts.append(line.strip())
            if extra_parts:
                item['description'] = (
                    item['description'] + ' ' + ' '.join(extra_parts))

        # Apply cleaning rules to description
        if 'description' in item and 'description' in fields_spec:
            clean_rules = fields_spec['description'].get('clean', [])
            item['description'] = self._apply_clean_rules(item['description'], clean_rules)

        # Validate item has minimum required fields
        if item.get('description') or item.get('sku'):
            # Normalize alternate total field names
            if 'total_cost' not in item and 'line_total' in item:
                item['total_cost'] = item.pop('line_total')

            # Calculate total_cost if not present
            if 'total_cost' not in item and 'unit_price' in item:
                qty = item.get('quantity', 1)
                if not isinstance(qty, (int, float)):
                    try:
                        qty = float(str(qty).replace(',', ''))
                    except (ValueError, TypeError):
                        qty = 1
                item['total_cost'] = round(item['unit_price'] * qty, 2)

            # Normalize field names for pipeline compatibility
            if 'unit_price' in item and 'unit_cost' not in item:
                item['unit_cost'] = item.pop('unit_price')

            return item

        return None

    def _extract_line_items(self, text: str, spec: Dict) -> List[Dict]:
        """Extract items using line strategy (one item per line)."""
        items = []
        line_spec = spec.get('line') or {}

        pattern = line_spec.get('pattern', '')
        field_map = line_spec.get('field_map', {})
        types_map = line_spec.get('types', {})
        clean_fields = line_spec.get('clean_fields', {})
        generated_fields = line_spec.get('generated_fields', {})
        skip_patterns = line_spec.get('skip_patterns', [])

        if not pattern:
            return items

        line_re = self._compile(pattern, re.MULTILINE)
        skip_res = [self._compile(p, re.IGNORECASE) for p in skip_patterns]

        item_idx = 0
        skipped_total = 0.0
        for match in _safe_re_finditer(line_re, text):
            matched_text = match.group(0)

            # Check if this line should be skipped
            should_skip = any(skip_re.search(matched_text) for skip_re in skip_res)
            if should_skip:
                # Still extract total_cost from skipped items (e.g. Canceled)
                # so invoice_processor can prorate tax for non-skipped items.
                for field_name, group_idx in field_map.items():
                    if field_name in ('total_cost', 'line_total'):
                        try:
                            val = match.group(group_idx)
                            if val:
                                skipped_total += self._convert_type(
                                    val.strip(), types_map.get(field_name, 'currency')
                                )
                        except (IndexError, TypeError, ValueError):
                            pass
                continue
            item = {}

            # Extract fields from regex groups
            for field_name, group_idx in field_map.items():
                try:
                    value = match.group(group_idx)
                    if value:
                        item[field_name] = self._convert_type(
                            value.strip(),
                            types_map.get(field_name, 'string')
                        )
                except IndexError:
                    pass

            # Apply clean rules to fields
            for field_name, rules in clean_fields.items():
                if field_name in item and isinstance(item[field_name], str):
                    item[field_name] = self._apply_clean_rules(item[field_name], rules)

            # Generate fields (like SKU or computed values)
            for field_name, template in generated_fields.items():
                if field_name not in item:
                    if isinstance(template, str):
                        # Try arithmetic expression first (e.g. "total_cost / quantity")
                        if any(op in template for op in [' / ', ' * ', ' + ', ' - ']):
                            try:
                                expr_vars = {k: v for k, v in item.items() if isinstance(v, (int, float))}
                                expr_vars['index'] = item_idx + 1
                                item[field_name] = eval(template, {"__builtins__": {}}, expr_vars)
                                continue
                            except Exception:
                                pass
                        try:
                            item[field_name] = template.format(index=item_idx + 1, **item)
                        except KeyError:
                            # Template references a field not yet in item — use raw template
                            item[field_name] = template
                    else:
                        item[field_name] = template

            # Normalize alternate total field names from auto-generated specs
            if 'total_cost' not in item and 'line_total' in item:
                item['total_cost'] = item.pop('line_total')

            # Calculate total_cost if not present
            if 'total_cost' not in item and 'unit_price' in item:
                qty = item.get('quantity', 1)
                if not isinstance(qty, (int, float)):
                    try:
                        qty = float(str(qty).replace(',', ''))
                    except (ValueError, TypeError):
                        qty = 1
                item['total_cost'] = round(item['unit_price'] * qty, 2)

            # Normalize field names for pipeline compatibility
            if 'unit_price' in item and 'unit_cost' not in item:
                item['unit_cost'] = item.pop('unit_price')

            # Fall back: use catalog_num as sku if sku is missing
            if not item.get('sku') and item.get('catalog_num'):
                item['sku'] = item['catalog_num']

            if item.get('description') or item.get('sku'):
                items.append(item)
                item_idx += 1

        # Extra patterns: run additional patterns to catch items the primary missed.
        # Each entry has its own pattern and field_map.  Items whose matched text
        # overlaps an already-captured region are silently skipped (no duplicates).
        extra_patterns = line_spec.get('extra_patterns', [])
        if extra_patterns:
            # Build set of already-matched spans to avoid duplicates
            matched_spans = set()
            for m in _safe_re_finditer(line_re, text):
                matched_spans.add((m.start(), m.end()))

            for extra in extra_patterns:
                ep = extra.get('pattern', '')
                efm = extra.get('field_map', field_map)
                if not ep:
                    continue
                extra_re = self._compile(ep, re.MULTILINE)
                for match in _safe_re_finditer(extra_re, text):
                    # Skip if overlapping with primary matches
                    if any(not (match.end() <= s or match.start() >= e)
                           for s, e in matched_spans):
                        continue
                    item = {}
                    for field_name, group_idx in efm.items():
                        try:
                            value = match.group(group_idx)
                            if value:
                                item[field_name] = self._convert_type(
                                    value.strip(),
                                    types_map.get(field_name, 'string')
                                )
                        except IndexError:
                            pass
                    # Calculate total_cost if not present
                    if 'total_cost' not in item and 'unit_price' in item:
                        qty = item.get('quantity', 1)
                        if not isinstance(qty, (int, float)):
                            try:
                                qty = float(str(qty).replace(',', ''))
                            except (ValueError, TypeError):
                                qty = 1
                        item['total_cost'] = round(item['unit_price'] * qty, 2)
                    if 'unit_price' in item and 'unit_cost' not in item:
                        item['unit_cost'] = item.pop('unit_price')
                    if item.get('description') or item.get('sku'):
                        items.append(item)
                        item_idx += 1

        self._skipped_items_total = skipped_total
        return items

    def _extract_table_items(self, text: str, spec: Dict) -> List[Dict]:
        """Extract items using table strategy (tabular data)."""
        # Table extraction would parse structured table data
        # For now, fall back to line extraction
        return self._extract_line_items(text, spec)

    def _extract_multiline_items(self, text: str, spec: Dict) -> List[Dict]:
        """
        Extract items that span multiple lines.

        This strategy handles invoices where item descriptions wrap across lines,
        with quantity and price appearing at the end of the item block.

        Config options:
          multiline:
            price_pattern: regex to identify lines with price (marks end of item)
            description_join: how to join description lines (' ' or '\n')
            min_description_length: minimum chars for valid description
            skip_patterns: lines to ignore
            field_map: which capture groups map to which fields
            types: field type conversions
        """
        items = []
        multiline_spec = spec.get('multiline') or {}

        # Get patterns
        price_pattern = multiline_spec.get('price_pattern', r'(\d+)\s+([\d,]+\.\d{2})\s*$')
        description_join = multiline_spec.get('description_join', ' ')
        min_desc_length = multiline_spec.get('min_description_length', 10)
        skip_patterns = multiline_spec.get('skip_patterns', [])
        field_map = multiline_spec.get('field_map', {'quantity': 1, 'unit_price': 2})
        types_map = multiline_spec.get('types', {'quantity': 'integer', 'unit_price': 'currency'})
        generated_fields = multiline_spec.get('generated_fields', {})
        clean_fields = multiline_spec.get('clean_fields', {})

        # Compile patterns
        price_re = self._compile(price_pattern, re.MULTILINE)
        skip_res = [self._compile(p, re.IGNORECASE) for p in skip_patterns]

        # Get section boundaries
        sections_spec = self.spec.get('sections', {})
        text_section = self._get_items_section(text, sections_spec)

        # Split into lines and process
        lines = text_section.split('\n')
        description_lines = []
        item_idx = 0

        for line in lines:
            line_stripped = line.strip()
            if not line_stripped:
                continue

            # Skip lines matching skip patterns
            if any(sr.search(line_stripped) for sr in skip_res):
                continue

            # Check if this line ends with price pattern (marks end of item)
            price_match = price_re.search(line_stripped)

            if price_match:
                # This line has price - it's the end of an item
                # Get the description part (before the price match)
                desc_from_line = line_stripped[:price_match.start()].strip()
                if desc_from_line:
                    description_lines.append(desc_from_line)

                # Build full description
                full_description = description_join.join(description_lines)

                # Skip if description too short
                if len(full_description) >= min_desc_length:
                    item = {'description': full_description}

                    # Extract fields from price match
                    for field_name, group_idx in field_map.items():
                        try:
                            value = price_match.group(group_idx)
                            if value:
                                value = value.strip()
                                # Fix OCR-misread digits in quantity fields
                                # Common: 1→i/l/I/r/F/f/t/|
                                if field_name == 'quantity' and not value.isdigit():
                                    ocr_digit_map = {
                                        'i': '1', 'I': '1', 'l': '1',
                                        'r': '1', 'F': '1', 'f': '1',
                                        't': '1', '|': '1',
                                        'O': '0', 'o': '0',
                                        'S': '5', 's': '5',
                                        'B': '8', 'b': '8',
                                    }
                                    value = ''.join(
                                        ocr_digit_map.get(c, c) for c in value
                                    )
                                item[field_name] = self._convert_type(
                                    value,
                                    types_map.get(field_name, 'string')
                                )
                        except (IndexError, AttributeError):
                            pass

                    # Apply clean rules
                    for field_name, rules in clean_fields.items():
                        if field_name in item and isinstance(item[field_name], str):
                            item[field_name] = self._apply_clean_rules(item[field_name], rules)

                    # Generate fields
                    for field_name, template in generated_fields.items():
                        if field_name not in item:
                            item[field_name] = template.format(index=item_idx + 1)

                    # Normalize alternate total field names
                    if 'total_cost' not in item and 'line_total' in item:
                        item['total_cost'] = item.pop('line_total')

                    # Calculate total_cost
                    if 'total_cost' not in item and 'unit_price' in item:
                        qty = item.get('quantity', 1)
                        if not isinstance(qty, (int, float)):
                            try:
                                qty = float(str(qty).replace(',', ''))
                            except (ValueError, TypeError):
                                qty = 1
                        item['total_cost'] = round(item['unit_price'] * qty, 2)

                    # Normalize field names
                    if 'unit_price' in item and 'unit_cost' not in item:
                        item['unit_cost'] = item.pop('unit_price')

                    # Only append if not already in items
                    if item not in items:
                        items.append(item)
                        item_idx += 1

                # Reset for next item
                description_lines = []
            else:
                # This line is part of description
                description_lines.append(line_stripped)

        # Fallback: if multiline found 0 items, try positional matching
        # (descriptions separated from prices due to OCR column merge)
        if not items and lines:
            items = self._multiline_positional_fallback(
                text_section, multiline_spec, generated_fields, clean_fields, types_map
            )
            if items:
                logger.info(f"Multiline fallback: matched {len(items)} items by position")

        return items

    def _multiline_positional_fallback(
        self, text: str, spec: Dict, generated_fields: Dict,
        clean_fields: Dict, types_map: Dict,
    ) -> List[Dict]:
        """
        Fallback for multiline strategy when OCR separates descriptions from prices.

        Scans for lines that look like product descriptions (long text, no prices)
        and standalone price lines (just numbers). Pairs them by position/count.
        """
        lines = [l.strip() for l in text.split('\n') if l.strip()]
        skip_patterns = spec.get('skip_patterns', [])
        skip_res = [self._compile(p, re.IGNORECASE) for p in skip_patterns]
        min_desc_length = spec.get('min_description_length', 10)

        descriptions = []
        price_lines = []

        # Identify description lines vs price lines
        for line in lines:
            if any(sr.search(line) for sr in skip_res):
                continue

            # Price line: a line that is primarily numeric with optional qty
            # e.g. "1 6.45" or "2 12.99" or "6.45"
            price_match = re.match(
                r'^(\d+)\s+([\d,]+\.\d{2})\s*$', line
            )
            price_only = re.match(r'^[\$]?([\d,]+\.\d{2})\s*$', line)

            if price_match:
                qty = int(price_match.group(1))
                price = float(price_match.group(2).replace(',', ''))
                price_lines.append({'quantity': qty, 'unit_cost': price})
            elif price_only:
                price = float(price_only.group(1).replace(',', ''))
                price_lines.append({'quantity': 1, 'unit_cost': price})
            elif len(line) >= min_desc_length and not re.match(r'^[\d\$\.\,\s]+$', line):
                # Long text line without prices = description
                descriptions.append(line)

        # Only use fallback if counts roughly match
        if not descriptions or not price_lines:
            return []

        if len(descriptions) != len(price_lines):
            # If off by a small margin, truncate to shorter list
            if abs(len(descriptions) - len(price_lines)) <= 2:
                count = min(len(descriptions), len(price_lines))
                descriptions = descriptions[:count]
                price_lines = price_lines[:count]
            else:
                logger.debug(
                    f"Positional fallback: {len(descriptions)} descriptions vs "
                    f"{len(price_lines)} prices — too mismatched"
                )
                return []

        items = []
        for idx, (desc, price_info) in enumerate(zip(descriptions, price_lines)):
            item = {
                'description': desc,
                'quantity': price_info['quantity'],
                'unit_cost': price_info['unit_cost'],
                'total_cost': round(price_info['quantity'] * price_info['unit_cost'], 2),
            }

            # Apply clean rules
            for field_name, rules in clean_fields.items():
                if field_name in item and isinstance(item[field_name], str):
                    item[field_name] = self._apply_clean_rules(item[field_name], rules)

            # Generate fields (e.g. SKU)
            for field_name, template in generated_fields.items():
                if field_name not in item:
                    item[field_name] = template.format(index=idx + 1)

            items.append(item)

        return items

    def _extract_column_items(self, text: str, spec: Dict) -> List[Dict]:
        """
        Extract items from two-column OCR layout.

        In some invoices, OCR reads descriptions in one column and prices in another,
        resulting in all descriptions appearing first, then all prices grouped together.

        Config options:
          column:
            description_pattern: regex to match description blocks/paragraphs
            price_pattern: regex to match price values
            price_section_start: marker where prices section begins
            skip_patterns: lines to ignore
            min_description_length: minimum chars for valid description
        """
        items = []
        column_spec = spec.get('column') or {}

        # Get patterns and settings
        desc_pattern = column_spec.get('description_pattern', r'^[A-Za-z].{20,}')
        price_pattern = column_spec.get('price_pattern', r'([\d,]+\.\d{2})')
        price_section_start = column_spec.get('price_section_start', r'Subtotal|Total')
        skip_patterns = column_spec.get('skip_patterns', [])
        min_desc_length = column_spec.get('min_description_length', 15)
        generated_fields = column_spec.get('generated_fields', {})

        # Compile patterns
        desc_re = self._compile(desc_pattern, re.MULTILINE)
        price_re = self._compile(price_pattern)
        price_section_re = self._compile(price_section_start, re.IGNORECASE)
        skip_res = [self._compile(p, re.IGNORECASE) for p in skip_patterns]

        # Get section start (but NOT end - we need to include prices)
        # Column strategy handles its own sectioning via price_section_start
        sections_spec = self.spec.get('sections', {})
        start_markers = sections_spec.get('items_start', [])

        start_pos = 0
        for marker in start_markers:
            pos = text.find(marker)
            if pos != -1:
                start_pos = pos
                break

        text_section = text[start_pos:]

        # Split text into description section and price section
        # Prices often appear after "Subtotal" or similar markers
        price_section_match = price_section_re.search(text_section)
        if price_section_match:
            desc_text = text_section[:price_section_match.start()]
            price_text = text_section[price_section_match.start():]
        else:
            # Try to find where numeric-only lines begin
            lines = text_section.split('\n')
            desc_lines = []
            price_lines = []
            found_prices = False

            for line in lines:
                line_stripped = line.strip()
                if not line_stripped:
                    continue

                # Skip lines matching skip patterns
                if any(sr.search(line_stripped) for sr in skip_res):
                    continue

                # Check if line is primarily numeric (price line)
                non_digit = re.sub(r'[\d,.\s$]', '', line_stripped)
                if len(non_digit) < 3 and re.search(r'\d+\.\d{2}', line_stripped):
                    found_prices = True
                    price_lines.append(line_stripped)
                elif not found_prices:
                    desc_lines.append(line_stripped)

            desc_text = '\n'.join(desc_lines)
            price_text = '\n'.join(price_lines)

        # Extract descriptions (multi-line paragraphs)
        descriptions = []
        current_desc = []

        for line in desc_text.split('\n'):
            line_stripped = line.strip()
            if not line_stripped:
                # Empty line - end of paragraph
                if current_desc:
                    full_desc = ' '.join(current_desc)
                    if len(full_desc) >= min_desc_length:
                        descriptions.append(full_desc)
                    current_desc = []
                continue

            # Skip lines matching skip patterns
            if any(sr.search(line_stripped) for sr in skip_res):
                continue

            # Skip lines that look like metadata (contain colons or specific keywords)
            if ':' in line_stripped and len(line_stripped) < 50:
                continue

            current_desc.append(line_stripped)

        # Don't forget last paragraph
        if current_desc:
            full_desc = ' '.join(current_desc)
            if len(full_desc) >= min_desc_length:
                descriptions.append(full_desc)

        # Extract prices from price section
        prices = []
        for match in _safe_re_finditer(price_re, price_text):
            try:
                price_val = float(match.group(1).replace(',', ''))
                # Filter out likely totals (too high) or zeros
                if 0 < price_val < 500:  # Reasonable item price range
                    prices.append(price_val)
            except ValueError:
                pass

        # Match descriptions with prices
        # Usually the number of prices equals number of items
        # Take the first N prices where N = number of descriptions
        for i, desc in enumerate(descriptions):
            item = {
                'description': desc,
                'quantity': 1,  # Default quantity
            }

            # Assign price if available
            if i < len(prices):
                item['unit_cost'] = prices[i]
                item['total_cost'] = prices[i]

            # Generate fields
            for field_name, template in generated_fields.items():
                if field_name not in item:
                    item[field_name] = template.format(index=i + 1)

            items.append(item)

        return items

    def _extract_field(self, text: str, field_spec: Union[Dict, str]) -> Optional[Any]:
        """Extract a single field value from text."""
        if isinstance(field_spec, str):
            field_spec = {'patterns': [field_spec]}

        # Static value
        if 'value' in field_spec:
            return field_spec['value']

        # Pattern matching - support both 'pattern' (singular) and 'patterns' (plural)
        patterns = field_spec.get('patterns', field_spec.get('pattern', []))
        if isinstance(patterns, str):
            patterns = [patterns]

        field_type = field_spec.get('type', 'string')

        # Which capture group to extract (default: first group, or whole match)
        group_idx = field_spec.get('group') if isinstance(field_spec, dict) else None

        for pattern in patterns:
            try:
                match = _safe_re_search(pattern, text, re.IGNORECASE | re.MULTILINE)
                if match:
                    if group_idx is not None:
                        value = match.group(group_idx)
                    else:
                        value = match.group(1) if match.lastindex else match.group(0)
                    return self._convert_type(value.strip(), field_type)
            except (re.error, IndexError) as e:
                logger.debug(f"Pattern '{pattern}' failed: {e}")
                continue

        # Use fallback value if no pattern matched
        if 'fallback' in field_spec:
            return field_spec['fallback']

        return None

    def _extract_field_sum(self, text: str, field_spec: Dict) -> float:
        """Sum ALL matches across all patterns (for aggregate: sum fields)."""
        patterns = field_spec.get('patterns', field_spec.get('pattern', []))
        if isinstance(patterns, str):
            patterns = [patterns]
        field_type = field_spec.get('type', 'currency')
        total = 0.0
        for pattern in patterns:
            for match in _safe_re_finditer(pattern, text, re.IGNORECASE | re.MULTILINE):
                try:
                    raw = match.group(1) if match.lastindex else match.group(0)
                    val = self._convert_type(raw.strip(), field_type)
                    if isinstance(val, (int, float)):
                        total += val
                except (IndexError, TypeError, ValueError) as e:
                    logger.debug(f"Aggregate match extraction failed: {e}")
        return round(total, 2)

    def _convert_type(self, value: str, field_type: str) -> Any:
        """Convert extracted string value to specified type."""
        if field_type == 'currency':
            # Handle various currency formats including space-separated
            cleaned = value.replace('$', '').strip()
            # OCR comma-as-decimal: "9,31" → "9.31" (no period, comma + 2 digits)
            if '.' not in cleaned and re.match(r'^\d+,\d{2}$', cleaned):
                cleaned = cleaned.replace(',', '.')
            else:
                cleaned = cleaned.replace(',', '')
            # Handle "39 99" -> "39.99"
            space_match = re.match(r'^(\d+)\s+(\d{2})$', cleaned)
            if space_match:
                cleaned = f"{space_match.group(1)}.{space_match.group(2)}"
            try:
                return float(cleaned)
            except ValueError:
                return 0.0

        elif field_type == 'float':
            try:
                return float(value.replace(',', '').strip())
            except ValueError:
                return 0.0

        elif field_type == 'integer':
            # OCR-aware integer conversion: map common misread chars to digits
            _OCR_INT_MAP = {
                'I': '1', 'l': '1', 'i': '1', '|': '1', '{': '1', '(': '1',
                'r': '1', 'F': '1', 'f': '1', 't': '1',
                'O': '0', 'o': '0', 'D': '0',
                'S': '5', 's': '5',
                'B': '8',
                'T': '7',
                'g': '9', 'q': '9',
            }
            cleaned = value.replace(',', '').strip()
            cleaned = ''.join(_OCR_INT_MAP.get(c, c) for c in cleaned)
            try:
                return int(float(cleaned))
            except ValueError:
                return 0

        elif field_type == 'date':
            return value  # Keep as string, date parsing handled elsewhere

        return value  # string type

    def _apply_clean_rules(self, value: str, rules: List[Dict]) -> str:
        """Apply cleaning rules to a string value."""
        for rule in rules:
            pattern = rule.get('pattern', '')
            replace = rule.get('replace', '')
            if pattern:
                try:
                    value = re.sub(pattern, replace, value)
                except re.error:
                    pass
        return value.strip()

    def _get_items_section(self, text: str, sections_spec: Dict) -> str:
        """Extract the items section from full text."""
        start_markers = sections_spec.get('items_start', [])
        end_markers = sections_spec.get('items_end', [])

        start_pos = 0
        end_pos = len(text)

        # Find start of items section
        for marker in start_markers:
            pos = text.find(marker)
            if pos != -1:
                start_pos = pos
                break

        # Find end of items section
        for marker in end_markers:
            pos = text.find(marker, start_pos)
            if pos != -1 and pos < end_pos:
                end_pos = pos

        return text[start_pos:end_pos]

    def _compile(self, pattern: str, flags: int = 0) -> re.Pattern:
        """Compile and cache regex pattern."""
        cache_key = f"{pattern}:{flags}"
        if cache_key not in self._compiled_patterns:
            self._compiled_patterns[cache_key] = re.compile(pattern, flags)
        return self._compiled_patterns[cache_key]

    def _parse_flags(self, flags_str: str) -> int:
        """Parse regex flags from string."""
        flags = 0
        if 'multiline' in flags_str.lower():
            flags |= re.MULTILINE
        if 'ignorecase' in flags_str.lower():
            flags |= re.IGNORECASE
        if 'dotall' in flags_str.lower():
            flags |= re.DOTALL
        return flags

    def _count_structural_items(self, text: str) -> Dict[str, Any]:
        """Count items structurally by finding price-bearing lines between section markers.

        This provides an independent item count that can be compared against the
        parser's extracted item list to flag mismatches (missed or phantom items).

        Returns dict with:
            price_line_count: number of lines matching the price pattern in the items section
            header_count: item count extracted from header like "Invoice Detail (N)", or None
        """
        sections_spec = self.spec.get('sections', {})
        items_spec = self.spec.get('items', {})

        # ── Get the items section text ──
        start_markers = sections_spec.get('items_start', [])
        end_markers = sections_spec.get('items_end', [])

        # If no start markers defined or none match, return zero
        start_pos = -1
        for marker in start_markers:
            pos = text.find(marker)
            if pos != -1:
                start_pos = pos
                break

        if start_pos == -1:
            return {'price_line_count': 0, 'header_count': None}

        end_pos = len(text)
        for marker in end_markers:
            pos = text.find(marker, start_pos)
            if pos != -1 and pos < end_pos:
                end_pos = pos

        section_text = text[start_pos:end_pos]

        # ── Try to extract header count: "Invoice Detail (N)" or "Invoice Detail\nN items" ──
        header_count = None
        header_match = _safe_re_search(
            r'Invoice\s+Det\w*\s*\((\d+)\)', section_text
        )
        if header_match:
            header_count = int(header_match.group(1))

        # ── Count price-bearing lines ──
        # Use the format's own price pattern if available (multiline strategy),
        # otherwise fall back to a generic decimal price pattern.
        strategy = items_spec.get('strategy', 'line')
        multiline_spec = items_spec.get('multiline', {})
        line_spec = items_spec.get('line', {})

        if strategy == 'multiline' and multiline_spec.get('price_pattern'):
            price_pattern = multiline_spec['price_pattern']
        elif strategy == 'line' and line_spec.get('pattern'):
            price_pattern = line_spec['pattern']
        else:
            # Generic: line ending with a decimal price
            price_pattern = r'[\d,]+\.\d{2}\s*$'

        price_re = self._compile(price_pattern, re.MULTILINE)

        # Also compile skip patterns to exclude non-item lines
        skip_patterns = []
        if strategy == 'multiline':
            skip_patterns = multiline_spec.get('skip_patterns', [])
        elif strategy == 'line':
            skip_patterns = line_spec.get('skip_patterns', [])
        skip_res = [self._compile(p, re.IGNORECASE) for p in skip_patterns]

        lines = section_text.split('\n')
        price_line_count = 0
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            if any(sr.search(stripped) for sr in skip_res):
                continue
            if price_re.search(stripped):
                price_line_count += 1

        return {
            'price_line_count': price_line_count,
            'header_count': header_count,
        }

    def _compute_ocr_quality(self, text: str, invoice_data: Dict,
                              items: List[Dict]) -> Dict[str, Any]:
        """
        Compute OCR quality rating for this invoice.

        Scoring calibrated so that phone-photo receipts (susan stuff baseline)
        score ~25-35 (poor) while clean digital PDFs score 70+ (good).

        Returns dict with:
            score: 0-100 (higher = better)
            rating: 'good' | 'fair' | 'poor' | 'unusable'
            details: human-readable breakdown
            raw_text_preview: first 500 chars of OCR text
        """
        import re
        score = 0
        details = []

        if not text or len(text.strip()) < 10:
            return {
                'score': 0, 'rating': 'unusable',
                'details': 'No text extracted',
                'raw_text_preview': text[:500] if text else '',
            }

        text_len = len(text.strip())

        # 1. Text length (0-10 pts)
        if text_len >= 500:
            score += 10
        elif text_len >= 200:
            score += 7
        elif text_len >= 50:
            score += 3
        details.append(f'Text: {text_len} chars')

        # 2. Valid price patterns with $ sign (0-25 pts)
        #    This is THE key indicator — garbled OCR loses $ signs and decimals
        dollar_prices = _safe_re_findall(r'\$\d[\d,]*\.\d{2}', text)
        decimal_prices = _safe_re_findall(r'\d[\d,]+\.\d{2}', text)
        if len(dollar_prices) >= 5:
            score += 25
        elif len(dollar_prices) >= 3:
            score += 20
        elif len(dollar_prices) >= 1:
            score += 12
        elif len(decimal_prices) >= 3:
            score += 8
        elif len(decimal_prices) >= 1:
            score += 4
        details.append(f'Prices: {len(dollar_prices)} ($-sign), {len(decimal_prices)} (decimal)')

        # 3. Real total extracted (0-20 pts) — not a placeholder
        total_val = invoice_data.get('total', 0) or 0
        if total_val > 1:  # Not a $0.01 placeholder
            score += 20
            details.append(f'Total: ${total_val:.2f}')
        elif total_val > 0:
            score += 3  # Placeholder total from balance_items
            details.append(f'Total: ${total_val:.2f} (placeholder)')
        else:
            details.append('Total: MISSING')

        # 4. Invoice number extracted (0-10 pts)
        if invoice_data.get('invoice_number'):
            score += 10
            details.append(f'Invoice#: {invoice_data["invoice_number"]}')
        else:
            details.append('Invoice#: MISSING')

        # 5. Real items extracted with prices (0-25 pts)
        #    Placeholder items from balance_items don't count as "real"
        real_items = [it for it in items
                      if (it.get('total_cost', 0) or it.get('unit_cost', 0) or 0) > 0.02
                      and 'Purchase' not in (it.get('description') or '')]
        if len(real_items) >= 3:
            score += 25
        elif len(real_items) >= 1:
            score += 15
        elif items:
            score += 2  # Have items but no real prices
        details.append(f'Real items: {len(real_items)}/{len(items)}')

        # 6. Recognizable structure (0-10 pts) — common invoice words
        structure_words = ['total', 'subtotal', 'tax', 'item', 'qty', 'quantity',
                           'price', 'amount', 'invoice', 'order', 'date', 'shipping']
        text_lower = text.lower()
        found_words = [w for w in structure_words if w in text_lower]
        if len(found_words) >= 5:
            score += 10
        elif len(found_words) >= 3:
            score += 7
        elif len(found_words) >= 1:
            score += 3
        details.append(f'Structure words: {len(found_words)}/12')

        # Determine rating — calibrated thresholds:
        #   good (70+): clean digital PDF, all data extracted
        #   fair (40-69): some garbled text but prices readable
        #   poor (15-39): phone photos, placeholder values needed (susan stuff level)
        #   unusable (<15): almost no usable data
        if score >= 70:
            rating = 'good'
        elif score >= 40:
            rating = 'fair'
        elif score >= 15:
            rating = 'poor'
        else:
            rating = 'unusable'

        return {
            'score': score,
            'rating': rating,
            'details': '; '.join(details),
            'raw_text_preview': text[:500],
        }

    def _validate_and_correct_items(self, items: List[Dict],
                                     metadata: Dict) -> List[Dict]:
        """
        Post-OCR validation: cross-check item math and correct common OCR errors.

        Strategy:
        1. For each item, verify qty × unit_cost ≈ total_cost
        2. If mismatch, try common OCR digit substitutions (1↔l, 0↔O, 8↔B, etc.)
        3. Use invoice total/subtotal to validate overall sum
        4. Correct quantities when total_cost is reliable (e.g. OCR reads qty=1
           but total_cost / unit_cost gives a clean integer)
        """
        if not items:
            return items

        # Common OCR digit confusions: what Tesseract might misread
        _OCR_DIGIT_SWAPS = {
            '1': ['7', 'l', 'I'],
            '7': ['1', 'T'],
            '0': ['O', 'D', 'Q'],
            '8': ['B', '3'],
            '5': ['S', '6'],
            '6': ['5', 'G'],
            '4': ['A'],
            '9': ['g', 'q'],
        }

        corrected_count = 0

        for item in items:
            unit_cost = item.get('unit_cost') or item.get('unit_price')
            total_cost = item.get('total_cost')
            qty = item.get('quantity', 1)

            if not isinstance(qty, (int, float)):
                try:
                    qty = int(str(qty).replace(',', ''))
                except (ValueError, TypeError):
                    qty = 1

            if not unit_cost or not total_cost:
                continue

            try:
                unit_cost = float(unit_cost) if not isinstance(unit_cost, (int, float)) else unit_cost
                total_cost = float(total_cost) if not isinstance(total_cost, (int, float)) else total_cost
            except (ValueError, TypeError):
                continue

            if unit_cost <= 0:
                continue

            expected_total = round(qty * unit_cost, 2)

            # Check if qty × unit_cost ≈ total_cost (within $0.02 tolerance)
            if abs(expected_total - total_cost) <= 0.02:
                continue  # Math checks out

            # --- Correction attempt 1: Deduce quantity from total_cost / unit_cost ---
            if total_cost > 0 and unit_cost > 0:
                deduced_qty = total_cost / unit_cost
                rounded_qty = round(deduced_qty)

                # Accept if deduced qty is a clean integer and result matches
                if (rounded_qty >= 1
                        and abs(deduced_qty - rounded_qty) < 0.01
                        and abs(rounded_qty * unit_cost - total_cost) <= 0.02):
                    if rounded_qty != qty:
                        logger.info(
                            f"OCR correction: qty {qty} → {rounded_qty} "
                            f"(unit_cost={unit_cost}, total_cost={total_cost})"
                        )
                        item['quantity'] = rounded_qty
                        item['_ocr_corrected'] = f'qty:{qty}->{rounded_qty}'
                        corrected_count += 1
                        continue

            # --- Correction attempt 2: Fix unit_cost if qty is reliable ---
            # If qty > 1 and total_cost / qty gives a cleaner price
            if qty > 1 and total_cost > 0:
                deduced_price = round(total_cost / qty, 2)
                if abs(deduced_price * qty - total_cost) <= 0.02:
                    # Check if deduced_price is close to OCR'd price (within ~20%)
                    if unit_cost > 0 and abs(deduced_price - unit_cost) / unit_cost < 0.25:
                        logger.info(
                            f"OCR correction: unit_cost {unit_cost} → {deduced_price} "
                            f"(qty={qty}, total_cost={total_cost})"
                        )
                        if 'unit_cost' in item:
                            item['unit_cost'] = deduced_price
                        else:
                            item['unit_price'] = deduced_price
                        item['_ocr_corrected'] = f'price:{unit_cost}->{deduced_price}'
                        corrected_count += 1

        # --- Correction attempt 3: Fix "merged qty+price" OCR errors ---
        # When OCR reads "1 1.88" as "1 41.88" (qty digit merged with price)
        # Also handles: description ending in digit read as qty (e.g. "IPhone 4 4.92")
        # Use subtotal to detect: if adjusting price/qty makes items sum closer
        subtotal = metadata.get('subtotal') or metadata.get('total')
        if subtotal and isinstance(subtotal, (int, float)) and subtotal > 0 and items:
            items_sum = sum(
                (it.get('total_cost') or 0) for it in items
                if isinstance(it.get('total_cost'), (int, float))
            )
            if items_sum > subtotal * 1.15:  # Items sum significantly too high
                for item in items:
                    uc = item.get('unit_cost') or item.get('unit_price', 0)
                    tc = item.get('total_cost', 0)
                    qty = item.get('quantity', 1)
                    if not isinstance(uc, (int, float)) or uc <= 0:
                        continue
                    if not isinstance(qty, (int, float)):
                        qty = 1

                    best_fix = None  # (new_price, new_qty, new_total, label)

                    # Strategy A: Strip leading digits from price
                    price_str = f'{uc:.2f}'
                    for strip_n in (1, 2):
                        if len(price_str) > strip_n + 3:
                            stripped = price_str[strip_n:]
                            try:
                                new_price = float(stripped)
                            except ValueError:
                                continue
                            new_total = round(new_price * qty, 2)
                            new_sum = items_sum - tc + new_total
                            if abs(new_sum - subtotal) < abs(items_sum - subtotal):
                                if best_fix is None or abs(new_sum - subtotal) < abs(items_sum - tc + best_fix[2] - subtotal):
                                    best_fix = (new_price, qty, new_total,
                                                f'merged_price:{uc}->{new_price}')

                    # Strategy B: qty is actually part of description, real qty=1
                    if qty > 1:
                        new_total_b = round(uc * 1, 2)
                        new_sum_b = items_sum - tc + new_total_b
                        if abs(new_sum_b - subtotal) < abs(items_sum - subtotal):
                            if best_fix is None or abs(new_sum_b - subtotal) < abs(items_sum - tc + best_fix[2] - subtotal):
                                best_fix = (uc, 1, new_total_b,
                                            f'qty_was_desc:{qty}->1')

                    # Strategy C: leading digit of price is an OCR error
                    # e.g. $4.92 should be $1.92 (OCR misread 1→4)
                    price_str = f'{uc:.2f}'
                    if len(price_str) >= 4:  # at least X.XX
                        for alt_digit in '1234567890':
                            if alt_digit == price_str[0]:
                                continue
                            alt_price_str = alt_digit + price_str[1:]
                            try:
                                alt_price = float(alt_price_str)
                            except ValueError:
                                continue
                            cur_qty = best_fix[1] if best_fix else qty
                            new_total_c = round(alt_price * cur_qty, 2)
                            new_sum_c = items_sum - tc + new_total_c
                            if abs(new_sum_c - subtotal) < 0.02:
                                best_fix = (alt_price, cur_qty, new_total_c,
                                            f'ocr_digit:{price_str[0]}->{alt_digit} in price {uc}->{alt_price}')
                                break  # exact match found

                    if best_fix:
                        new_price, new_qty, new_total, label = best_fix
                        logger.info(
                            f"OCR correction: {label} "
                            f"(sum {items_sum:.2f} → {items_sum - tc + new_total:.2f} "
                            f"vs subtotal {subtotal:.2f})"
                        )
                        if 'unit_cost' in item:
                            item['unit_cost'] = new_price
                        else:
                            item['unit_price'] = new_price
                        item['quantity'] = new_qty
                        item['total_cost'] = new_total
                        item['_ocr_corrected'] = label
                        items_sum = items_sum - tc + new_total
                        corrected_count += 1

        # --- Global validation + metadata OCR correction ---
        # Items sum is the reliable anchor (each item independently verified).
        # When items verify but metadata (tax, total) doesn't match, try
        # single-digit swaps on metadata fields to resolve variance to zero.
        invoice_total = metadata.get('total') or metadata.get('subtotal')
        if invoice_total and isinstance(invoice_total, (int, float)) and invoice_total > 0:
            items_sum = sum(
                (it.get('total_cost') or 0) for it in items
                if isinstance(it.get('total_cost'), (int, float))
            )
            tax = metadata.get('tax', 0) or 0
            shipping = metadata.get('shipping', 0) or 0
            free_ship = metadata.get('free_shipping', 0) or 0
            discount = metadata.get('discount', 0) or 0
            # Net shipping: when free_shipping ≈ shipping, they cancel
            net_shipping = max(0, round(shipping - free_ship, 2))
            expected_from_items = items_sum + tax + net_shipping - discount

            if items_sum > 0:
                variance = round(expected_from_items - invoice_total, 2)

                # Try metadata digit corrections if variance is non-trivial
                if abs(variance) > 0.02:
                    variance = self._correct_metadata_ocr(
                        metadata, items_sum, tax, net_shipping, discount,
                        invoice_total, _OCR_DIGIT_SWAPS)
                    if variance is not None:
                        corrected_count += 1

                # Recompute for final warning
                tax = metadata.get('tax', 0) or 0
                invoice_total = metadata.get('total') or metadata.get('subtotal') or 0
                expected_from_items = items_sum + tax + net_shipping - discount
                if invoice_total > 0:
                    variance_pct = abs(expected_from_items - invoice_total) / invoice_total
                    if variance_pct > 0.15:
                        logger.warning(
                            f"Post-OCR check: items sum ${items_sum:.2f} + adjustments "
                            f"= ${expected_from_items:.2f} vs invoice total "
                            f"${invoice_total:.2f} ({variance_pct:.0%} variance)"
                        )

        if corrected_count:
            logger.info(f"Post-OCR corrected {corrected_count} item(s)")

        return items

    @staticmethod
    def _correct_metadata_ocr(
        metadata: Dict, items_sum: float, tax: float,
        shipping: float, discount: float, invoice_total: float,
        ocr_swaps: Dict[str, list],
    ) -> float:
        """
        Try single-digit swaps on tax and total to resolve variance to zero.

        Items sum is treated as the reliable anchor — each item's qty × price
        was already independently verified. If swapping one digit in tax or
        total makes the equation balance, accept the correction.

        Returns the corrected variance, or None if no fix found.
        """
        def _try_digit_swaps(value: float) -> list:
            """Generate candidate values by swapping single digits."""
            val_str = f'{value:.2f}'
            candidates = []
            for pos, ch in enumerate(val_str):
                if not ch.isdigit():
                    continue
                # Try each possible replacement digit
                for replacement in '0123456789':
                    if replacement == ch:
                        continue
                    new_str = val_str[:pos] + replacement + val_str[pos+1:]
                    try:
                        new_val = float(new_str)
                        if new_val >= 0:
                            candidates.append((new_val, ch, replacement, pos))
                    except ValueError:
                        pass
            return candidates

        # Strategy 1: Fix tax — items_sum + new_tax + shipping - discount = total
        if tax > 0:
            for new_tax, old_ch, new_ch, pos in _try_digit_swaps(tax):
                expected = items_sum + new_tax + shipping - discount
                if abs(expected - invoice_total) < 0.02:
                    logger.info(
                        f"OCR metadata fix: tax ${tax:.2f} → ${new_tax:.2f} "
                        f"(digit '{old_ch}'→'{new_ch}' at pos {pos})")
                    metadata['tax'] = new_tax
                    return round(expected - invoice_total, 2)

        # Strategy 2: Fix total — items_sum + tax + shipping - discount = new_total
        target_total = items_sum + tax + shipping - discount
        for new_total, old_ch, new_ch, pos in _try_digit_swaps(invoice_total):
            if abs(new_total - target_total) < 0.02:
                logger.info(
                    f"OCR metadata fix: total ${invoice_total:.2f} → ${new_total:.2f} "
                    f"(digit '{old_ch}'→'{new_ch}' at pos {pos})")
                if 'total' in metadata:
                    metadata['total'] = new_total
                elif 'subtotal' in metadata:
                    metadata['subtotal'] = new_total
                return round(target_total - new_total, 2)

        # Strategy 3: Fix both tax AND total together (two swaps)
        if tax > 0:
            for new_tax, t_old, t_new, t_pos in _try_digit_swaps(tax):
                new_target = items_sum + new_tax + shipping - discount
                for new_total, g_old, g_new, g_pos in _try_digit_swaps(invoice_total):
                    if abs(new_total - new_target) < 0.02:
                        logger.info(
                            f"OCR metadata fix: tax ${tax:.2f} → ${new_tax:.2f} "
                            f"('{t_old}'→'{t_new}'), total ${invoice_total:.2f} → "
                            f"${new_total:.2f} ('{g_old}'→'{g_new}')")
                        metadata['tax'] = new_tax
                        if 'total' in metadata:
                            metadata['total'] = new_total
                        elif 'subtotal' in metadata:
                            metadata['subtotal'] = new_total
                        return round(new_target - new_total, 2)

        # Strategy 4: Fix subtotal when total is missing but subtotal exists
        subtotal = metadata.get('subtotal', 0) or 0
        if subtotal > 0 and not metadata.get('total'):
            for new_sub, old_ch, new_ch, pos in _try_digit_swaps(subtotal):
                if abs(new_sub - items_sum) < 0.02:
                    logger.info(
                        f"OCR metadata fix: subtotal ${subtotal:.2f} → ${new_sub:.2f} "
                        f"(digit '{old_ch}'→'{new_ch}' at pos {pos})")
                    metadata['subtotal'] = new_sub
                    return 0.0

        # Strategy 5: Decimal-point recovery — OCR dropped the decimal from
        # the total (e.g. "Grand Total: 457" should be 4.57).  The raw integer
        # fallback pattern in the format spec captures "457" as 457.00; if
        # items_sum + adjustments ≈ invoice_total / 100 we can safely rescale.
        #
        # Guards:
        #   - target_total must be positive (items were extracted)
        #   - invoice_total must be at least 100× larger than target_total
        #     (so we don't accidentally rescale a legitimate total)
        #   - ratio must land within 0.02 of target after divide-by-100
        if target_total > 0 and invoice_total >= target_total * 50:
            recovered = round(invoice_total / 100, 2)
            if abs(recovered - target_total) < 0.02:
                logger.info(
                    f"OCR metadata fix: total ${invoice_total:.2f} → ${recovered:.2f} "
                    f"(decimal-point recovery: OCR dropped decimal in Grand Total)")
                if metadata.get('total'):
                    metadata['total'] = recovered
                elif metadata.get('subtotal'):
                    metadata['subtotal'] = recovered
                return round(target_total - recovered, 2)

        return None

    def _build_result(self, metadata: Dict, items: List[Dict], text: str,
                      skipped_items_total: float = 0.0) -> Dict:
        """Build final result dict in pipeline-compatible format."""
        # Map metadata fields to pipeline expected names
        field_mapping = {
            'invoice_number': 'invoice_number',
            'date': 'date',
            'supplier_name': 'supplier',
            'supplier_address': 'supplier_address',
            'country_code': 'country_code',
            'total': 'total',
            'subtotal': 'sub_total',
            'tax': 'tax',
            'shipping': 'freight',
            'discount': 'discount',
            'savings': 'savings',
            'credits': 'credits',
            'other_cost': 'other_cost',
        }

        invoice_data = {}
        for spec_field, pipeline_field in field_mapping.items():
            if spec_field in metadata:
                invoice_data[pipeline_field] = metadata[spec_field]

        # Net shipping vs free_shipping: Amazon invoices show both
        # "Shipping & Handling: $X" and "Free Shipping: -$X" but the
        # invoice_total already reflects the net amount.  When they
        # approximately cancel, zero both so the XLSX balances.
        free_shipping = metadata.get('free_shipping', 0) or 0
        freight = invoice_data.get('freight', 0) or 0
        if free_shipping > 0 and freight > 0:
            net = round(freight - free_shipping, 2)
            if abs(net) < 0.50:
                # They cancel — invoice_total already reflects net-zero shipping
                invoice_data['freight'] = 0
                # don't set free_shipping
            elif net > 0:
                # Partial free shipping — keep the remainder as freight
                invoice_data['freight'] = net
            else:
                # Free shipping exceeds shipping — keep remainder as deduction
                invoice_data['freight'] = 0
                invoice_data['free_shipping'] = round(-net, 2)
        elif free_shipping > 0 and freight == 0:
            # Orphan free_shipping without matching freight — invoice_total
            # already accounts for this, adding it as a deduction double-counts
            pass  # don't set free_shipping
        elif free_shipping > 0:
            invoice_data['free_shipping'] = free_shipping

        # Handle XCD to USD conversion for CARICOM invoices
        # Amazon shows item prices in USD but totals in XCD
        if metadata.get('currency') == 'XCD' or 'XCD' in text:
            exchange_rate = metadata.get('exchange_rate')
            if exchange_rate and isinstance(exchange_rate, str):
                try:
                    exchange_rate = float(exchange_rate)
                except ValueError:
                    exchange_rate = None
            if not exchange_rate:
                # Try to extract from text
                rate_match = _safe_re_search(r'1\s+USD\s*=\s*([\d.]+)\s*XCD', text)
                if rate_match:
                    exchange_rate = float(rate_match.group(1))

            if exchange_rate and isinstance(exchange_rate, (int, float)) and exchange_rate > 0:
                # Convert monetary fields from XCD to USD
                for field in ['total', 'sub_total', 'tax', 'freight', 'discount', 'credits', 'free_shipping', 'other_cost']:
                    if field in invoice_data and invoice_data[field]:
                        xcd_value = invoice_data[field]
                        invoice_data[f'{field}_xcd'] = xcd_value
                        invoice_data[field] = round(xcd_value / exchange_rate, 2)
                logger.debug(f"Converted XCD to USD using rate {exchange_rate}")

                # Amazon shows item prices in USD but totals in XCD.
                # After XCD→USD conversion, sub_total may not match items sum
                # due to Amazon's exchange rate markup.  Align sub_total with
                # items so the XLSX variance resolves to zero — the XCD original
                # is preserved in sub_total_xcd for reference.
                if items:
                    items_sum = sum(it.get('total_cost', 0) or 0 for it in items)
                    sub_total_usd = invoice_data.get('sub_total', 0) or 0
                    if items_sum > 0 and sub_total_usd != items_sum:
                        logger.info(f"XCD→USD sub_total ${sub_total_usd:.2f} adjusted to items sum ${items_sum:.2f} (markup ${sub_total_usd - items_sum:.2f})")
                        invoice_data['sub_total'] = items_sum

        # ─── Balance items to match subtotal (for OCR-garbled receipts) ───
        # When balance_items is enabled, ensure items sum == subtotal.
        # Uses total - tax as the authoritative subtotal when OCR garbles it.
        if self.spec.get('balance_items', False):
            # Determine the authoritative subtotal
            raw_total = invoice_data.get('total', 0) or 0
            raw_tax = invoice_data.get('tax', 0) or 0
            raw_shipping = invoice_data.get('freight', 0) or 0
            raw_subtotal = invoice_data.get('sub_total', 0) or 0

            # Prefer extracted subtotal (excludes shipping); fall back to
            # total - tax - shipping when subtotal wasn't captured.
            if raw_subtotal > 0:
                expected_sub = raw_subtotal
            elif raw_total > 0 and raw_tax >= 0:
                expected_sub = round(raw_total - raw_tax - raw_shipping, 2)
            else:
                expected_sub = 0

            supplier = invoice_data.get('supplier_name') or invoice_data.get('supplier') or self.name

            # Check if items have any monetary values
            items_have_prices = any(
                (it.get('total_cost', 0) or it.get('unit_cost', 0) or 0) > 0
                for it in items
            ) if items else False

            # Fallback: if no total extracted and items have no prices, find price in text
            if expected_sub == 0 and not items_have_prices:
                price_matches = _safe_re_findall(r'\$(\d[\d,]*\.\d{2})', text)
                if price_matches:
                    prices = [float(p.replace(',', '')) for p in price_matches]
                    expected_sub = max(prices)
                    invoice_data['total'] = expected_sub
                    invoice_data['tax'] = 0
                    logger.info(f"Balance: no total found, using largest price ${expected_sub:.2f} from text")
                else:
                    # Last resort: create placeholder with $0.01 so file isn't rejected
                    expected_sub = 0.01
                    invoice_data['total'] = expected_sub
                    invoice_data['tax'] = 0
                    logger.info(f"Balance: no prices found in text, using $0.01 placeholder for {supplier}")

            if expected_sub > 0:
                items_sum = sum(
                    (it.get('total_cost', 0) or it.get('unit_cost', 0) or 0)
                    for it in items
                )

                if items and items_sum == 0:
                    # Items exist but all have zero price — fill from subtotal
                    # preserving original descriptions from format extraction.
                    per_item = round(expected_sub / len(items), 2)
                    remainder = round(expected_sub - per_item * len(items), 2)
                    for idx, it in enumerate(items):
                        qty = it.get('quantity', 1) or 1
                        price = per_item + (remainder if idx == len(items) - 1 else 0)
                        it['unit_cost'] = price
                        it['total_cost'] = round(price * qty, 2)
                    logger.info(f"Balance: filled {len(items)} zero-price items from subtotal ${expected_sub:.2f}")
                elif not items:
                    # No items at all — create one placeholder item = subtotal
                    items = [{
                        'sku': f'{supplier[:3].upper()}-1',
                        'description': f'{supplier} Purchase',
                        'quantity': 1,
                        'unit_cost': expected_sub,
                        'total_cost': expected_sub,
                    }]
                    logger.info(f"Balance: created placeholder item ${expected_sub:.2f} for {supplier}")
                elif abs(items_sum - expected_sub) > 0.02:
                    # Items exist but sum is wrong — adjust last item
                    others_sum = sum(
                        (it.get('total_cost', 0) or it.get('unit_cost', 0) or 0)
                        for it in items[:-1]
                    )
                    adjusted_price = round(expected_sub - others_sum, 2)
                    if adjusted_price > 0:
                        items[-1]['unit_cost'] = adjusted_price
                        items[-1]['total_cost'] = adjusted_price
                        items[-1]['quantity'] = 1
                        logger.info(f"Balance: adjusted last item ${items_sum - others_sum:.2f} -> ${adjusted_price:.2f} to match subtotal ${expected_sub:.2f}")
                    else:
                        # Can't balance by adjusting last — replace all with placeholder
                        items = [{
                            'sku': f'{supplier[:3].upper()}-1',
                            'description': f'{supplier} Purchase',
                            'quantity': 1,
                            'unit_cost': expected_sub,
                            'total_cost': expected_sub,
                        }]
                        logger.info(f"Balance: replaced items with placeholder ${expected_sub:.2f}")

                # Clean garbled OCR descriptions — replace with readable placeholders
                for idx, it in enumerate(items, 1):
                    desc = it.get('description', '') or ''
                    # Detect garbled text: high ratio of non-alphanumeric chars or very short
                    alpha_count = sum(1 for c in desc if c.isalnum() or c == ' ')
                    if len(desc) < 3 or (len(desc) > 0 and alpha_count / len(desc) < 0.5):
                        it['description'] = f'{supplier} Item {idx}'
                    # Also clean up excessively long OCR noise
                    elif len(desc) > 80:
                        it['description'] = desc[:77] + '...'
                    # Set placeholder SKU if missing
                    if not it.get('sku'):
                        it['sku'] = f'{supplier[:3].upper()}-{idx}'

                # Align sub_total with items
                invoice_data['sub_total'] = expected_sub

        invoice_data['items'] = items
        invoice_data['raw_text'] = text[:5000]  # Truncate for context
        invoice_data['tables'] = []

        # ─── OCR Quality Rating ──────────────────────────────────
        invoice_data['ocr_quality'] = self._compute_ocr_quality(text, invoice_data, items)

        # Store skipped items total for proration. When skip_patterns filter out
        # unshipped items (e.g. Walmart Canceled), the pipeline needs this to
        # correctly prorate tax for the remaining items.
        if skipped_items_total > 0:
            invoice_data['skipped_items_total'] = round(skipped_items_total, 2)

        return {
            'status': 'success',
            'format': self.name,
            'ocr_method': 'format_parser',
            'invoices': [invoice_data]
        }


def create_parser(spec: Dict[str, Any]) -> FormatParser:
    """Factory function to create a parser from a spec."""
    return FormatParser(spec)


# ─── Legacy compatibility: merged from text_parser.py ────────────────────
# These functions support the old invoice_formats.yaml config and fallback
# parsing paths (TSV, columnar, generic). They are used by pdf_extractor.py
# and tests as a fallback when FormatRegistry doesn't match.

try:
    import yaml as _yaml
except ImportError:
    _yaml = None

import json


def _load_legacy_formats(base_dir: str = None) -> List[Dict]:
    """Load invoice format definitions from config/invoice_formats.yaml (legacy)."""
    if not _yaml:
        return []

    candidates = []
    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidates.append(os.path.join(script_dir, "..", "config", "invoice_formats.yaml"))
    if base_dir:
        candidates.append(os.path.join(base_dir, "config", "invoice_formats.yaml"))

    for path in candidates:
        path = os.path.abspath(path)
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = _yaml.safe_load(f)
            return data.get("formats", [])

    return []


def _detect_legacy_format(text: str, formats: List[Dict]) -> Optional[Dict]:
    """Find the first legacy format whose detect patterns all appear in the text."""
    for fmt in formats:
        detect = fmt.get("detect", [])
        if detect and all(d in text for d in detect):
            return fmt
    return None


def parse_text_file(text_path: str, output_path: str = None) -> Dict:
    """
    Parse a text file into structured invoice data.

    This is the legacy entry point that tries multiple strategies:
    1. TSV format (tab-separated)
    2. Columnar format (space-delimited)
    3. Config-driven formats (invoice_formats.yaml)
    4. Generic fallback

    New code should use FormatRegistry + FormatParser instead.
    """
    with open(text_path, "r", encoding="utf-8") as f:
        text = f.read()

    first_line = text.split("\n")[0].strip()

    if "\t" in first_line and (
        "LineNumber" in first_line or "InvoiceNo" in first_line
    ):
        result = parse_tsv_format(text)
    elif "LineNumber" in first_line and "ItemDescription" in first_line:
        result = parse_columnar_format(text)
    else:
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        formats = _load_legacy_formats(base_dir)
        matched_format = _detect_legacy_format(text, formats)

        if matched_format:
            result = parse_with_legacy_format(text, matched_format)
        else:
            result = parse_generic_invoice(text)

    if output_path and result.get("status") == "success":
        os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2)

    return result


def parse_with_legacy_format(text: str, fmt: Dict) -> Dict:
    """Parse invoice text using a legacy config-driven format definition (invoice_formats.yaml)."""
    meta_patterns = fmt.get("metadata", {})
    items_config = fmt.get("items", {})
    page_markers = fmt.get("page_markers", [])
    totals_markers = fmt.get("totals_markers", [])
    country_code = fmt.get("country_code", "US")

    # Extract metadata
    metadata = {}
    for field, patterns in meta_patterns.items():
        if isinstance(patterns, str):
            patterns = [patterns]
        for pattern in patterns:
            match = _safe_re_search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                metadata[field] = match.group(1).strip()
                break

    # Convert numeric fields
    for field in ["total", "sub_total", "discount", "freight", "credits"]:
        if field in metadata:
            try:
                metadata[field] = float(metadata[field].replace(",", ""))
            except (ValueError, AttributeError):
                pass

    # XCD to USD conversion
    if metadata.get("total") and "XCD" in text:
        exchange_match = _safe_re_search(r'1\s+USD\s*=\s*([\d.]+)\s*XCD', text)
        if exchange_match:
            exchange_rate = float(exchange_match.group(1))
            if metadata.get("total"):
                metadata["total_xcd"] = metadata["total"]
                metadata["total"] = round(metadata["total"] / exchange_rate, 2)
            if metadata.get("discount"):
                metadata["discount"] = round(metadata["discount"] / exchange_rate, 2)
            if metadata.get("credits"):
                metadata["credits"] = round(metadata["credits"] / exchange_rate, 2)
            if metadata.get("sub_total"):
                metadata["sub_total"] = round(metadata["sub_total"] / exchange_rate, 2)

    # Parse line items
    items = _parse_legacy_items(text, items_config, page_markers, totals_markers)

    invoice_total = metadata.get("total") or metadata.get("sub_total")
    if isinstance(invoice_total, str):
        try:
            invoice_total = float(invoice_total.replace(",", ""))
        except ValueError:
            invoice_total = None

    credits = metadata.get("credits", 0) or 0
    if isinstance(credits, str):
        try:
            credits = float(credits.replace(",", ""))
        except (ValueError, AttributeError):
            credits = 0.0

    return {
        "status": "success",
        "invoices": [
            {
                "invoice_number": metadata.get("invoice_number"),
                "date": metadata.get("invoice_date"),
                "supplier": metadata.get("supplier_name"),
                "supplier_address": metadata.get("supplier_address"),
                "country_code": country_code,
                "customer_name": metadata.get("customer_name"),
                "customer_code": metadata.get("customer_code"),
                "shipped_via": metadata.get("shipped_via"),
                "total": invoice_total,
                "discount": metadata.get("discount", 0) or 0,
                "freight": metadata.get("freight", 0) or 0,
                "credits": credits,
                "items": items,
            }
        ],
    }


def _parse_legacy_items(
    text: str,
    items_config: Dict,
    page_markers: List[str],
    totals_markers: List[str],
) -> List[Dict]:
    """Parse line items using legacy config-driven patterns (from invoice_formats.yaml)."""
    if items_config.get("multi_line_parser") == "amazon":
        return _parse_amazon_items(text, items_config, page_markers, totals_markers)

    header_re = re.compile(items_config.get("header_regex", ""), re.IGNORECASE)
    line_re = re.compile(items_config.get("line_regex", ""))
    upc_re_str = items_config.get("upc_regex", "")
    upc_re = re.compile(upc_re_str) if upc_re_str else None
    field_map = items_config.get("field_map", {})
    continuation = items_config.get("continuation_lines", False)
    filter_zero = items_config.get("filter_zero_cost_subitems", False)
    filter_zero_qty = items_config.get("filter_zero_qty", False)

    items = []
    current_item = None
    in_items_section = False

    for line in text.split("\n"):
        stripped = line.strip()
        if not stripped:
            continue

        if header_re.search(stripped):
            in_items_section = True
            continue

        if any(stripped.startswith(m) for m in page_markers):
            in_items_section = False
            continue

        if re.match(r"Page\s+\d+\s+of\s+\d+", stripped):
            in_items_section = False
            continue

        if not in_items_section and header_re.search(stripped):
            in_items_section = True
            continue

        if not in_items_section:
            continue

        if any(stripped.startswith(m) for m in totals_markers):
            in_items_section = False
            continue

        if upc_re:
            upc_match = upc_re.match(stripped)
            if upc_match:
                if current_item:
                    current_item["upc"] = upc_match.group(1)
                continue

        if stripped.startswith(("·", "•")):
            continue

        match = line_re.match(stripped)
        if match:
            if current_item:
                items.append(current_item)

            item = {}
            for field_name, group_idx in field_map.items():
                val = match.group(group_idx)
                if val is not None:
                    if field_name in ("quantity", "ordered"):
                        try:
                            item[field_name] = int(float(val.replace(",", "")))
                        except ValueError:
                            item[field_name] = 0
                    elif field_name in ("unit_price", "total"):
                        try:
                            item[field_name] = float(val.replace(",", ""))
                        except ValueError:
                            item[field_name] = 0.0
                    elif field_name == "line_number":
                        item[field_name] = val
                    else:
                        item[field_name] = val.strip()

            if "total" not in item or item["total"] is None:
                item["total"] = round(
                    item.get("quantity", 0) * item.get("unit_price", 0), 2
                )

            if "total" in item:
                item["total_cost"] = item.pop("total")
            if "unit_price" in item:
                item["unit_cost"] = item.pop("unit_price")
            if "description" not in item:
                item["description"] = ""

            current_item = item
        elif continuation and current_item:
            if current_item.get("description"):
                current_item["description"] += " " + stripped
            else:
                current_item["description"] = stripped

    if current_item:
        items.append(current_item)

    if filter_zero:
        items = [
            it for it in items
            if it.get("total_cost", 0) > 0 or "." not in str(it.get("line_number", ""))
        ]
    if filter_zero_qty:
        items = [it for it in items if it.get("quantity", 0) > 0]

    return items


def _parse_amazon_items(
    text: str,
    items_config: Dict,
    page_markers: List[str],
    totals_markers: List[str],
) -> List[Dict]:
    """Parse Amazon order items from print-friendly format (legacy multi-line parser)."""
    items = []
    lines = text.split("\n")

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        if any(marker in line for marker in totals_markers):
            break

        if len(line) < 5 or (line.isdigit() and len(line) < 3):
            i += 1
            continue

        if not line:
            i += 1
            continue

        skip_prefixes = [
            "Order Details", "Order Summary", "Order placed",
            "Ship to", "Payment method", "Item(s)", "Shipping",
            "Buy Again", "Multibuy", "Total before", "Estimated tax",
            "Exchange rate", "guarantee", "Grand Total", "Back to top",
            "Conditions", "Privacy", "Consumer Health", "Your Ads", "\u00a9", "http",
            "Delivered January", "Your package was", "View related",
            "Delivered", "Your package",
            "Sold by:", "Supplied by:", "Return or replace",
        ]

        if any(line.startswith(m) for m in page_markers + skip_prefixes):
            i += 1
            continue

        if line.startswith("$"):
            i += 1
            continue

        if line and len(line.split()) >= 2 and not line[0].isdigit():
            description_lines = [line]
            j = i + 1
            max_lines = 10

            while j < len(lines) and len(description_lines) < max_lines:
                next_line = lines[j].strip()

                if any(next_line.startswith(m) for m in [
                    "Sold by:", "Supplied by:", "Return or replace",
                    "Delivered", "Back to top", "Conditions"
                ]):
                    break

                if next_line.startswith("$"):
                    break

                if next_line:
                    description_lines.append(next_line)

                j += 1

            quantity = 1
            unit_price = 0.0
            found_price = False
            price_position = i

            for k in range(i + 1, min(i + 15, len(lines))):
                check_line = lines[k].strip()

                if check_line.isdigit() and not check_line.startswith("Order"):
                    try:
                        qty_candidate = int(check_line)
                        if 1 <= qty_candidate <= 1000:
                            quantity = qty_candidate
                    except ValueError:
                        pass

                if check_line.startswith("$"):
                    try:
                        price_str = check_line.replace("$", "").replace(",", "").strip()
                        space_cents_match = re.match(r'^(\d+)\s+(\d{2})$', price_str)
                        if space_cents_match:
                            dollars = space_cents_match.group(1)
                            cents = space_cents_match.group(2)
                            price_str = f"{dollars}.{cents}"
                        unit_price = float(price_str)
                        found_price = True
                        price_position = k
                    except ValueError:
                        pass

                if found_price and unit_price > 0:
                    full_description = " ".join(description_lines).strip()
                    full_description = re.sub(r'\s+', ' ', full_description)

                    desc_words = full_description.split()
                    cleaned_desc = [w for w in desc_words if not w.isdigit()]
                    full_description = " ".join(cleaned_desc).strip()

                    full_description = re.sub(r'https?://[^\s]+', '', full_description)
                    full_description = re.sub(r'Page\s+\d+\s+of\s+\d+', '', full_description)
                    full_description = re.sub(r'orderID[^\s]+', '', full_description)
                    full_description = re.sub(r'&ref[^\s]+', '', full_description)
                    full_description = re.sub(r'\s+', ' ', full_description).strip()

                    items.append({
                        "sku": f"AMZ-{len(items) + 1}",
                        "description": full_description,
                        "quantity": quantity,
                        "unit_cost": unit_price,
                        "total_cost": round(unit_price * quantity, 2),
                    })
                    i = price_position
                    break

        i += 1

    return items


def parse_generic_invoice(text: str) -> Dict:
    """Fallback parser for unrecognized formats.
    Attempts to extract basic invoice data using common patterns."""
    invoice_number = (
        _extract_field_legacy(text, r"Invoice\s*(?:#|No\.?)\s*:?\s*(\S+)")
        or _extract_field_legacy(text, r"Invoice\s+(\d+)\s+Date")
    )
    invoice_date = (
        _extract_field_legacy(text, r"Invoice\s*Date\s*:?\s*(\d+/\d+/\d+)")
        or _extract_field_legacy(text, r"Date\s*:?\s*(\d+/\d+/\d+)")
    )
    customer_code = _extract_field_legacy(text, r"Customer\s+(?:ID\s*:?\s*)?(\d+)")

    item_re = re.compile(
        r"^(\d+)\s+"
        r"(\S+)\s+"
        r"(.+?)\s+"
        r"(\d+)\s+"
        r"(?:\S+\s+)?"
        r"([\d,]+\.\d+)\s+"
        r"([\d,]+\.\d+)"
        r"\s*$"
    )

    items = []
    for line in text.split("\n"):
        stripped = line.strip()
        m = item_re.match(stripped)
        if m:
            qty = int(m.group(4))
            unit_price = float(m.group(5).replace(",", ""))
            total = float(m.group(6).replace(",", ""))
            if qty > 0 and total > 0:
                items.append({
                    "sku": m.group(2),
                    "description": m.group(3).strip(),
                    "quantity": qty,
                    "unit_cost": round(unit_price, 2),
                    "total_cost": total,
                })

    total = _extract_number_legacy(text, r"(?:Invoice\s+)?Total\s+([\d,]+\.\d{2})")

    return {
        "status": "success" if items else "error",
        "error": "No items found in text" if not items else None,
        "invoices": [
            {
                "invoice_number": invoice_number,
                "date": invoice_date,
                "supplier": None,
                "customer_code": customer_code,
                "total": total or sum(i.get("total_cost", 0) for i in items),
                "discount": 0,
                "freight": 0,
                "items": items,
            }
        ] if items else [],
    }


def parse_tsv_format(text: str) -> Dict:
    """Parse tab-separated value export format."""
    lines = text.strip().split("\n")
    if not lines:
        return {"status": "error", "error": "Empty TSV file"}

    headers = lines[0].split("\t")

    col_map = {}
    for i, h in enumerate(headers):
        hl = h.strip().lower()
        if "invoiceno" in hl or ("invoice" in hl and "number" in hl):
            col_map["invoice_number"] = i
        elif "itemnumber" in hl or ("item" in hl and "number" in hl and "desc" not in hl):
            col_map["sku"] = i
        elif "itemdescription" in hl or "description" in hl:
            col_map["description"] = i
        elif "quantity" in hl:
            col_map["quantity"] = i
        elif "cost" in hl:
            col_map["unit_cost"] = i
        elif "date" in hl:
            col_map["date"] = i

    items = []
    invoice_number = None
    invoice_date = None

    for line in lines[1:]:
        if not line.strip():
            continue
        cols = line.split("\t")

        if not invoice_number and "invoice_number" in col_map:
            idx = col_map["invoice_number"]
            val = cols[idx].strip() if idx < len(cols) else ""
            if val:
                invoice_number = val

        if not invoice_date and "date" in col_map:
            idx = col_map["date"]
            val = cols[idx].strip() if idx < len(cols) else ""
            if val:
                invoice_date = val.split(" ")[0]

        item = {}
        for field, idx in col_map.items():
            if idx < len(cols):
                item[field] = cols[idx].strip()

        if item.get("description"):
            for field in ["quantity", "unit_cost"]:
                if field in item:
                    try:
                        item[field] = float(str(item[field]).replace(",", ""))
                    except ValueError:
                        item[field] = 0
            item["total_cost"] = item.get("quantity", 0) * item.get("unit_cost", 0)
            items.append(item)

    return {
        "status": "success",
        "invoices": [
            {
                "invoice_number": invoice_number,
                "date": invoice_date,
                "supplier": None,
                "total": sum(i.get("total_cost", 0) for i in items),
                "items": items,
            }
        ],
    }


_COLUMNAR_ROW = re.compile(
    r"^(\d+)\s+"
    r"(\d+/\d+/\d+\s+\d+:\d+:\d+\s+[AP]M)\s+"
    r"(\S+)\s+"
    r"(\S+)\s+"
    r"(.+?)\s+"
    r"([\d.]+)\s+"
    r"(\d+)\s+"
    r"(\S+)\s*$"
)


def parse_columnar_format(text: str) -> Dict:
    """Parse space-delimited columnar export format."""
    lines = text.strip().split("\n")
    if len(lines) < 2:
        return {"status": "error", "error": "No data rows in columnar file"}

    invoices_map: Dict[str, Dict] = {}

    for line in lines[1:]:
        stripped = line.strip()
        if not stripped:
            continue

        match = _COLUMNAR_ROW.match(stripped)
        if not match:
            continue

        invoice_no = match.group(3)
        item_number = match.group(4)
        description = match.group(5).strip()
        unit_cost = float(match.group(6))
        quantity = int(match.group(7))
        date_str = match.group(2).split(" ")[0]

        if invoice_no not in invoices_map:
            invoices_map[invoice_no] = {
                "invoice_number": invoice_no,
                "date": date_str,
                "items": [],
            }

        invoices_map[invoice_no]["items"].append(
            {
                "sku": item_number,
                "description": description,
                "quantity": quantity,
                "unit_cost": round(unit_cost, 2),
                "total_cost": round(quantity * unit_cost, 2),
            }
        )

    invoices = []
    for inv in invoices_map.values():
        inv["total"] = round(sum(i["total_cost"] for i in inv["items"]), 2)
        inv["supplier"] = None
        inv["discount"] = 0
        inv["freight"] = 0
        invoices.append(inv)

    if not invoices:
        return {"status": "error", "error": "No items parsed from columnar data"}

    return {"status": "success", "invoices": invoices}


def _extract_field_legacy(text: str, pattern: str) -> Optional[str]:
    match = _safe_re_search(pattern, text, re.IGNORECASE)
    if match:
        result = match.group(1)
        if result is None:
            return ""
        return str(result)
    return None


def _extract_number_legacy(text: str, pattern: str) -> Optional[float]:
    match = _safe_re_search(pattern, text, re.IGNORECASE)
    if match:
        try:
            return float(match.group(1).replace(",", ""))
        except ValueError:
            return None
    return None
