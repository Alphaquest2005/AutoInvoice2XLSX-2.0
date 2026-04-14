#!/usr/bin/env python3
"""
Stage 3: Classifier
Rule engine + web lookup fallback for tariff code classification.
"""

import json
import os
import re
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ─── HARDCODED CET VALIDATION ────────────────────────────────────────────────
# CARICOM CET codes MUST be 8-digit end-nodes with DUTY RATE, UNIT, and SITC REV 4.
# Category headings (descriptions ending with ":") have NO duty rate and MUST be rejected.
# This is enforced at classification time — invalid codes are auto-corrected or rejected.

# Known category headings that MUST NOT be used as classification codes.
# These are parent codes (CET detail 00) with subcodes below them.
# Format: invalid_code -> correct_end_node_code
KNOWN_CATEGORY_HEADINGS = {
    '39233000': '39233010',  # Carboys, bottles, flasks: → bottles
    '56074900': '56074910',  # Other: → twine and ropes
    '61159900': '61159910',  # Of other textile materials: → socks/ankle-socks
    '95079000': '95079010',  # Other: → fishing tackle
    '96039000': '96039090',  # Other: → other brushes
    '96034000': '96034020',  # Paint brushes: → paint/varnish brushes
    '34011900': '34011910',  # Other: → soap products
    '33051010': '33051000',  # subcategory header → shampoos
}

# Codes with wrong CET detail digits (suffix doesn't match any end-node)
KNOWN_WRONG_CODES = {
    '84099199': '84099120',  # → marine craft engine parts
    '84831099': '84831020',  # → marine transmission shafts
    '85291099': '85291000',  # → aerials/reflectors (single end-node)
    '73063090': '73063000',  # → welded tubes (single end-node)
    '73181590': '73181500',  # → screws/bolts (single end-node)
    '84219990': '84219900',  # → filter parts (single end-node)
    '96034090': '96034020',  # → paint/varnish brushes
    '32091090': '32091000',  # → paints/varnishes (32091000 is the only valid end-node)
}

# Codes referencing non-existent HS subheadings
KNOWN_BAD_HEADINGS = {
    '34025000': '34022090',  # 3402.50 doesn't exist → retail cleaning preps
    '61159100': '61152100',  # 6115.91 doesn't exist → synthetic fibre tights
    '82030000': '82032000',  # heading level → pliers/pincers/tweezers
    '82131000': '82130090',  # 8213.10 doesn't exist → scissors other
    '85395200': '85395000',  # 8539.52 doesn't exist → LED lamps
    '96060000': '96050000',  # heading level → travel sewing kits
}

# Combined invalid code map (loaded once, used at classification time)
_INVALID_CODE_MAP = {**KNOWN_CATEGORY_HEADINGS, **KNOWN_WRONG_CODES, **KNOWN_BAD_HEADINGS}

# Also load from invalid_codes.json at runtime (supplements hardcoded list)
_invalid_codes_from_file = None
# Authoritative CET valid leaf codes (loaded from data/cet.db SSOT)
_cet_valid_codes = None
# Assessed classifications from ASYCUDA SQL Server databases
_assessed_classifications = None


def _load_invalid_codes(base_dir: str = '.') -> dict:
    """Load invalid_codes.json and merge with hardcoded invalid codes."""
    global _invalid_codes_from_file
    if _invalid_codes_from_file is not None:
        return _invalid_codes_from_file

    result = dict(_INVALID_CODE_MAP)
    invalid_path = os.path.join(base_dir, 'rules', 'invalid_codes.json')
    try:
        if os.path.exists(invalid_path):
            with open(invalid_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            for code, info in data.items():
                if code.startswith('_'):
                    continue  # skip comment keys
                if isinstance(info, dict) and 'correct_code' in info:
                    result[code] = info['correct_code']
    except Exception as e:
        logger.warning(f"Failed to load invalid_codes.json: {e}")

    _invalid_codes_from_file = result
    return result


def _load_cet_valid_codes(base_dir: str = '.') -> set:
    """Load authoritative CARICOM CET valid 8-digit leaf codes from data/cet.db (SSOT).

    Only loads leaf codes (is_leaf=1) — category headings are excluded from validation.
    """
    global _cet_valid_codes
    if _cet_valid_codes is not None:
        return _cet_valid_codes

    codes = set()

    # SSOT: load leaf codes from cet.db (SQLite)
    cet_db_path = os.path.join(base_dir, 'data', 'cet.db')
    try:
        if os.path.exists(cet_db_path):
            import sqlite3
            conn = sqlite3.connect(cet_db_path)
            cursor = conn.cursor()
            cursor.execute('SELECT hs_code FROM cet_codes WHERE enabled = 1 AND is_leaf = 1')
            for row in cursor.fetchall():
                code = row[0]
                if len(code) == 8 and code.isdigit():
                    codes.add(code)
            conn.close()
            if codes:
                logger.info(f"[CET] Loaded {len(codes)} valid CET leaf codes from {cet_db_path}")
    except Exception as e:
        logger.warning(f"Failed to load CET codes from cet.db: {e}")

    if not codes:
        logger.error("[CET] NO valid CET codes loaded — tariff code validation is DISABLED. "
                      "Populate data/cet.db with CARICOM CET schedule.")

    _cet_valid_codes = codes
    return codes


# ── Assessed classifications (Layer 0 — customs-verified entries) ──

_ASSESSED_NOISE_WORDS = {
    'the', 'a', 'an', 'and', 'or', 'for', 'with', 'in', 'on', 'to', 'of',
    'no', 'nr', 'num', 'pcs', 'pc', 'ea', 'each', 'qty', 'quantity',
    'size', 'color', 'colour', 'pack', 'set', 'pieces', 'item', 'items',
    'new', 'used', 'unit', 'units', 'assorted', 'various', 'other',
}


def _normalize_for_assessed(desc: str) -> str:
    """Normalize a description for assessed lookup matching.
    Must match the normalization used in extract_assessed_codes.py."""
    if not desc:
        return ''
    text = desc.lower()
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    words = [w for w in text.split() if w not in _ASSESSED_NOISE_WORDS and len(w) > 1]
    return ' '.join(words)


def _load_assessed_classifications(base_dir: str = '.') -> dict:
    """Load assessed tariff classifications from data/assessed_classifications.json."""
    global _assessed_classifications
    if _assessed_classifications is not None:
        return _assessed_classifications

    assessed_path = os.path.join(base_dir, 'data', 'assessed_classifications.json')
    try:
        if os.path.exists(assessed_path):
            with open(assessed_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            entries = data.get('entries', {})
            logger.info(f"[ASSESSED] Loaded {len(entries)} assessed classifications")
            _assessed_classifications = entries
            return entries
    except Exception as e:
        logger.warning(f"Failed to load assessed classifications: {e}")

    _assessed_classifications = {}
    return {}


def _cross_validate_chapter(norm_query: str, candidate_code: str,
                            assessed: dict,
                            matched_entry: dict = None) -> bool:
    """
    Cross-validate an assessed match against other assessed entries.

    Two checks:
    1. Inventory-ref check: if the matched entry shares an inventory_ref
       with a SKU-prefixed entry that gives a different chapter, reject.
       SKU-prefixed entries are more reliable (linked to actual inventory).
    2. Sibling-vote check: if the majority of closely related entries
       (75%+ token overlap) place this product in a different chapter
       AND the candidate is a minority (<30% of total weight), reject.

    Returns True if the match looks consistent, False to reject.
    """
    from collections import Counter

    candidate_chapter = candidate_code[:2]

    # ── Check 1: Inventory-ref cross-reference against SKU-prefixed entries ──
    # SKU-prefixed entries (start with digits) are tied to specific inventory
    # records. When the candidate entry conflicts with a SKU-prefixed entry
    # for the same inventory ref AND the SKU-prefixed entry has more
    # occurrences, the candidate is likely a data-entry error.
    if matched_entry:
        inv_refs = set(matched_entry.get('inventory_refs', []))
        candidate_count = matched_entry.get('count', 1)
        if inv_refs:
            for assessed_desc, entry in assessed.items():
                if not assessed_desc or not assessed_desc[0].isdigit():
                    continue
                entry_refs = set(entry.get('inventory_refs', []))
                if inv_refs & entry_refs:
                    ref_chapter = entry['code'][:2]
                    ref_count = entry.get('count', 1)
                    if ref_chapter != candidate_chapter and ref_count >= candidate_count:
                        logger.debug(
                            f"[ASSESSED CROSS-CHECK] Inventory ref conflict: "
                            f"'{assessed_desc[:40]}' -> {entry['code']} (n={ref_count}) "
                            f"vs candidate {candidate_code} (n={candidate_count})"
                        )
                        return False

    # ── Check 2: Sibling-vote check (token overlap) ──
    query_tokens = set(norm_query.split())
    if len(query_tokens) < 2:
        return True

    chapter_votes = Counter()
    for assessed_desc, entry in assessed.items():
        assessed_tokens = set(assessed_desc.split())
        if len(assessed_tokens) < 2:
            continue
        overlap = query_tokens & assessed_tokens
        if len(overlap) < max(2, len(query_tokens) * 0.75):
            continue
        chapter = entry['code'][:2]
        chapter_votes[chapter] += entry.get('count', 1)

    if not chapter_votes:
        return True

    total_weight = sum(chapter_votes.values())
    candidate_weight = chapter_votes.get(candidate_chapter, 0)
    majority_chapter, majority_weight = chapter_votes.most_common(1)[0]

    if candidate_chapter == majority_chapter:
        return True

    # Reject if candidate chapter is a clear minority
    if candidate_weight < total_weight * 0.30:
        logger.debug(
            f"[ASSESSED CROSS-CHECK] Chapter {candidate_chapter} "
            f"(weight={candidate_weight}/{total_weight}) vs majority "
            f"{majority_chapter} (weight={majority_weight}) "
            f"for '{norm_query[:50]}'"
        )
        return False

    return True


def lookup_assessed_classification(description: str, base_dir: str = '.',
                                    expected_chapter: str = None):
    """
    Look up an item description against assessed (customs-certified) classifications.

    This is Layer 0 — the highest confidence classification source because
    these codes were verified by customs officers in actual declarations.

    Args:
        description: Item description to look up
        base_dir: Project base directory
        expected_chapter: 2-digit chapter from LLM pre-assignment. If set,
            assessed matches whose chapter differs are rejected.

    Matching tiers:
      1. Exact normalized match (confidence 0.97)
      2. Containment match (confidence 0.90)
      3. Token overlap >70% (confidence 0.80)
    """
    if not description or len(description.strip()) < 3:
        return None

    assessed = _load_assessed_classifications(base_dir)
    if not assessed:
        return None

    norm_query = _normalize_for_assessed(description)
    if not norm_query or len(norm_query) < 4:
        return None

    # Tier 1: Exact match
    if norm_query in assessed:
        entry = assessed[norm_query]
        code = validate_and_correct_code(entry['code'], base_dir)
        # Gate: LLM expected_chapter check
        if expected_chapter and code[:2] != expected_chapter:
            logger.warning(
                f"[ASSESSED] Rejected exact match '{norm_query[:50]}' -> {code}: "
                f"chapter {code[:2]} vs LLM expected ch{expected_chapter}")
        # Cross-validate even exact matches: officer can mis-code the same
        # product repeatedly.  If the majority of related assessed entries
        # place this product in a different chapter, the code is suspect.
        elif _cross_validate_chapter(norm_query, code, assessed, matched_entry=entry):
            return {
                'code': code,
                'category': entry.get('category', 'ASSESSED'),
                'confidence': min(0.97, 0.90 + (entry.get('confidence', 0.5) * 0.07)),
                'source': 'assessed_exact',
                'notes': (f"Assessed classification (exact match, "
                          f"{entry['count']}/{entry['total']} entries, "
                          f"sources: {', '.join(entry.get('sources', []))}). "
                          f"Sample: {entry.get('sample_desc', '')[:80]}"),
            }
        else:
            logger.warning(
                f"[ASSESSED] Rejected exact match '{norm_query[:50]}' -> {code}: "
                f"chapter conflicts with majority of related assessed entries"
            )

    # Tier 2: Containment match — query in assessed desc, or assessed desc in query
    # Requirements to prevent false matches on generic single-word entries:
    #   - Both strings must be at least 2 words (no single-word containment)
    #   - The shorter string must be >= 12 chars
    #   - The match must cover >= 50% of the longer string's length
    best_containment = None
    best_containment_score = 0.0

    query_words = norm_query.split()
    if len(query_words) >= 2:
        for assessed_desc, entry in assessed.items():
            assessed_words = assessed_desc.split()
            if len(assessed_words) < 2:
                continue  # Skip single-word assessed entries for containment

            shorter_len = min(len(norm_query), len(assessed_desc))
            longer_len = max(len(norm_query), len(assessed_desc))
            if shorter_len < 12:
                continue  # Skip very short strings

            coverage = shorter_len / longer_len if longer_len > 0 else 0

            if norm_query in assessed_desc and coverage >= 0.40:
                if coverage > best_containment_score:
                    best_containment = (assessed_desc, entry)
                    best_containment_score = coverage
            elif assessed_desc in norm_query and coverage >= 0.40:
                if coverage > best_containment_score:
                    best_containment = (assessed_desc, entry)
                    best_containment_score = coverage

    if best_containment and best_containment_score >= 0.40:
        desc_key, entry = best_containment
        code = validate_and_correct_code(entry['code'], base_dir)
        # Gate: LLM expected_chapter check
        if expected_chapter and code[:2] != expected_chapter:
            logger.warning(
                f"[ASSESSED] Rejected containment match '{desc_key[:50]}' -> {code}: "
                f"chapter {code[:2]} vs LLM expected ch{expected_chapter}")
        # Cross-validate: reject if other assessed entries for similar
        # descriptions disagree at the chapter level (officer data errors)
        elif _cross_validate_chapter(norm_query, code, assessed, matched_entry=entry):
            return {
                'code': code,
                'category': entry.get('category', 'ASSESSED'),
                'confidence': min(0.92, 0.85 + (entry.get('confidence', 0.5) * 0.07)),
                'source': 'assessed_containment',
                'notes': (f"Assessed classification (containment match against "
                          f"'{desc_key[:60]}', "
                          f"{entry['count']}/{entry['total']} entries)"),
            }
        else:
            logger.warning(
                f"[ASSESSED] Rejected containment match '{desc_key[:50]}' -> {code}: "
                f"chapter conflicts with other assessed entries for '{norm_query[:50]}'"
            )

    # Tier 3: Token overlap >75% (requires at least 3 tokens in query)
    # Only use high-confidence assessed entries (>= 0.85 confidence, >= 5 occurrences)
    query_tokens = set(norm_query.split())
    if len(query_tokens) >= 3:
        best_overlap = None
        best_overlap_ratio = 0.0

        for assessed_desc, entry in assessed.items():
            assessed_tokens = set(assessed_desc.split())
            if len(assessed_tokens) < 2:
                continue  # Skip single-word assessed entries
            if entry.get('confidence', 0) < 0.85 or entry.get('count', 0) < 5:
                continue  # Skip low-confidence entries for fuzzy matching

            overlap = query_tokens & assessed_tokens
            if len(overlap) < 3:
                continue  # Require at least 3 common tokens

            ratio = len(overlap) / len(query_tokens)
            reverse_ratio = len(overlap) / len(assessed_tokens)
            combined = (ratio + reverse_ratio) / 2

            if combined > 0.75 and combined > best_overlap_ratio:
                best_overlap = (assessed_desc, entry)
                best_overlap_ratio = combined

        if best_overlap and best_overlap_ratio > 0.75:
            desc_key, entry = best_overlap
            code = validate_and_correct_code(entry['code'], base_dir)
            # Gate: LLM expected_chapter check
            if expected_chapter and code[:2] != expected_chapter:
                logger.warning(
                    f"[ASSESSED] Rejected token-overlap match '{desc_key[:50]}' -> {code}: "
                    f"chapter {code[:2]} vs LLM expected ch{expected_chapter}")
            # Cross-validate token overlap matches the same way
            elif _cross_validate_chapter(norm_query, code, assessed, matched_entry=entry):
                return {
                    'code': code,
                    'category': entry.get('category', 'ASSESSED'),
                    'confidence': min(0.85, 0.75 + (best_overlap_ratio * 0.10)),
                    'source': 'assessed_token_overlap',
                    'notes': (f"Assessed classification (token overlap "
                              f"{best_overlap_ratio:.0%} against "
                              f"'{desc_key[:60]}')"),
                }
            else:
                logger.warning(
                    f"[ASSESSED] Rejected token-overlap match '{desc_key[:50]}' -> {code}: "
                    f"chapter conflicts with other assessed entries for '{norm_query[:50]}'"
                )

    return None


def validate_and_correct_code(code: str, base_dir: str = '.') -> str:
    """
    Validate a tariff code against the authoritative CARICOM CET schedule.

    ENFORCED RULES:
    1. Code must be exactly 8 digits
    2. Known invalid codes are auto-corrected via invalid_codes.json
    3. Code must exist in the CARICOM CET valid leaf codes (data/cet.db, is_leaf=1)
    4. If code not in CET, try suffix '00' as fallback (many headings have no subdivision)

    Returns the corrected code, or the original if valid.
    """
    if not code or code == 'UNKNOWN':
        return code

    # Step 1: Check known invalid code mappings
    invalid_map = _load_invalid_codes(base_dir)
    if code in invalid_map:
        corrected = invalid_map[code]
        logger.warning(
            f"[CET VALIDATION] Auto-corrected invalid code {code} -> {corrected}"
        )
        return corrected

    # Step 2: Validate against authoritative CET code list (cet.db SSOT)
    cet_codes = _load_cet_valid_codes(base_dir)
    if not cet_codes:
        # CET list not available — validation disabled, return code but warn
        logger.warning(f"[CET VALIDATION] Cannot validate {code} — no CET codes loaded")
        return code

    if code in cet_codes:
        return code

    # Code not in CET — try auto-correction: replace suffix with 00
    base6 = code[:6]
    fallback = base6 + '00'
    if fallback in cet_codes:
        # Also check the fallback isn't a known invalid code (category heading)
        if fallback in invalid_map:
            corrected = invalid_map[fallback]
            logger.warning(
                f"[CET VALIDATION] Code {code} not in CET, fallback {fallback} "
                f"is a category heading, corrected to {corrected}"
            )
            return corrected
        logger.warning(
            f"[CET VALIDATION] Code {code} not in CET, corrected to {fallback} "
            f"(no national subdivision for {base6[:4]}.{base6[4:6]})"
        )
        return fallback

    # Try broader fallbacks: find any valid code under the same 6-digit heading
    # Pick the '90' suffix (catch-all "other") or the last valid code
    heading_codes = sorted(c for c in cet_codes
                           if c[:6] == base6 and c not in invalid_map)
    if heading_codes:
        # Prefer '90' suffixes (catch-all "other" categories)
        other_codes = [c for c in heading_codes if c.endswith('90')]
        chosen = other_codes[-1] if other_codes else heading_codes[-1]
        logger.warning(
            f"[CET VALIDATION] Code {code} not in CET, using nearest code "
            f"{chosen} under heading {base6[:4]}.{base6[4:6]}"
        )
        return chosen

    # Try one more level: find any valid code under the same 4-digit heading
    base4 = code[:4]
    heading4_codes = sorted(c for c in cet_codes
                            if c[:4] == base4 and c not in invalid_map)
    if heading4_codes:
        other_codes = [c for c in heading4_codes if c.endswith('90') or c.endswith('00')]
        chosen = other_codes[-1] if other_codes else heading4_codes[-1]
        logger.warning(
            f"[CET VALIDATION] Code {code} not in CET, using nearest code "
            f"{chosen} under heading {base4}"
        )
        return chosen

    # No valid code found under this heading — log and return original
    logger.warning(
        f"[CET VALIDATION] Code {code} not found in CET and no fallback available, "
        f"rejecting as invalid"
    )
    return code


# ─── END CET VALIDATION ──────────────────────────────────────────────────────

# Try to import web search capabilities
try:
    import requests
    REQUESTS_AVAILABLE = True
    USE_REQUESTS = True
except ImportError:
    REQUESTS_AVAILABLE = False
    USE_REQUESTS = False

# Fallback to urllib (standard library)
try:
    from urllib.request import urlopen, Request
    from urllib.parse import quote as url_quote
    from urllib.error import URLError, HTTPError
    URLLIB_AVAILABLE = True
except ImportError:
    URLLIB_AVAILABLE = False

# Try to import Anthropic SDK for LLM classification
try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

# We can do web lookups if either is available
WEB_LOOKUP_AVAILABLE = REQUESTS_AVAILABLE or URLLIB_AVAILABLE


# Brand name to product category mappings (for when only brand is in description)
BRAND_MAPPINGS = {
    'zak designs': {'code': '39241090', 'category': 'HOUSEHOLD', 'description': 'Plastic tableware (water bottles)'},
    'hydro flask': {'code': '73239390', 'category': 'HOUSEHOLD', 'description': 'Stainless steel bottles'},
    'yeti': {'code': '73239390', 'category': 'HOUSEHOLD', 'description': 'Stainless steel bottles/coolers'},
    'stanley': {'code': '73239390', 'category': 'HOUSEHOLD', 'description': 'Stainless steel bottles'},
    'anker': {'code': '85044000', 'category': 'ELECTRONICS', 'description': 'Portable chargers/electronics'},
    'beats': {'code': '85183000', 'category': 'ELECTRONICS', 'description': 'Headphones'},
    'jbl': {'code': '85182200', 'category': 'ELECTRONICS', 'description': 'Speakers'},
    'carter': {'code': '61034290', 'category': 'CLOTHING', 'description': "Children's clothing"},
    'gerber': {'code': '61034290', 'category': 'CLOTHING', 'description': "Baby clothing"},
    'pampers': {'code': '96190029', 'category': 'BABY CARE', 'description': 'Diapers'},
    'huggies': {'code': '96190029', 'category': 'BABY CARE', 'description': 'Diapers'},
}

# Common product category to HS code mappings for web lookup fallback
CATEGORY_HS_CODES = {
    # Clothing - Children's/Baby
    'children_cotton_clothing': {
        'code': '61034290',
        'category': 'CLOTHING',
        'keywords': ['kids', 'children', 'child', 'toddler', 'baby', 'boys', 'girls', 'infant'],
        'material_keywords': ['cotton', 'pants', 'shorts', 'trousers'],
        'description': "Children's cotton trousers/shorts"
    },
    'children_synthetic_clothing': {
        'code': '61034390',
        'category': 'CLOTHING',
        'keywords': ['kids', 'children', 'child', 'toddler', 'baby', 'boys', 'girls'],
        'material_keywords': ['polyester', 'synthetic', 'nylon'],
        'description': "Children's synthetic trousers"
    },
    'children_tshirts': {
        'code': '61099020',
        'category': 'CLOTHING',
        'keywords': ['kids', 'children', 'child', 'toddler', 'baby', 'boys', 'girls'],
        'item_keywords': ['t-shirt', 'tshirt', 'shirt', 'top'],
        'description': "Children's T-shirts"
    },
    # Adult Clothing
    'adult_cotton_pants': {
        'code': '62034290',
        'category': 'CLOTHING',
        'keywords': ['men', 'women', 'adult'],
        'item_keywords': ['pants', 'trousers', 'jeans'],
        'material_keywords': ['cotton', 'denim'],
        'description': "Adult cotton trousers"
    },
    # Household items
    'plastic_kitchenware': {
        'code': '39241090',
        'category': 'HOUSEHOLD',
        'keywords': ['water bottle', 'bottle', 'cup', 'mug', 'container', 'tumbler', 'plastic'],
        'description': "Plastic tableware/kitchenware"
    },
    'stainless_bottle': {
        'code': '73239390',
        'category': 'HOUSEHOLD',
        'keywords': ['stainless', 'steel', 'metal', 'flask', 'thermos', 'insulated'],
        'item_keywords': ['bottle', 'flask'],
        'description': "Stainless steel bottles/flasks"
    },
    # Lighting
    'led_lamps': {
        'code': '94054000',
        'category': 'LIGHTING',
        'keywords': ['lamp', 'light', 'led', 'desk lamp', 'table lamp', 'floor lamp', 'lighting'],
        'item_keywords': ['lamp', 'light'],
        'description': "LED lamps and lighting fixtures"
    },
    # Electronics
    'phone_accessories': {
        'code': '85177000',
        'category': 'ELECTRONICS',
        'keywords': ['phone case', 'phone cover', 'screen protector', 'charger', 'cable', 'usb', 'iphone', 'samsung', 'case'],
        'item_keywords': ['phone', 'case', 'cover'],
        'description': "Phone accessories"
    },
    'headphones': {
        'code': '85183000',
        'category': 'ELECTRONICS',
        'keywords': ['headphone', 'earphone', 'earbud', 'airpod', 'headset'],
        'description': "Headphones/earphones"
    },
    'speakers': {
        'code': '85182200',
        'category': 'ELECTRONICS',
        'keywords': ['speaker', 'bluetooth speaker', 'portable speaker', 'wireless speaker'],
        'item_keywords': ['speaker'],
        'description': "Loudspeakers/speakers"
    },
    # Toys
    'toys_general': {
        'code': '95030090',
        'category': 'TOYS',
        'keywords': ['toy', 'toys', 'game', 'puzzle', 'doll', 'action figure', 'lego', 'plush', 'building blocks', 'blocks'],
        'item_keywords': ['toy', 'game', 'blocks', 'building'],
        'description': "Toys and games"
    },
    # Beauty/Personal Care
    'cosmetics': {
        'code': '33049900',
        'category': 'BEAUTY',
        'keywords': ['makeup', 'cosmetic', 'lipstick', 'mascara', 'foundation', 'beauty'],
        'description': "Cosmetics"
    },
    'skincare': {
        'code': '33049100',
        'category': 'BEAUTY',
        'keywords': ['lotion', 'cream', 'moisturizer', 'serum', 'skincare', 'face wash'],
        'description': "Skincare products"
    },
    # Bags
    'bags_handbags': {
        'code': '42022200',
        'category': 'BAGS',
        'keywords': ['handbag', 'purse', 'bag', 'backpack', 'tote', 'shoulder bag'],
        'description': "Handbags and bags"
    },
    # Jewelry/Accessories
    'jewelry': {
        'code': '71179000',
        'category': 'JEWELRY',
        'keywords': ['jewelry', 'necklace', 'bracelet', 'ring', 'earring', 'pendant'],
        'description': "Imitation jewelry"
    },
    # Home Decor
    'home_decor': {
        'code': '39264000',
        'category': 'HOME',
        'keywords': ['decor', 'decoration', 'ornament', 'figurine', 'vase'],
        'description': "Home decoration items"
    },
}


def _validate_hardcoded_mappings(base_dir: str = '.') -> None:
    """Validate BRAND_MAPPINGS and CATEGORY_HS_CODES against CET at startup.

    Logs warnings for any hardcoded codes that don't exist in the CET list,
    so they can be corrected in the source code.
    """
    cet_codes = _load_cet_valid_codes(base_dir)
    if not cet_codes:
        return  # Can't validate without CET list

    invalid_map = _load_invalid_codes(base_dir)

    for brand, info in BRAND_MAPPINGS.items():
        code = info.get('code', '')
        if code in invalid_map:
            logger.warning(
                f"[STARTUP] BRAND_MAPPINGS['{brand}'] code {code} is a known "
                f"invalid code (correct: {invalid_map[code]})")
        elif code and code not in cet_codes:
            logger.warning(
                f"[STARTUP] BRAND_MAPPINGS['{brand}'] code {code} not in CET list")

    for cat_id, info in CATEGORY_HS_CODES.items():
        code = info.get('code', '')
        if code in invalid_map:
            logger.warning(
                f"[STARTUP] CATEGORY_HS_CODES['{cat_id}'] code {code} is a known "
                f"invalid code (correct: {invalid_map[code]})")
        elif code and code not in cet_codes:
            logger.warning(
                f"[STARTUP] CATEGORY_HS_CODES['{cat_id}'] code {code} not in CET list")


# Flag to run startup validation only once
_startup_validated = False


def lookup_hs_code_web(description: str, config: Dict = None) -> Optional[Dict]:
    """
    Look up HS code for a product description using category matching and web search.

    Args:
        description: Product description
        config: Pipeline configuration

    Returns:
        Classification dict or None
    """
    if not description:
        return None

    config = config or {}
    base_dir = config.get('base_dir', '.')

    # Run startup validation once (checks hardcoded mappings against CET)
    global _startup_validated
    if not _startup_validated:
        _validate_hardcoded_mappings(base_dir)
        _startup_validated = True

    desc_lower = description.lower()

    # First try brand-based matching (for short descriptions that are just brand names)
    for brand, brand_info in BRAND_MAPPINGS.items():
        if brand in desc_lower:
            code = validate_and_correct_code(brand_info['code'], base_dir)
            return {
                'code': code,
                'category': brand_info['category'],
                'confidence': 0.7,
                'source': 'brand_lookup',
                'notes': brand_info.get('description', f'Brand: {brand}'),
            }

    # Then try category-based matching
    best_match = None
    best_score = 0

    for category_id, category_info in CATEGORY_HS_CODES.items():
        score = 0
        keywords = category_info.get('keywords', [])
        material_keywords = category_info.get('material_keywords', [])
        item_keywords = category_info.get('item_keywords', [])

        # Count keyword matches
        for keyword in keywords:
            if keyword.lower() in desc_lower:
                score += 1

        # Material and item keywords are bonus points
        for mat_keyword in material_keywords:
            if mat_keyword.lower() in desc_lower:
                score += 2

        for item_keyword in item_keywords:
            if item_keyword.lower() in desc_lower:
                score += 2

        if score > best_score:
            best_score = score
            best_match = category_info

    # Require at least 2 keyword matches for confidence
    if best_match and best_score >= 2:
        code = validate_and_correct_code(best_match['code'], base_dir)
        confidence = min(0.75, 0.5 + (best_score * 0.05))
        return {
            'code': code,
            'category': best_match['category'],
            'confidence': confidence,
            'source': 'category_lookup',
            'notes': best_match.get('description', ''),
        }

    # Try web search to gather context, then use LLM to classify
    web_verify = config.get('web_verify', {})
    llm_enabled = config.get('llm_classification', {}).get('enabled', True)

    if WEB_LOOKUP_AVAILABLE and web_verify.get('enabled', True):
        # Gather web search results as context
        web_context = _gather_web_context(description, config)

        # If LLM classification is enabled, use it to analyze web results
        if llm_enabled and web_context:
            llm_result = classify_with_llm(description, web_context, config)
            if llm_result:
                # Cache the LLM result for future use
                base_dir = config.get('base_dir', '.')
                _save_to_cache(_extract_search_terms(description), description, llm_result, base_dir)
                logger.info(f"LLM classified: {description[:50]}... -> {llm_result['code']}")
                return llm_result

        # Fallback: try direct web search (without LLM analysis)
        web_result = _search_hs_code_online(description, config)
        if web_result:
            return web_result

    return None


def _gather_web_context(description: str, config: Dict = None) -> str:
    """
    Search the web for product information to provide context for LLM classification.

    Returns a summary of web search results about the product.
    """
    if not WEB_LOOKUP_AVAILABLE:
        return ""

    search_terms = _extract_search_terms(description)
    if not search_terms:
        return ""

    context_parts = []

    # Search for product + HS code information
    try:
        if USE_REQUESTS:
            encoded_query = requests.utils.quote(f"{search_terms} HS code tariff classification CARICOM")
        else:
            encoded_query = url_quote(f"{search_terms} HS code tariff classification CARICOM")

        url = f"https://html.duckduckgo.com/html/?q={encoded_query}"
        text = _fetch_url(url)

        if text:
            # Extract relevant snippets (look for HS codes and descriptions)
            # Find text around HS code mentions
            hs_mentions = re.finditer(r'.{0,100}(\d{4}[\.\s]?\d{2}[\.\s]?\d{2}).{0,100}', text, re.IGNORECASE)
            for match in list(hs_mentions)[:5]:  # Take up to 5 mentions
                snippet = match.group(0)
                # Clean HTML tags
                snippet = re.sub(r'<[^>]+>', ' ', snippet)
                snippet = re.sub(r'\s+', ' ', snippet).strip()
                if len(snippet) > 20:
                    context_parts.append(snippet)

            # Also look for tariff/classification mentions
            tariff_mentions = re.finditer(r'.{0,50}(tariff|classif|HS code|HTS|customs).{0,100}', text, re.IGNORECASE)
            for match in list(tariff_mentions)[:3]:
                snippet = match.group(0)
                snippet = re.sub(r'<[^>]+>', ' ', snippet)
                snippet = re.sub(r'\s+', ' ', snippet).strip()
                if len(snippet) > 20 and snippet not in context_parts:
                    context_parts.append(snippet)

    except Exception as e:
        logger.debug(f"Web context gathering failed: {e}")

    return "\n".join(context_parts) if context_parts else f"Product type: {search_terms}"


def _search_hs_code_online(description: str, config: Dict = None) -> Optional[Dict]:
    """
    Search online databases for HS code using multiple strategies.

    Strategies:
    1. Check local cache of previous lookups
    2. Search DuckDuckGo for "[product] HS code tariff"
    3. Query HTS.usitc.gov directly
    4. Search for similar products with known codes

    Successful lookups are cached for future reference.
    """
    if not WEB_LOOKUP_AVAILABLE:
        return None

    config = config or {}
    base_dir = config.get('base_dir', '.')

    # Clean description for search
    search_terms = _extract_search_terms(description)
    if not search_terms:
        return None

    # Strategy 1: Check local cache first
    cache_result = _check_lookup_cache(search_terms, base_dir)
    if cache_result:
        logger.info(f"Cache hit for '{search_terms}': {cache_result['code']}")
        return cache_result

    # Strategy 2: DuckDuckGo search for HS codes
    result = _search_duckduckgo(search_terms)
    if result:
        _save_to_cache(search_terms, description, result, base_dir)
        return result

    # Strategy 3: HTS.usitc.gov direct search
    result = _search_hts_gov(search_terms)
    if result:
        _save_to_cache(search_terms, description, result, base_dir)
        return result

    # Strategy 4: Try simplified product terms
    simplified = _simplify_description(description)
    if simplified != search_terms:
        result = _search_duckduckgo(simplified)
        if result:
            _save_to_cache(search_terms, description, result, base_dir)
            return result

    logger.debug(f"Web search found no results for: {description[:50]}")
    return None


def _extract_search_terms(description: str) -> str:
    """Extract meaningful search terms from product description."""
    # Remove noise words and special characters
    text = re.sub(r'[^\w\s]', ' ', description)
    words = text.split()

    # Remove common noise
    noise = {'the', 'a', 'an', 'and', 'or', 'for', 'with', 'in', 'on', 'to', 'of',
             'size', 'color', 'pack', 'set', 'pcs', 'pieces', 'item', 'new'}

    # Keep meaningful words
    meaningful = [w for w in words if w.lower() not in noise and len(w) > 1]

    # Take first 5 meaningful words
    return ' '.join(meaningful[:5])


def _simplify_description(description: str) -> str:
    """Create simplified search terms focusing on product type."""
    text = description.lower()

    # Extract key product indicators
    product_words = []

    # Check for material
    materials = ['cotton', 'polyester', 'leather', 'plastic', 'metal', 'steel',
                 'stainless', 'wood', 'glass', 'rubber', 'silicone', 'nylon']
    for mat in materials:
        if mat in text:
            product_words.append(mat)
            break

    # Check for product type
    products = ['pants', 'shirt', 'dress', 'shoes', 'bag', 'bottle', 'phone',
                'case', 'charger', 'cable', 'toy', 'game', 'watch', 'jewelry',
                'headphone', 'earphone', 'makeup', 'cream', 'lotion']
    for prod in products:
        if prod in text:
            product_words.append(prod)
            break

    # Check for demographic
    demographics = ['kids', 'children', 'baby', 'men', 'women', 'adult', 'toddler']
    for demo in demographics:
        if demo in text:
            product_words.insert(0, demo)
            break

    return ' '.join(product_words) if product_words else _extract_search_terms(description)


def _fetch_url(url: str, timeout: int = 10) -> Optional[str]:
    """Fetch URL content using requests or urllib."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    }

    try:
        if USE_REQUESTS:
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code == 200:
                return response.text
        elif URLLIB_AVAILABLE:
            req = Request(url, headers=headers)
            with urlopen(req, timeout=timeout) as response:
                return response.read().decode('utf-8', errors='ignore')
    except Exception as e:
        logger.debug(f"URL fetch failed for {url[:50]}: {e}")

    return None


def _search_duckduckgo(search_terms: str) -> Optional[Dict]:
    """Search DuckDuckGo for HS code information."""
    try:
        # Search for product + HS code
        query = f"{search_terms} HS code tariff classification"

        if USE_REQUESTS:
            encoded_query = requests.utils.quote(query)
        else:
            encoded_query = url_quote(query)

        url = f"https://html.duckduckgo.com/html/?q={encoded_query}"
        text = _fetch_url(url)

        if text:
            # Look for HS code patterns in results
            # Pattern: 4 digits, optional separator, 2 digits, optional separator, 2 digits
            hs_patterns = [
                r'HS\s*[:-]?\s*(\d{4})[\.\s]?(\d{2})[\.\s]?(\d{2})',  # HS: 6103.42.00
                r'HTS\s*[:-]?\s*(\d{4})[\.\s]?(\d{2})[\.\s]?(\d{2})',  # HTS: 6103.42.00
                r'tariff\s+code\s*[:-]?\s*(\d{4})[\.\s]?(\d{2})[\.\s]?(\d{2})',
                r'(\d{4})\.(\d{2})\.(\d{2})\d{0,2}',  # 6103.42.0010 format
            ]

            for pattern in hs_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    # Combine matched groups
                    code = ''.join(matches[0])[:8]
                    if len(code) == 8 and _is_valid_hs_code(code):
                        return {
                            'code': code,
                            'category': 'WEB_LOOKUP',
                            'confidence': 0.65,
                            'source': 'web_search_duckduckgo',
                            'notes': f'Found via web search for: {search_terms}',
                        }

    except Exception as e:
        logger.debug(f"DuckDuckGo search failed: {e}")

    return None


def _search_hts_gov(search_terms: str) -> Optional[Dict]:
    """Search HTS.usitc.gov for HS codes."""
    try:
        if USE_REQUESTS:
            encoded_query = requests.utils.quote(search_terms)
        else:
            encoded_query = url_quote(search_terms)

        url = f"https://hts.usitc.gov/search?query={encoded_query}"
        text = _fetch_url(url)

        if text:
            # HTS site returns results in specific format
            # Look for 8-10 digit codes
            patterns = [
                r'<td[^>]*>\s*(\d{4}\.\d{2}\.\d{2})\d{0,2}\s*</td>',
                r'(\d{4}\.\d{2}\.\d{2})\d{0,2}',
            ]

            for pattern in patterns:
                matches = re.findall(pattern, text)
                if matches:
                    code = matches[0].replace('.', '')[:8]
                    if len(code) == 8 and _is_valid_hs_code(code):
                        return {
                            'code': code,
                            'category': 'WEB_LOOKUP',
                            'confidence': 0.7,
                            'source': 'web_search_hts_gov',
                            'notes': f'Found via HTS.gov search for: {search_terms}',
                        }

    except Exception as e:
        logger.debug(f"HTS.gov search failed: {e}")

    return None


def _is_valid_hs_code(code: str, base_dir: str = '.') -> bool:
    """Validate that a code is a real, usable CARICOM CET end-node code.

    Checks (in order):
    1. Format: exactly 8 digits, chapter 01-97 (not 77)
    2. Not a known invalid/category-heading code
    3. Exists in the authoritative CET valid codes list

    This is the central gatekeeper — every code from every source
    (rules, web, LLM, cache) must pass this check.
    """
    if len(code) != 8:
        return False

    # Must be all digits
    if not code.isdigit():
        return False

    # First 2 digits indicate chapter (01-97)
    chapter = int(code[:2])
    if chapter < 1 or chapter > 97:
        return False

    # Chapter 77 is reserved for future use
    if chapter == 77:
        return False

    # Reject known invalid codes (category headings, wrong digits, bad headings)
    invalid_map = _load_invalid_codes(base_dir)
    if code in invalid_map:
        return False

    # Check against authoritative CET list (if available)
    cet_codes = _load_cet_valid_codes(base_dir)
    if cet_codes and code not in cet_codes:
        return False

    return True


def _check_lookup_cache(search_terms: str, base_dir: str) -> Optional[Dict]:
    """Check if we have a cached lookup for these search terms.

    Re-validates cached codes against current CET list to catch stale
    or previously-invalid codes that were cached before validation
    was strengthened.
    """
    cache_path = os.path.join(base_dir, 'data', 'hs_lookup_cache.json')

    if not os.path.exists(cache_path):
        return None

    try:
        with open(cache_path, 'r', encoding='utf-8') as f:
            cache = json.load(f)

        # Normalize search terms for lookup
        key = search_terms.lower().strip()

        if key in cache:
            entry = cache[key]
            code = entry.get('code', '')
            # Re-validate cached code against current CET list
            if code and code != 'UNKNOWN':
                code = validate_and_correct_code(code, base_dir)
            return {
                'code': code,
                'category': entry.get('category', 'CACHED'),
                'confidence': 0.8,
                'source': 'cache',
                'notes': entry.get('notes', 'From lookup cache'),
            }

        # Try partial matching
        for cached_key, entry in cache.items():
            if key in cached_key or cached_key in key:
                code = entry.get('code', '')
                if code and code != 'UNKNOWN':
                    code = validate_and_correct_code(code, base_dir)
                return {
                    'code': code,
                    'category': entry.get('category', 'CACHED'),
                    'confidence': 0.7,
                    'source': 'cache_partial',
                    'notes': f"Partial match from cache: {cached_key}",
                }

    except Exception as e:
        logger.debug(f"Cache lookup failed: {e}")

    return None


def _save_to_cache(search_terms: str, description: str, result: Dict, base_dir: str):
    """Save a successful lookup to cache for future use.

    Verification gates (all must pass before caching):
    1. Code must not be UNKNOWN
    2. Code must pass CET validation (valid 8-digit CET end-node)
    3. Source confidence must meet minimum threshold:
       - assessed/rules/manual: always cached (trusted sources)
       - web_search: confidence >= 0.80
       - llm_classification: confidence >= 0.85
    4. Cross-check against assessed data: if assessed data has a
       classification for this description at the 4-digit heading level
       and it disagrees, reject the cache (assessed data wins)
    """
    code = result.get('code', '')
    if not code or code == 'UNKNOWN':
        return  # Don't cache failures

    source = result.get('source', 'web_search')
    confidence = result.get('confidence', 0.5)

    # Gate 1: Validate code format against CET
    code = validate_and_correct_code(code, base_dir)
    if not code or code == 'UNKNOWN':
        logger.debug(f"Cache rejected (invalid CET code): {search_terms} -> {result.get('code')}")
        return

    # Gate 2: Confidence threshold by source
    trusted_sources = {'assessed_exact', 'assessed_containment', 'assessed_token_overlap',
                       'rule', 'manual_correction', 'classification_rule'}
    if source not in trusted_sources:
        min_confidence = 0.85 if 'llm' in source else 0.80
        if confidence < min_confidence:
            logger.info(
                f"Cache rejected (low confidence {confidence:.2f} < {min_confidence} "
                f"for source '{source}'): {search_terms} -> {code}"
            )
            return

    # Gate 3: Cross-check against assessed customs data
    if source not in trusted_sources:
        assessed_result = lookup_assessed_classification(description, base_dir)
        if assessed_result and assessed_result.get('code', 'UNKNOWN') != 'UNKNOWN':
            assessed_code = assessed_result['code']
            # Compare at 4-digit heading level — different heading = definitely wrong
            if code[:4] != assessed_code[:4]:
                logger.info(
                    f"Cache rejected (conflicts with assessed data): "
                    f"{search_terms} -> {code} (heading {code[:4]}), "
                    f"assessed says {assessed_code} (heading {assessed_code[:4]}) "
                    f"via {assessed_result.get('source', '?')}"
                )
                return

    cache_path = os.path.join(base_dir, 'data', 'hs_lookup_cache.json')

    try:
        # Load existing cache
        cache = {}
        if os.path.exists(cache_path):
            with open(cache_path, 'r', encoding='utf-8') as f:
                cache = json.load(f)

        # Add new entry (with validated code)
        key = search_terms.lower().strip()
        cache[key] = {
            'code': code,
            'category': result.get('category', 'WEB_LOOKUP'),
            'original_description': description,
            'source': source,
            'confidence': confidence,
            'notes': result.get('notes', ''),
            'cached_at': __import__('datetime').datetime.now().isoformat(),
        }

        # Save cache atomically (write to temp, then rename to prevent corruption)
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        tmp_path = cache_path + '.tmp'
        with open(tmp_path, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=2)
        os.replace(tmp_path, cache_path)

        logger.info(f"Cached HS lookup: {key} -> {code} (source={source}, conf={confidence:.2f})")

    except Exception as e:
        logger.debug(f"Failed to save to cache: {e}")

    # Dual-write to SQLite classifications DB (Phase 1: non-blocking)
    try:
        from classification_db import upsert_classification, get_db_path
        db_path = get_db_path(base_dir)
        if os.path.exists(db_path):
            is_trusted = source in trusted_sources
            upsert_classification(
                db_path, description, code, source,
                confidence=confidence,
                category=result.get('category', ''),
                unverified=0 if is_trusted else 1,
            )
    except Exception as e:
        logger.debug(f"DB dual-write failed (non-blocking): {e}")


def _load_llm_settings(base_dir: str) -> Dict:
    """Load LLM settings, preferring core.config if available."""
    # Try centralized config first (SSOT)
    try:
        from core.config import get_config
        cfg = get_config()
        if cfg.llm_api_key:
            return {
                'api_key': cfg.llm_api_key,
                'base_url': cfg.llm_base_url,
                'model': cfg.llm_model,
            }
    except (ImportError, Exception):
        pass

    # Fallback: load from app's settings.json
    settings_path = os.path.join(base_dir, 'data', 'settings.json')
    try:
        if os.path.exists(settings_path):
            with open(settings_path, 'r', encoding='utf-8') as f:
                settings = json.load(f)
            return {
                'api_key': settings.get('apiKey', ''),
                'base_url': settings.get('baseUrl', 'https://api.z.ai/api/anthropic'),
                'model': settings.get('model', 'glm-5'),  # SSOT: src/autoinvoice/domain/models/settings.py
            }
    except Exception as e:
        logger.debug(f"Failed to load LLM settings: {e}")

    # Fallback to environment variables
    return {
        'api_key': os.environ.get('ZAI_API_KEY', os.environ.get('ANTHROPIC_API_KEY', '')),
        'base_url': os.environ.get('ZAI_BASE_URL', 'https://api.z.ai/api/anthropic'),
        'model': os.environ.get('ZAI_MODEL', 'glm-5'),  # SSOT: src/autoinvoice/domain/models/settings.py
    }


def _build_classification_prompt(description: str, web_results: str) -> str:
    """Build the LLM classification prompt with domain-specific guidance."""
    return f"""You are a CARICOM customs tariff classification expert. Determine the correct 8-digit HS code for this product.

Product Description: {description}

Web Search Context:
{web_results}

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

5. APPAREL CLASSIFICATION (Chapters 61-62):
   - Knitted/crocheted garments → Chapter 61
   - Woven/not-knitted garments → Chapter 62
   - Men's cotton trousers: denim → 62034210, other → 62034220, other cotton → 62034290
   - Men's trousers: synthetic fibres → 62034310, other materials → 62034910/62034990
   - Women's cotton trousers: → 62046210/62046290
   - Women's trousers: synthetic fibres → 62046310, other materials → 62046910/62046990
   - T-shirts, singlets (knitted) → 61091000/61099000

6. Codes MUST be exactly 8 digits and must be valid CARICOM CET end-node codes.
   Do NOT invent codes — only use codes that exist in the CARICOM Common External Tariff schedule.

Respond with ONLY a JSON object:
{{"code": "XXXXXXXX", "category": "CATEGORY", "confidence": 0.X, "reasoning": "brief explanation"}}"""


def classify_with_llm(description: str, web_results: str, config: Dict = None) -> Optional[Dict]:
    """
    Use LLM (Claude/GLM via Z.AI) to classify a product based on description and web search results.

    The LLM analyzes the product and determines the correct CARICOM CET code.

    Args:
        description: Product description
        web_results: Web search results providing context
        config: Configuration including API key

    Returns:
        Classification dict or None
    """
    config = config or {}
    base_dir = config.get('base_dir', '.')

    # Load LLM settings from app config
    llm_settings = _load_llm_settings(base_dir)
    api_key = config.get('anthropic_api_key') or llm_settings.get('api_key')
    base_url = llm_settings.get('base_url', 'https://api.z.ai/api/anthropic')
    model = llm_settings.get('model', 'glm-5')  # SSOT: src/autoinvoice/domain/models/settings.py

    if not api_key:
        logger.debug("No API key available for LLM classification")
        return None

    # Always use urllib for Z.AI endpoint (Anthropic SDK defaults to api.anthropic.com)
    # The Z.AI key only works with the Z.AI proxy endpoint
    if 'z.ai' in base_url or not ANTHROPIC_AVAILABLE:
        return _classify_with_llm_urllib(description, web_results, api_key, base_url, model, config)

    try:
        client = anthropic.Anthropic(api_key=api_key, base_url=base_url)

        # Build prompt
        prompt = _build_classification_prompt(description, web_results)

        response = client.messages.create(
            model="claude-3-haiku-20240307",  # Use fast/cheap model for classification
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}]
        )

        # Parse response
        response_text = response.content[0].text.strip()

        # Extract JSON from response
        json_match = re.search(r'\{[^}]+\}', response_text)
        if json_match:
            result = json.loads(json_match.group())
            code = result.get('code', '').replace('.', '').replace(' ', '')

            if len(code) == 8 and code.isdigit():
                # Validate LLM code against CET before accepting
                code = validate_and_correct_code(code, base_dir)
                confidence = result.get('confidence', 0.75)

                # Reject low-confidence LLM classifications
                if confidence < 0.4:
                    logger.warning(
                        f"[LLM] Rejecting low-confidence classification "
                        f"({confidence}) for: {description[:50]}... -> {code}"
                    )
                    return None

                return {
                    'code': code,
                    'category': result.get('category', 'LLM_CLASSIFIED'),
                    'confidence': confidence,
                    'source': 'llm_classification',
                    'notes': result.get('reasoning', 'Classified by LLM'),
                }

        logger.warning(f"LLM response not parseable: {response_text[:100]}")

    except Exception as e:
        logger.warning(f"LLM classification failed: {e}")

    return None


def _classify_with_llm_urllib(description: str, web_results: str, api_key: str, base_url: str, model: str, config: Dict) -> Optional[Dict]:
    """Fallback LLM classification using urllib (no anthropic SDK needed)."""
    if not URLLIB_AVAILABLE:
        return None

    base_dir = config.get('base_dir', '.') if config else '.'

    try:
        prompt = _build_classification_prompt(description, web_results)

        request_data = json.dumps({
            "model": model,
            "max_tokens": 200,
            "messages": [{"role": "user", "content": prompt}]
        }).encode('utf-8')

        # Use the Z.AI endpoint (or direct Anthropic)
        api_endpoint = f"{base_url.rstrip('/')}/v1/messages"
        logger.debug(f"LLM classification request to: {api_endpoint}")

        req = Request(
            api_endpoint,
            data=request_data,
            headers={
                'Content-Type': 'application/json',
                'x-api-key': api_key,
                'anthropic-version': '2023-06-01',
            }
        )

        with urlopen(req, timeout=30) as response:
            result = json.loads(response.read().decode('utf-8'))

        response_text = result['content'][0]['text'].strip()
        logger.debug(f"LLM response: {response_text[:200]}")

        # Extract JSON from response
        json_match = re.search(r'\{[^}]+\}', response_text)
        if json_match:
            parsed = json.loads(json_match.group())
            code = parsed.get('code', '').replace('.', '').replace(' ', '')

            if len(code) == 8 and code.isdigit():
                # Validate LLM code against CET before accepting
                code = validate_and_correct_code(code, base_dir)
                confidence = parsed.get('confidence', 0.75)

                # Reject low-confidence LLM classifications
                if confidence < 0.4:
                    logger.warning(
                        f"[LLM] Rejecting low-confidence classification "
                        f"({confidence}) for: {description[:50]}... -> {code}"
                    )
                    return None

                return {
                    'code': code,
                    'category': parsed.get('category', 'LLM_CLASSIFIED'),
                    'confidence': confidence,
                    'source': 'llm_classification',
                    'notes': parsed.get('reasoning', 'Classified by LLM'),
                }

    except Exception as e:
        logger.warning(f"LLM urllib classification failed: {e}")

    return None


def run(input_path: str, output_path: str, config: Dict = None, context: Dict = None) -> Dict:
    """Classify items using rules and optional web lookup fallback."""
    if not input_path or not os.path.exists(input_path):
        return {'status': 'error', 'error': f'Input not found: {input_path}'}

    base_dir = context.get('base_dir', '.') if context else '.'
    rules_path = os.path.join(base_dir, 'rules', 'classification_rules.json')

    if not os.path.exists(rules_path):
        return {'status': 'error', 'error': f'Rules not found: {rules_path}'}

    with open(input_path) as f:
        data = json.load(f)

    with open(rules_path) as f:
        rules_data = json.load(f)

    rules = sorted(
        rules_data.get('rules', []),
        key=lambda r: r.get('priority', 0),
        reverse=True
    )
    noise_words = set(rules_data.get('word_analysis', {}).get('noise_words', []))

    items = data.get('items', data if isinstance(data, list) else [])
    classified = 0
    unmatched = 0
    bundle_inherited = 0

    # Build a SKU-to-classification map for inheritance
    sku_classifications = {}

    # First pass: Classify regular items (non-bundles)
    for item in items:
        is_bundle = item.get('is_bundle', False)
        if is_bundle:
            continue  # Skip bundles in first pass

        desc = item.get('description', '')
        match = classify_item(desc, rules, noise_words, base_dir)

        if match:
            item['classification'] = match
            classified += 1
            # Store classification by SKU for bundle inheritance
            sku = item.get('sku', '')
            if sku:
                sku_classifications[sku] = match
        else:
            # Try web lookup for unknown items
            web_match = lookup_hs_code_web(desc, config)
            if web_match:
                # Validate web lookup codes too
                web_match['code'] = validate_and_correct_code(web_match['code'], base_dir)
                item['classification'] = web_match
                classified += 1
                logger.info(f"Web lookup found: {desc[:50]}... -> {web_match['code']}")
                # Store for bundle inheritance
                sku = item.get('sku', '')
                if sku:
                    sku_classifications[sku] = web_match
            else:
                item['classification'] = {
                    'code': 'UNKNOWN',
                    'category': 'UNCLASSIFIED',
                    'confidence': 0,
                    'needs_review': True
                }
                unmatched += 1

    # Second pass: Classify bundles by inheriting from referenced items
    for item in items:
        is_bundle = item.get('is_bundle', False)
        if not is_bundle:
            continue

        desc = item.get('description', '')

        # Try to inherit classification from bundle references
        inherited = inherit_bundle_classification(item, sku_classifications)

        if inherited:
            # Validate inherited codes too
            inherited['code'] = validate_and_correct_code(inherited['code'], base_dir)
            item['classification'] = inherited
            item['classification']['inherited_from_bundle'] = True
            bundle_inherited += 1
            classified += 1
        else:
            # Fall back to regular classification
            match = classify_item(desc, rules, noise_words, base_dir)
            if match:
                item['classification'] = match
                classified += 1
            else:
                item['classification'] = {
                    'code': 'UNKNOWN',
                    'category': 'UNCLASSIFIED',
                    'confidence': 0,
                    'needs_review': True
                }
                unmatched += 1

    result = {
        'status': 'success',
        'items_classified': classified,
        'items_unmatched': unmatched,
        'total_items': len(items),
        'bundles_inherited': bundle_inherited,
    }

    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        output_data = data if isinstance(data, dict) else {'items': data}
        if isinstance(data, dict):
            output_data['items'] = items
        with open(output_path, 'w') as f:
            json.dump(output_data, f, indent=2)

    return result


def classify_item(description: str, rules: List[Dict], noise_words: set,
                   base_dir: str = '.', expected_chapter: str = None) -> Optional[Dict]:
    """Apply classification rules to an item description.

    Classification priority:
      Layer 0: Assessed customs entries (highest confidence — customs-verified)
      Layer 1: Classification rules (pattern matching)

    Args:
        description: Product description
        rules: Classification rules
        noise_words: Set of noise words to ignore
        base_dir: Project base directory
        expected_chapter: 2-digit chapter from LLM pre-assignment (Phase 1).
            Used to gate assessed lookups — rejects matches from wrong chapters.

    All returned codes are validated against the CARICOM CET to ensure they are
    valid end-node codes with DUTY RATE, UNIT, and SITC REV 4 data.
    """
    # WS-B4: lazily seed classifications.db on first classification.
    # Cheap no-op after the first call per process.
    try:
        from classification_db import ensure_db_seeded
        ensure_db_seeded(base_dir)
    except Exception as _e:
        logger.debug(f"[DB-SEED] ensure_db_seeded failed: {_e}")

    # ── Layer 0: Assessed classifications (customs-verified) ──
    assessed_result = lookup_assessed_classification(description, base_dir,
                                                      expected_chapter=expected_chapter)
    if assessed_result and assessed_result.get('code') and assessed_result['code'] != 'UNKNOWN':
        logger.info(f"[ASSESSED] {description[:50]}... -> {assessed_result['code']} "
                     f"({assessed_result['source']})")
        return assessed_result

    # ── Layer 1: Rule-based classification ──
    desc_upper = description.upper()

    for rule in rules:
        patterns = rule.get('patterns', [])
        exclude = rule.get('exclude', [])

        excluded = False
        for excl in exclude:
            if excl.upper() in desc_upper:
                excluded = True
                break

        if excluded:
            continue

        for pattern in patterns:
            if pattern.upper() in desc_upper:
                code = validate_and_correct_code(rule['code'], base_dir)
                return {
                    'code': code,
                    'category': rule.get('category', 'PRODUCTS'),
                    'confidence': rule.get('confidence', 0.8),
                    'rule_id': rule.get('id'),
                    'notes': rule.get('notes'),
                }

    return None


def inherit_bundle_classification(item: Dict, sku_classifications: Dict) -> Optional[Dict]:
    """
    Try to inherit classification for a bundle item from its referenced items.

    Bundles like ST-MFHG should inherit classification from MFHG01, MFHG02, etc.
    """
    import re

    sku = item.get('sku', '')
    bundle_refs = item.get('bundle_references', [])

    # Try explicit references first
    for ref in bundle_refs:
        if ref.endswith('*'):
            # Wildcard reference like "MFHG*" - find any matching SKU
            prefix = ref[:-1]
            for classified_sku, classification in sku_classifications.items():
                if classified_sku.startswith(prefix):
                    return classification.copy()
        elif ref in sku_classifications:
            return sku_classifications[ref].copy()

    # Try to derive reference from bundle SKU (e.g., ST-MFHG -> MFHG01)
    sku_upper = sku.upper()
    for prefix in ['ST-', 'DP-', 'TST-', 'T-']:
        if sku_upper.startswith(prefix):
            base_code = sku_upper[len(prefix):]
            # Look for items starting with this base code
            for classified_sku, classification in sku_classifications.items():
                if classified_sku.upper().startswith(base_code):
                    return classification.copy()
            break

    return None
