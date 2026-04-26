"""
Auto-generate format specs for unmatched invoices using LLM.

Called by process_single_invoice when no format matches.
Generates a YAML spec, saves to _auto/, retries parsing with it.

Batch-aware: generates once per supplier, caches for reuse.
"""

import logging
import os
import re
import sys
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Ensure pipeline directory is on path
PIPELINE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PIPELINE_DIR not in sys.path:
    sys.path.insert(0, PIPELINE_DIR)

# In-memory cache: supplier_name -> spec_path (or None = failed, don't retry)
# Prevents duplicate LLM calls for same supplier within one batch run.
_batch_spec_cache: Dict[str, Optional[str]] = {}


# Phrases that appear in invoice payment / totals footers, NOT in real
# product descriptions.  When an LLM-generated spec extracts items whose
# descriptions match these labels, the spec almost certainly grabbed the
# totals section instead of the actual line items (HAWB9421183 incident:
# "Order Total", "Item(s) Subtotal", "Estimated tax", "Grand Total" all
# emitted as bogus item rows).  Used by `_items_look_bogus` to reject the
# spec before it pollutes the XLSX.
_FOOTER_LABEL_RE = re.compile(
    r'(?i)\b('
    r'grand\s*tot(?:al|ak)'        # "Grand Total" / OCR variant "Grand Totak"
    r'|sub.?total'                 # subtotal / sub-total
    r'|estimated\s*tax'            # "Estimated tax to be collected"
    r'|order\s*total'              # "Order Total"
    r'|total\s*before\s*tax'       # "Total before tax"
    r'|item\s*\(\s*s\s*\)\s*subtotal'  # "Item(s) Subtotal"
    r'|payment\s*method'           # "Payment Method"
    r'|shipping\s*(?:&|and)\s*handling'  # "Shipping & Handling"
    r')\b'
)


def _items_look_bogus(items: List[Dict]) -> Optional[str]:
    """Return a reason string if items look like extracted footer junk,
    or None if they look like real products.

    Heuristics (any one triggers rejection):
      - 50%+ of descriptions match a known totals/footer label
      - 50%+ of items have quantity == 0 AND price > 0 (real items have qty)
    """
    if not items:
        return None
    n = len(items)
    footer_hits = sum(
        1 for it in items
        if _FOOTER_LABEL_RE.search(str(it.get('description') or it.get('supplier_item_desc') or ''))
    )
    if footer_hits and footer_hits >= max(1, (n + 1) // 2):
        return f"{footer_hits}/{n} item descriptions match totals-section labels"

    zero_qty_priced = sum(
        1 for it in items
        if (it.get('quantity') or 0) == 0
        and ((it.get('total_cost') or 0) > 0 or (it.get('unit_price') or 0) > 0)
    )
    if zero_qty_priced and zero_qty_priced >= max(1, (n + 1) // 2):
        return f"{zero_qty_priced}/{n} items have quantity=0 with a price (footer values, not items)"

    return None


def reset_batch_cache():
    """Clear the batch cache. Call at start of each pipeline run."""
    _batch_spec_cache.clear()


def try_auto_generate(
    text: str,
    registry,
    supplier_name: str = '',
    pdf_file: str = '',
) -> Optional[Tuple[Dict, str]]:
    """
    Attempt to auto-generate a format spec for unmatched invoice text.

    Args:
        text: Extracted PDF text
        registry: FormatRegistry instance (will be mutated via register_spec)
        supplier_name: Detected supplier name (used for caching and spec naming)
        pdf_file: PDF filename for logging

    Returns:
        Tuple of (parsed_invoice_data, spec_path) on success, None on failure.
    """
    from workflow.format_spec_generator import generate_format_spec, discard_spec
    from format_parser import create_parser
    from stages.supplier_resolver import normalize_parse_result

    cache_key = supplier_name.lower().strip() if supplier_name else ''

    # If no supplier name provided, try to detect from text for caching
    if not cache_key:
        cache_key = _detect_supplier_key(text)

    # 1. Check batch cache — already generated/failed for this supplier?
    if cache_key and cache_key in _batch_spec_cache:
        cached_path = _batch_spec_cache[cache_key]
        if cached_path is None:
            logger.debug(f"Skipping auto-gen for {cache_key}: previously failed this batch")
            return None
        # Previously succeeded — try parsing with cached spec
        result = _try_parse_with_spec_file(cached_path, text)
        if result:
            return result

    # 2. Check if _auto/ already has a spec for this supplier from a prior run
    existing_path = _find_existing_auto_spec(cache_key)
    if existing_path:
        result = _try_parse_with_spec_file(existing_path, text)
        if result:
            if cache_key:
                _batch_spec_cache[cache_key] = existing_path
            print(f"    Reusing auto-generated spec: {os.path.basename(existing_path)}")
            return result
        # Existing spec didn't work — fall through to regenerate

    # 3. Generate new spec via LLM
    print(f"    Auto-generating format spec for: {supplier_name or cache_key or pdf_file}...")
    spec_result = generate_format_spec(
        invoice_text=text,
        detected_supplier=supplier_name or cache_key or None,
    )

    if not spec_result or not spec_result.get('success'):
        error = spec_result.get('error', 'Unknown error') if spec_result else 'No result'
        print(f"    Auto-gen failed: {error}")
        if cache_key:
            _batch_spec_cache[cache_key] = None
        return None

    spec_path = spec_result['spec_path']
    spec_data = spec_result['spec_data']
    format_name = spec_result['format_name']

    # 4. Hot-load into registry for future detection
    if hasattr(registry, 'register_spec'):
        registry.register_spec(spec_data, source_label='auto')

    # 5. Try parsing with the new spec
    try:
        parser = create_parser(spec_data)
        raw_result = parser.parse(text)
    except Exception as e:
        logger.warning(f"Auto-generated parser failed: {e}")
        discard_spec(spec_path)
        if cache_key:
            _batch_spec_cache[cache_key] = None
        return None

    # 6. Validate: did it actually extract items?
    invoice_data = normalize_parse_result(raw_result)
    items = invoice_data.get('items', [])

    if len(items) == 0:
        print(f"    Auto-generated spec produced 0 items — discarding")
        discard_spec(spec_path)
        if cache_key:
            _batch_spec_cache[cache_key] = None
        return None

    bogus_reason = _items_look_bogus(items)
    if bogus_reason:
        print(f"    Auto-generated spec rejected: {bogus_reason}")
        discard_spec(spec_path)
        if cache_key:
            _batch_spec_cache[cache_key] = None
        return None

    print(f"    Auto-generated spec '{format_name}' extracted {len(items)} items")
    if cache_key:
        _batch_spec_cache[cache_key] = spec_path

    # Tag for later promotion
    invoice_data['_auto_spec_path'] = spec_path
    return (invoice_data, spec_path)


def _detect_supplier_key(text: str) -> str:
    """Detect a cache key from invoice text for batch deduplication."""
    import re

    # Try common supplier identification patterns
    patterns = [
        (r'Budget\s*Marine', 'budget_marine'),
        (r'SHEIN', 'shein'),
        (r'TEMU', 'temu'),
        (r'Amazon\.com|AMAZON', 'amazon'),
        (r'Walmart', 'walmart'),
        (r'West\s*Marine', 'west_marine'),
    ]
    for pattern, key in patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return key

    # Try first line of the PDF — many invoices start with the company name
    # before any labels like "INVOICE", "BILL TO", etc.
    first_lines = text[:500].strip().split('\n')
    for line in first_lines[:3]:
        line = line.strip()
        # Skip generic labels
        if re.match(r'^(INVOICE|BILL\s+TO|SHIP\s+TO|DATE|QTY|DESCRIPTION|TEL|FAX|EMAIL)', line, re.IGNORECASE):
            continue
        # A company name is typically 2+ words, all caps or title case, no digits-only
        if len(line) > 4 and not line.isdigit() and re.match(r'^[A-Z][A-Za-z\s&\',\.\-]{3,40}$', line):
            name = re.sub(r'[^a-z0-9]', '_', line.strip().lower()).strip('_')
            if name and len(name) > 3:
                return name[:30]

    # Try "Sold by" / "From" / company name in first 500 chars
    header = text[:500]
    match = re.search(
        r'(?:Sold\s+by|Supplier|From|Ship\s+from|Bill\s+To)[:\s]+([A-Za-z0-9][A-Za-z0-9\s]{2,30})',
        header, re.IGNORECASE
    )
    if match:
        name = re.sub(r'[^a-z0-9]', '_', match.group(1).strip().lower()).strip('_')
        return name[:30] if name else ''

    return ''


def _find_existing_auto_spec(supplier_key: str) -> Optional[str]:
    """Check if _auto/ already has a spec matching this supplier."""
    if not supplier_key:
        return None
    try:
        from core.config import get_config
        cfg = get_config()
        auto_dir = cfg.auto_formats_dir
    except Exception:
        return None

    if not os.path.isdir(auto_dir):
        return None

    for ext in ('.yaml', '.yml'):
        candidate = os.path.join(auto_dir, f"{supplier_key}{ext}")
        if os.path.isfile(candidate):
            return candidate
    return None


def _try_parse_with_spec_file(
    spec_path: str,
    text: str,
) -> Optional[Tuple[Dict, str]]:
    """Try parsing text with an existing auto-generated spec file."""
    try:
        import yaml
        with open(spec_path, 'r', encoding='utf-8') as f:
            spec_data = yaml.safe_load(f)
        if not spec_data or not isinstance(spec_data, dict):
            return None
    except Exception:
        return None

    from format_parser import create_parser
    from stages.supplier_resolver import normalize_parse_result

    try:
        parser = create_parser(spec_data)
        raw_result = parser.parse(text)
    except Exception:
        return None

    invoice_data = normalize_parse_result(raw_result)
    items = invoice_data.get('items', [])
    if len(items) == 0:
        return None

    bogus_reason = _items_look_bogus(items)
    if bogus_reason:
        logger.info(
            f"Cached auto-spec rejected ({os.path.basename(spec_path)}): "
            f"{bogus_reason}"
        )
        return None

    invoice_data['_auto_spec_path'] = spec_path
    return (invoice_data, spec_path)
