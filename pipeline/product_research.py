"""
Product research module: fetches product details from supplier websites
(e.g. Budget Marine) to provide richer context for tariff classification.

Caches results to disk so repeated lookups are free.

Usage:
    from product_research import research_product, research_items_batch
    info = research_product(sku='SHD/833', description='Pole Handle, 6ft Telescopic')
    # Returns: {'name': ..., 'category_path': ..., 'details': ..., 'materials': ..., 'url': ...}
"""

import hashlib
import json
import logging
import os
import re
import time
from typing import Dict, List, Optional
from urllib.parse import quote

logger = logging.getLogger(__name__)

# ── Cache ────────────────────────────────────────────────────────────────────
CACHE_DIR = None  # Set on first call


def _cache_dir(base_dir: str = '.') -> str:
    global CACHE_DIR
    if CACHE_DIR is None:
        CACHE_DIR = os.path.join(base_dir, 'data', 'product_cache')
        os.makedirs(CACHE_DIR, exist_ok=True)
    return CACHE_DIR


def _cache_key(sku: str, description: str) -> str:
    raw = f"{sku}|{description}".lower().strip()
    return hashlib.md5(raw.encode()).hexdigest()


def _read_cache(key: str, base_dir: str = '.') -> Optional[dict]:
    path = os.path.join(_cache_dir(base_dir), f"{key}.json")
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            return None
    return None


def _write_cache(key: str, data: dict, base_dir: str = '.'):
    path = os.path.join(_cache_dir(base_dir), f"{key}.json")
    try:
        with open(path, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.debug(f"Cache write failed: {e}")


# ── URL builders ─────────────────────────────────────────────────────────────

def _slug_from_description(description: str) -> str:
    """Convert product description to URL slug."""
    slug = description.lower()
    slug = re.sub(r'[^a-z0-9\s-]', '', slug)
    slug = re.sub(r'\s+', '-', slug.strip())
    slug = re.sub(r'-+', '-', slug)
    return slug[:80]


def _build_search_url(query: str) -> str:
    """Build Budget Marine search URL."""
    return f"https://budgetmarine.com/search?q={quote(query)}"


def _build_product_urls(sku: str, description: str) -> List[str]:
    """Generate candidate URLs for a product."""
    urls = []

    # Try search by description keywords (most reliable)
    keywords = re.sub(r'[,/\-"]', ' ', description).split()
    # Use first 3-4 meaningful words
    search_words = [w for w in keywords if len(w) > 2 and not w.isdigit()][:4]
    if search_words:
        query = '+'.join(search_words)
        urls.append(_build_search_url(query))

    return urls


# ── Web fetching ─────────────────────────────────────────────────────────────

def _fetch_product_page(url: str) -> Optional[dict]:
    """Fetch and parse a Budget Marine product page."""
    try:
        import requests
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; AutoInvoice/2.0; product-research)',
            'Accept': 'text/html,application/xhtml+xml',
        }
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return None
        return {'html': resp.text, 'url': url}
    except Exception as e:
        logger.debug(f"Fetch failed for {url}: {e}")
        return None


def _parse_product_info_with_llm(html: str, sku: str, description: str) -> Optional[dict]:
    """Use LLM to extract structured product info from HTML."""
    try:
        from core.llm_client import get_llm_client
        llm = get_llm_client()
    except Exception:
        return None

    # Truncate HTML to avoid token limits
    html_truncated = html[:8000]

    prompt = f"""Extract product information from this Budget Marine webpage HTML.
I'm looking for the product: SKU={sku}, Description="{description}"

Extract and return JSON:
{{
  "found": true/false,
  "name": "full product name",
  "category_path": "Catalog > Category > Subcategory > ...",
  "brand": "manufacturer brand",
  "materials": "what materials it's made of",
  "product_type": "what kind of product this is (e.g., 'hand-held electric power tool', 'chemical cleaning product', 'stainless steel fitting')",
  "function": "what the product does / what it's used for",
  "specifications": "key specs (voltage, size, weight, etc.)",
  "marine_context": "how this product is used in a marine/boat context"
}}

If the product is NOT found on the page, set found=false and leave other fields empty.

HTML:
{html_truncated}"""

    try:
        result = llm.call_json(
            user_message=prompt,
            system_prompt="You extract structured product data from web pages. Return only valid JSON.",
            max_tokens=1024,
            cache_key_extra=f"product_research:{sku}",
        )
        return result if result and result.get('found') else None
    except Exception as e:
        logger.debug(f"LLM parse failed: {e}")
        return None


# ── Public API ───────────────────────────────────────────────────────────────

def research_product(sku: str, description: str, base_dir: str = '.') -> Optional[dict]:
    """
    Research a product by fetching its details from Budget Marine website.

    Returns dict with keys: name, category_path, brand, materials, product_type,
    function, specifications, marine_context, url.
    Returns None if product not found.
    """
    key = _cache_key(sku, description)

    # Check cache first
    cached = _read_cache(key, base_dir)
    if cached is not None:
        return cached if cached.get('found') else None

    # Try fetching from web
    urls = _build_product_urls(sku, description)

    for url in urls:
        page = _fetch_product_page(url)
        if not page:
            continue

        info = _parse_product_info_with_llm(page['html'], sku, description)
        if info and info.get('found'):
            info['url'] = url
            _write_cache(key, info, base_dir)
            logger.info(f"[RESEARCH] Found: {sku} -> {info.get('name', '')[:50]} "
                        f"({info.get('category_path', '')})")
            return info

    # Cache negative result to avoid re-fetching
    _write_cache(key, {'found': False, 'sku': sku}, base_dir)
    return None


def research_items_batch(items: List[dict], base_dir: str = '.',
                         max_items: int = 100, delay: float = 1.0) -> Dict[str, dict]:
    """
    Research multiple items. Returns {sku: product_info} dict.

    Args:
        items: List of dicts with 'sku' and 'description' keys
        base_dir: Project base directory
        max_items: Maximum number of web lookups to perform
        delay: Seconds between web requests to avoid rate limiting
    """
    results = {}
    web_lookups = 0

    for item in items:
        sku = item.get('sku', '')
        desc = item.get('description', '') or item.get('supp_desc', '')
        if not sku or not desc:
            continue

        # Check cache first (doesn't count as web lookup)
        key = _cache_key(sku, desc)
        cached = _read_cache(key, base_dir)
        if cached is not None:
            if cached.get('found'):
                results[sku] = cached
            continue

        # Web lookup
        if web_lookups >= max_items:
            break

        info = research_product(sku, desc, base_dir)
        if info:
            results[sku] = info

        web_lookups += 1
        if delay > 0 and web_lookups < max_items:
            time.sleep(delay)

    logger.info(f"[RESEARCH] Batch: {len(results)} found, {web_lookups} web lookups")
    return results


def enrich_classification_context(description: str, sku: str = '',
                                   product_info: dict = None) -> str:
    """
    Build an enriched description for classification by combining
    the item description with researched product information.

    Returns an enhanced description string for the LLM classifier.
    """
    if not product_info:
        return description

    parts = [description]

    if product_info.get('product_type'):
        parts.append(f"[Type: {product_info['product_type']}]")
    if product_info.get('materials'):
        parts.append(f"[Material: {product_info['materials']}]")
    if product_info.get('function'):
        parts.append(f"[Function: {product_info['function']}]")
    if product_info.get('category_path'):
        parts.append(f"[Store category: {product_info['category_path']}]")
    if product_info.get('brand'):
        parts.append(f"[Brand: {product_info['brand']}]")

    return ' '.join(parts)
