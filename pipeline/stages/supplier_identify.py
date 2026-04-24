"""
Supplier identification via in-app vision LLM and reverse image search.

Two-tier orchestrator used by ``supplier_resolver.get_supplier_info`` as
an intermediate step between the ``suppliers.json`` DB lookup (cheap,
deterministic) and the DuckDuckGo address fallback (text only). Fires
only when invoice-text extraction plus the DB lookup yield no supplier
name — i.e. the format YAML left ``supplier_name`` blank and no
``invoice_data.supplier_name`` exists.

Tier 1 — Vision LLM brand recognition (``identify_from_logo_vision``)
    Sends the first page of the invoice PDF to the Anthropic-compatible
    vision endpoint already configured in ``pipeline.multi_ocr`` and
    asks it to identify the merchant/brand from any visible logo,
    letterhead, customer-service number, or branding text. Fast and
    cheap for famous brands (H&M, Fashion Nova, Temu, Walmart, Amazon,
    SHEIN, etc.) which are well represented in the vision model's
    training set. Returns a canonical brand name.

Tier 3 — Reverse image search (``reverse_image_search``)
    Fallback for obscure merchants the vision model can't name. Uses
    SerpAPI's ``google_lens`` engine (requires ``SERPAPI_API_KEY``).
    The first-page PNG is briefly uploaded to tmpfiles.org (anonymous,
    24 h retention) so SerpAPI can fetch it by URL, and the most common
    ``source`` domain among visual matches seeds the brand name via a
    small curated map.

Both tiers degrade cleanly when API keys are missing — the caller falls
through to the existing DuckDuckGo address-only fallback. All results
(including negatives) are cached to
``workspace/_cache/brand/{pdf_fingerprint}.json`` so re-processing a
shipment never pays a second API call. Delete the cache file to retry.
"""

from __future__ import annotations

import base64
import hashlib
import io
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_CACHE_SUBDIR = os.path.join("workspace", "_cache", "brand")


# ── Image rendering ───────────────────────────────────────────

def _render_first_page_png(pdf_path: str, dpi: int = 150) -> Optional[bytes]:
    """Rasterize page 1 of the PDF to PNG bytes. Returns None if
    PyMuPDF isn't available or the file can't be opened."""
    try:
        import fitz  # type: ignore
    except ImportError:
        logger.debug("PyMuPDF not installed; cannot render PDF for brand detect")
        return None
    try:
        doc = fitz.open(pdf_path)
        if doc.page_count == 0:
            doc.close()
            return None
        page = doc.load_page(0)
        mat = fitz.Matrix(dpi / 72, dpi / 72)
        pix = page.get_pixmap(matrix=mat)
        png = pix.tobytes("png")
        doc.close()
        return png
    except Exception as e:
        logger.debug(f"page render failed for {pdf_path}: {e}")
        return None


# ── Cache ─────────────────────────────────────────────────────

def _cache_dir() -> Path:
    try:
        from pipeline.stages import supplier_resolver  # type: ignore
    except ImportError:
        from stages import supplier_resolver  # type: ignore
    root = Path(supplier_resolver._get_base_dir())
    d = root / _CACHE_SUBDIR
    d.mkdir(parents=True, exist_ok=True)
    return d


def _pdf_fingerprint(pdf_path: str) -> str:
    """SHA-256 over the first 64 KiB of the PDF — enough to disambiguate
    different shipments without paying to hash a whole multi-page PDF."""
    h = hashlib.sha256()
    with open(pdf_path, "rb") as f:
        h.update(f.read(65536))
    return h.hexdigest()[:32]


def _cache_get(key: str) -> Optional[Dict[str, Any]]:
    p = _cache_dir() / f"{key}.json"
    if not p.is_file():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _cache_put(key: str, data: Dict[str, Any]) -> None:
    p = _cache_dir() / f"{key}.json"
    try:
        p.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except Exception as e:
        logger.debug(f"brand cache write failed: {e}")


# ── Tier 1 — Vision LLM brand recognition ─────────────────────

_VISION_PROMPT = (
    "You are looking at the first page of an invoice or receipt. Your job "
    "is to identify the merchant / brand / supplier STRICTLY from what is "
    "actually visible on the page — a logo, letterhead, header, footer, "
    "customer-service phone number, return-address, domain name, or "
    "branded wordmark. Do NOT guess. Do NOT rely on document layout or "
    "product categories alone — many merchants share similar invoice "
    "templates. If no brand text or logo is visible, return brand=\"\".\n\n"
    "Before answering, internally locate the specific visible element "
    "(logo wordmark, domain in footer, phone number, etc.) that "
    "identifies the brand. Echo it verbatim in the \"evidence\" field. "
    "If you cannot point to a concrete on-page element, you MUST return "
    'brand="" with confidence="low".\n\n'
    "Return canonical brand names (e.g., 'H&M' not 'Hennes & Mauritz'; "
    "'Fashion Nova'; 'Temu'; 'SHEIN'; 'Walmart'; 'Amazon').\n\n"
    "Return ONLY a JSON object with exactly these keys:\n"
    '  "brand": canonical brand name, or "" if not clearly visible\n'
    '  "evidence": the exact text / wordmark / domain / phone you saw '
    'on the page that justifies the brand (quote verbatim), or "" if none\n'
    '  "confidence": one of "high", "medium", "low" — use "high" ONLY '
    'when a clear logo/wordmark is visible and unambiguous\n'
    '  "country_code": 2-letter ISO code of the merchant\'s primary '
    'country (e.g., US, CN, GB, SE), or "" if unknown\n'
    '  "visible_address": any supplier/merchant address printed on '
    'the page, or "" if none\n'
    "No commentary, no code fences — JSON only."
)


def _parse_vision_json(text: str) -> Dict[str, Any]:
    """Extract a JSON object from an LLM response that may be wrapped
    in code fences or surrounded by prose.

    Scans for the FIRST balanced ``{ ... }`` block and attempts to parse
    it. Tolerates models that loop on formatting and emit several
    partial copies of the JSON — we only need the first complete one.
    If no complete block is found, attempts a lenient key-extraction
    on the first fragment so partial responses still surface the
    brand name etc.
    """
    if not text:
        return {}
    cleaned = text.strip()
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s*```$", "", cleaned)

    # Find the first balanced JSON object via brace tracking.
    start = cleaned.find("{")
    while start != -1:
        depth = 0
        in_str = False
        escape = False
        for i in range(start, len(cleaned)):
            ch = cleaned[i]
            if escape:
                escape = False
                continue
            if ch == "\\" and in_str:
                escape = True
                continue
            if ch == '"':
                in_str = not in_str
                continue
            if in_str:
                continue
            if ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    candidate = cleaned[start:i + 1]
                    try:
                        return json.loads(candidate)
                    except json.JSONDecodeError:
                        break  # try next '{'
        start = cleaned.find("{", start + 1)

    # Lenient fallback: pull "key": "value" pairs out of whatever fragment
    # we got. Useful when the model truncated mid-address with max_tokens.
    out: Dict[str, Any] = {}
    for key in ("brand", "evidence", "country_code", "address",
                "visible_address", "confidence"):
        m = re.search(rf'"{key}"\s*:\s*"([^"]*)"', cleaned)
        if m:
            out[key] = m.group(1)
    return out


def _resolve_api_key() -> str:
    """Mirror ``pipeline.multi_ocr._resolve_api_key`` but avoid a hard
    import cycle if multi_ocr is unavailable in a minimal test env."""
    try:
        try:
            from pipeline.multi_ocr import _resolve_api_key as _mo_resolve  # type: ignore
        except ImportError:
            from multi_ocr import _resolve_api_key as _mo_resolve  # type: ignore
        return _mo_resolve()
    except Exception:
        return os.environ.get("ANTHROPIC_API_KEY") or \
               os.environ.get("ZAI_API_KEY", "")


def identify_from_logo_vision(pdf_path: str) -> Dict[str, Any]:
    """Tier 1: ask the in-app vision LLM to identify the merchant from
    the first-page image.

    Returns a dict ``{name, country_code, address, confidence, source}``
    or ``{}`` on any failure (missing key, render error, SDK missing,
    unparseable response, blank brand)."""
    api_key = _resolve_api_key()
    if not api_key:
        return {}

    png = _render_first_page_png(pdf_path)
    if not png:
        return {}

    base_url = os.environ.get("ANTHROPIC_BASE_URL",
                              "https://api.z.ai/api/anthropic")
    model = os.environ.get("VISION_MODEL", "glm-4.7")
    b64 = base64.b64encode(png).decode("utf-8")

    try:
        from anthropic import Anthropic  # type: ignore
    except ImportError:
        logger.debug("anthropic SDK not installed — skipping vision brand ID")
        return {}

    try:
        client = Anthropic(api_key=api_key, base_url=base_url)
        response = client.messages.create(
            model=model,
            max_tokens=400,
            messages=[{
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/png",
                            "data": b64,
                        },
                    },
                    {"type": "text", "text": _VISION_PROMPT},
                ],
            }],
        )
        raw = response.content[0].text if response.content else ""
    except Exception as e:
        logger.debug(f"vision brand-id call failed: {e}")
        return {}

    parsed = _parse_vision_json(raw)
    brand = (parsed.get("brand") or "").strip()
    if not brand:
        return {}
    return {
        "name": brand,
        "country_code": (parsed.get("country_code") or "").strip().upper()[:2],
        "address": (parsed.get("visible_address") or "").strip(),
        "confidence": (parsed.get("confidence") or "medium").lower(),
        "source": "vision",
    }


# ── Tier 2 — OCR-text → in-app LLM brand resolution ───────────
#
# Vision logo identification on Z.AI glm-4.7 hallucinates badly on
# scanned receipts (returns "Temu"/"Uber"/"Tiger Airways" for H&M).
# The regular pipeline OCR (tesseract/paddleocr via
# ``extract_pdf_text``) reliably captures the brand wordmark as plain
# text — e.g. "H&M" ends up on line 0 of the OCR output.  This tier
# feeds that OCR text to the in-app text LLM and asks it to identify
# the merchant and supply its canonical name, country, and registered
# address from its training-set world-knowledge.  No web call required.

_OCR_BRAND_PROMPT = (
    "You are given the OCR text from the first page of an invoice or "
    "receipt.  Identify the merchant / brand / supplier based STRICTLY "
    "on text that is actually present in the OCR (logos often appear "
    "as plain text like 'H&M', 'TEMU', 'SHEIN', etc.).  Do not guess.\n\n"
    "This data is used for CARICOM customs brokerage, so we need the "
    "**country and address the goods actually shipped FROM**, not the "
    "brand's global headquarters.  For a North-American consumer "
    "receipt paid in USD, this is almost always the brand's US "
    "operating-company address (fulfilment centre or US HQ), not the "
    "parent company's registered office.  Examples of the correct "
    "'ship-from' address to return:\n"
    "  - H&M (US receipt, USD): 'H & M Hennes & Mauritz L.P., 1600 "
    "River Road, Burlington, NJ 08016, USA', country_code='US'.\n"
    "  - Fashion Nova: 'Fashion Nova Inc., 2801 E 46th St, Vernon, CA "
    "90058, USA', country_code='US'.\n"
    "  - Temu / Shein (Chinese-fulfilled): CN address, country_code='CN'.\n"
    "Only fall back to the global HQ when the receipt clearly shipped "
    "internationally (non-USD currency, foreign address on doc, etc.).\n\n"
    "Return ONLY a JSON object with these keys:\n"
    '  "brand": canonical brand name (e.g. "H&M", "Fashion Nova"), '
    'or "" if not identifiable from the OCR text\n'
    '  "evidence": the exact substring from the OCR text that '
    'identifies the brand (quote verbatim), or "" if none\n'
    '  "country_code": 2-letter ISO country code of the '
    'ship-from country (US / CN / etc.), or "" if unknown\n'
    '  "address": full ship-from address (US operating company for US '
    'receipts), or "" if unknown\n'
    '  "confidence": one of "high", "medium", "low"\n'
    "No commentary, no code fences — JSON only.\n\n"
    "OCR TEXT:\n"
    "----------\n"
    "{ocr_text}\n"
    "----------"
)


def identify_from_ocr_text(pdf_path: str) -> Dict[str, Any]:
    """Tier 2: run the pipeline's text OCR on page 1, then ask the
    in-app text LLM to identify the merchant and fill in canonical
    name/country/address from its world-knowledge.

    This is more reliable than Tier 1 vision for scanned receipts
    because pipeline OCR (tesseract/paddleocr) captures wordmark text
    like 'H&M' deterministically, and the text LLM recognises well-known
    brand names without hallucinating the way the vision endpoint does.

    Returns ``{name, country_code, address, confidence, source}`` or
    ``{}`` on failure."""
    api_key = _resolve_api_key()
    if not api_key:
        return {}

    # Render the page and run the pipeline OCR exactly as the rest of
    # the extraction flow does, so we work from the same text the format
    # parser sees.
    try:
        try:
            from pipeline.stages.supplier_resolver import extract_pdf_text  # type: ignore
        except ImportError:
            from stages.supplier_resolver import extract_pdf_text  # type: ignore
    except Exception as e:
        logger.debug(f"extract_pdf_text import failed: {e}")
        return {}
    try:
        ocr_text = extract_pdf_text(pdf_path)
    except Exception as e:
        logger.debug(f"OCR failed for {pdf_path}: {e}")
        return {}
    if not ocr_text or not ocr_text.strip():
        return {}

    # Keep the prompt small: send the first ~1500 chars (header always
    # contains the brand wordmark).
    snippet = ocr_text[:1500]

    try:
        from anthropic import Anthropic  # type: ignore
    except ImportError:
        logger.debug("anthropic SDK not installed — skipping OCR-text brand ID")
        return {}

    base_url = os.environ.get("ANTHROPIC_BASE_URL",
                              "https://api.z.ai/api/anthropic")
    model = os.environ.get("BRAND_TEXT_MODEL",
                           os.environ.get("VISION_MODEL", "glm-4.7"))

    try:
        client = Anthropic(api_key=api_key, base_url=base_url)
        response = client.messages.create(
            model=model,
            max_tokens=800,
            messages=[{
                "role": "user",
                "content": _OCR_BRAND_PROMPT.format(ocr_text=snippet),
            }],
        )
        raw = response.content[0].text if response.content else ""
    except Exception as e:
        logger.debug(f"OCR-text brand-id call failed: {e}")
        return {}

    parsed = _parse_vision_json(raw)
    brand = (parsed.get("brand") or "").strip()
    if not brand:
        return {}
    return {
        "name": brand,
        "country_code": (parsed.get("country_code") or "").strip().upper()[:2],
        "address": (parsed.get("address") or "").strip(),
        "confidence": (parsed.get("confidence") or "medium").lower(),
        "source": "ocr_text_llm",
    }


# ── Tier 3 — Reverse image search (SerpAPI Google Lens) ────────

# Curated domain → canonical brand map for the brands we see most in
# the CARICOM broker corpus. Falls through to a Title-cased domain
# label if the domain isn't listed — callers can edit suppliers.json
# afterwards to normalise.
_DOMAIN_TO_BRAND = {
    "fashionnova": "Fashion Nova",
    "hm": "H&M",
    "handm": "H&M",
    "hennes-mauritz": "H&M",
    "amazon": "Amazon",
    "walmart": "Walmart",
    "temu": "Temu",
    "shein": "SHEIN",
    "alibaba": "Alibaba",
    "aliexpress": "AliExpress",
    "dhgate": "DHgate",
    "target": "Target",
    "macys": "Macy's",
    "nordstrom": "Nordstrom",
    "bestbuy": "Best Buy",
    "nike": "Nike",
    "adidas": "Adidas",
    "zara": "Zara",
    "uniqlo": "UNIQLO",
}


def _brand_from_domain(domain: str) -> str:
    """Convert a hostname like ``www.fashionnova.com`` into a canonical
    brand name. Unknown domains fall back to a Title-cased label."""
    if not domain:
        return ""
    d = re.sub(r"^https?://", "", domain).strip("/")
    d = re.sub(r"^www\.", "", d, flags=re.IGNORECASE)
    d = d.split("/")[0]
    label = d.split(".")[0].lower()
    if label in _DOMAIN_TO_BRAND:
        return _DOMAIN_TO_BRAND[label]
    return label.replace("-", " ").replace("_", " ").title()


def _host_image_ephemeral(png_bytes: bytes) -> Optional[str]:
    """Upload a PNG to a free ephemeral image host so SerpAPI can fetch
    it by URL. Uses tmpfiles.org (anonymous, 24 h retention). Returns
    the direct-download URL, or None if the upload fails — the caller
    treats None as a reason to skip the reverse-image tier entirely
    rather than leaking a filesystem path."""
    try:
        import requests  # type: ignore
    except ImportError:
        return None
    try:
        files = {"file": ("logo.png", io.BytesIO(png_bytes), "image/png")}
        resp = requests.post("https://tmpfiles.org/api/v1/upload",
                             files=files, timeout=15)
        if resp.status_code != 200:
            return None
        data = resp.json()
        url = (data.get("data") or {}).get("url", "")
        if not url:
            return None
        # tmpfiles.org returns the viewer URL; the raw file is served
        # from the /dl/ path.
        return url.replace("tmpfiles.org/", "tmpfiles.org/dl/")
    except Exception:
        return None


def reverse_image_search(pdf_path: str) -> Dict[str, Any]:
    """Tier 3 fallback: reverse-image-search the first-page PNG via
    SerpAPI Google Lens (requires ``SERPAPI_API_KEY``). Aggregates the
    top ``visual_matches`` by ``source`` domain and translates the most
    common domain to a brand name via ``_DOMAIN_TO_BRAND``.

    Returns a dict ``{name, country_code, address, confidence, source}``
    or ``{}`` when the key is missing, the upload fails, or no matches
    come back."""
    api_key = os.environ.get("SERPAPI_API_KEY") or \
              os.environ.get("SERPAPI_KEY")
    if not api_key:
        return {}
    png = _render_first_page_png(pdf_path, dpi=120)
    if not png:
        return {}

    try:
        hosted_url = _host_image_ephemeral(png)
    except Exception as e:
        logger.debug(f"ephemeral image host failed: {e}")
        return {}
    if not hosted_url:
        return {}

    from urllib.parse import urlencode
    from urllib.request import Request, urlopen
    params = {
        "engine": "google_lens",
        "url": hosted_url,
        "api_key": api_key,
    }
    try:
        req = Request(
            f"https://serpapi.com/search.json?{urlencode(params)}",
            headers={"User-Agent": "AutoInvoice/1.0"},
        )
        with urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.debug(f"SerpAPI google_lens call failed: {e}")
        return {}

    matches = data.get("visual_matches") or []
    if not matches:
        return {}
    from collections import Counter
    domains: Counter = Counter()
    for m in matches[:20]:
        src = (m.get("source") or "").strip()
        if src:
            domains[src.lower()] += 1
    if not domains:
        return {}
    top_domain, _count = domains.most_common(1)[0]
    name = _brand_from_domain(top_domain)
    if not name:
        return {}
    return {
        "name": name,
        "country_code": "",
        "address": "",
        "confidence": "medium",
        "source": "reverse_image_search",
    }


# ── Orchestrator ──────────────────────────────────────────────

def identify_supplier_from_pdf(
    pdf_path: str,
    *,
    try_reverse: bool = True,
    use_cache: bool = True,
) -> Dict[str, Any]:
    """Run the supplier-identification tiers in order until a brand is
    found:

      1. OCR-text → in-app text LLM (``identify_from_ocr_text``) —
         cheapest and most reliable for brands whose wordmark is
         captured by tesseract/paddleocr (H&M, TEMU, SHEIN, ...).
      2. Vision logo LLM (``identify_from_logo_vision``) — covers docs
         where only the logo graphic is present (no wordmark text).
      3. Reverse image search (``reverse_image_search``) — final
         fallback for obscure merchants; requires ``SERPAPI_API_KEY``.

    Returns a dict of the shape
    ``{"name", "country_code", "address", "confidence", "source"}``
    on success, or ``{}`` when no tier identified the supplier. Results
    (including empties) are cached to
    ``workspace/_cache/brand/{fingerprint}.json`` so repeat runs of the
    same shipment are free. Pass ``use_cache=False`` to bypass the
    cache (e.g. after adding API keys)."""
    if not pdf_path or not os.path.isfile(pdf_path):
        return {}

    key: Optional[str] = None
    if use_cache:
        try:
            key = _pdf_fingerprint(pdf_path)
        except OSError:
            key = None
        if key:
            cached = _cache_get(key)
            if cached is not None:
                return cached

    result: Dict[str, Any] = {}

    # Tier 1 — OCR text → in-app LLM
    try:
        result = identify_from_ocr_text(pdf_path)
    except Exception as e:
        logger.debug(f"ocr-text tier raised: {e}")
        result = {}

    # Tier 2 — vision logo LLM (fallback when OCR text had no brand wordmark)
    if not result.get("name"):
        try:
            result = identify_from_logo_vision(pdf_path)
        except Exception as e:
            logger.debug(f"vision tier raised: {e}")
            result = {}

    # Tier 3 — reverse image search
    if not result.get("name") and try_reverse:
        try:
            result = reverse_image_search(pdf_path)
        except Exception as e:
            logger.debug(f"reverse-image tier raised: {e}")
            result = {}

    if key and use_cache:
        _cache_put(key, result or {})

    return result or {}
