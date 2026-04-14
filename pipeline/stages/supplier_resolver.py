"""
Supplier resolution and classification utilities.

Extracted from process_bl.py for DRY reuse across pipeline modes.
Handles:
  - Supplier database (load/save/update from data/suppliers.json)
  - Supplier info resolution (invoice PDF → DB → web search)
  - Classification rule loading and application
  - PDF text extraction
  - Invoice data normalization (FormatParser → POMatcher format)
  - File discovery helpers (PO, BL, PDF files)
"""

import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# ── ISO Country Code → Country Name (single source of truth) ──
# Used to enforce country_code ↔ country referential integrity.
# When auto-adding suppliers, country name is ALWAYS derived from this map.
COUNTRY_MAP = {
    'US': 'United States', 'CA': 'Canada', 'MX': 'Mexico',
    'CN': 'China', 'JP': 'Japan', 'KR': 'South Korea', 'TW': 'Taiwan',
    'SG': 'Singapore', 'MY': 'Malaysia', 'TH': 'Thailand', 'VN': 'Vietnam',
    'IN': 'India', 'PK': 'Pakistan', 'BD': 'Bangladesh',
    'GB': 'United Kingdom', 'DE': 'Germany', 'FR': 'France', 'IT': 'Italy',
    'ES': 'Spain', 'NL': 'Netherlands', 'SE': 'Sweden', 'DK': 'Denmark',
    'NO': 'Norway', 'FI': 'Finland', 'BE': 'Belgium', 'AT': 'Austria',
    'CH': 'Switzerland', 'PT': 'Portugal', 'IE': 'Ireland', 'PL': 'Poland',
    'CZ': 'Czech Republic', 'HU': 'Hungary', 'RO': 'Romania',
    'AU': 'Australia', 'NZ': 'New Zealand',
    'BR': 'Brazil', 'AR': 'Argentina', 'CL': 'Chile', 'CO': 'Colombia',
    'TT': 'Trinidad and Tobago', 'GD': 'Grenada', 'BB': 'Barbados',
    'JM': 'Jamaica', 'GY': 'Guyana', 'SR': 'Suriname',
    'PA': 'Panama', 'CR': 'Costa Rica', 'SV': 'El Salvador',
    'GT': 'Guatemala', 'HN': 'Honduras', 'NI': 'Nicaragua',
    'SX': 'Sint Maarten', 'CW': 'Curaçao', 'AW': 'Aruba',
    'BZ': 'Belize', 'BS': 'Bahamas', 'HT': 'Haiti', 'DO': 'Dominican Republic',
    'AG': 'Antigua and Barbuda', 'LC': 'Saint Lucia', 'VC': 'Saint Vincent',
    'DM': 'Dominica', 'KN': 'Saint Kitts and Nevis',
    'QN': 'St. Maarten',
    'IL': 'Israel', 'AE': 'United Arab Emirates', 'SA': 'Saudi Arabia',
    'ZA': 'South Africa', 'NG': 'Nigeria', 'EG': 'Egypt',
    'RU': 'Russia', 'TR': 'Turkey', 'UA': 'Ukraine',
}

# US state abbreviations — used to detect US addresses from web lookups
_US_STATES = {
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
    'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
    'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
    'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
    'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
    'DC', 'PR', 'VI',
}


def country_name_from_code(code: str) -> str:
    """Look up country name from ISO 2-letter code. Returns '' if unknown."""
    return COUNTRY_MAP.get(code.upper(), '') if code else ''


def _address_implies_us(address: str) -> bool:
    """Check if an address contains US state abbreviation + ZIP pattern."""
    if not address:
        return False
    # Match ", ST 12345" or ", ST 12345-6789" at end of address
    return bool(re.search(
        r',\s*(' + '|'.join(_US_STATES) + r')\s+\d{5}(?:-\d{4})?',
        address
    ))


def _address_implies_canada(address: str) -> bool:
    """Check if an address contains Canadian postal code pattern."""
    if not address:
        return False
    return bool(re.search(r'[A-Z]\d[A-Z]\s*\d[A-Z]\d', address, re.IGNORECASE))


# Base directory (project root) — set by init() or auto-detected
_BASE_DIR: Optional[str] = None


def init(base_dir: str) -> None:
    """Set the project base directory for file lookups."""
    global _BASE_DIR
    _BASE_DIR = base_dir


def _get_base_dir() -> str:
    global _BASE_DIR
    if _BASE_DIR is None:
        # Auto-detect: this file is at pipeline/stages/supplier_resolver.py
        _BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return _BASE_DIR


# ── Supplier Database ──────────────────────────────────────────


def load_supplier_db() -> Dict:
    """Load supplier database from data/suppliers.json."""
    suppliers_path = os.path.join(_get_base_dir(), 'data', 'suppliers.json')
    if os.path.exists(suppliers_path):
        try:
            with open(suppliers_path, 'r') as f:
                data = json.load(f)
            return data.get('suppliers', {})
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to load suppliers.json: {e}")
    return {}


def save_supplier_db(supplier_db: Dict) -> None:
    """Save supplier database back to data/suppliers.json."""
    suppliers_path = os.path.join(_get_base_dir(), 'data', 'suppliers.json')
    try:
        # Load full file to preserve version/description
        if os.path.exists(suppliers_path):
            with open(suppliers_path, 'r') as f:
                data = json.load(f)
        else:
            data = {"version": "1.0.0", "description": "Supplier database for invoice processing"}
        data['suppliers'] = supplier_db
        from datetime import datetime
        data['last_updated'] = datetime.now().strftime("%Y-%m-%d")
        os.makedirs(os.path.dirname(suppliers_path), exist_ok=True)
        with open(suppliers_path, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.warning(f"Failed to save suppliers.json: {e}")


def update_supplier_entry(supplier_db: Dict, code: str,
                          resolved_info: Dict) -> bool:
    """
    Update or add a supplier entry in the database from resolved pipeline data.

    Only fills in missing/empty fields — never overwrites existing good data.
    This ensures invoice PDF data and web search results persist for future runs.

    Args:
        supplier_db: Mutable supplier database dict
        code: Supplier code (e.g. "CAL", "BRU")
        resolved_info: Dict with keys: code, name, address, country

    Returns:
        True if the database was modified
    """
    code_upper = code.upper()
    existing = supplier_db.get(code_upper, {})
    modified = False

    if not existing:
        # New supplier — create full entry with integrity checks
        address = resolved_info.get('address', '')
        cc = resolved_info.get('country', 'US')

        # Cross-validate: if address is clearly US, override country code
        if _address_implies_us(address):
            cc = 'US'
        elif _address_implies_canada(address):
            cc = 'CA'

        # Derive country name from code (single source of truth)
        country_name = country_name_from_code(cc)

        supplier_db[code_upper] = {
            'code': code_upper,
            'name': resolved_info.get('name', ''),
            'full_name': resolved_info.get('name', ''),
            'address': address,
            'country_code': cc,
            'country': country_name,
            'currency': 'USD',
            'notes': 'Auto-added from invoice pipeline',
        }
        modified = True
        logger.info(f"Added new supplier to DB: {code_upper} "
                    f"({resolved_info.get('name', '')}) "
                    f"country={cc}/{country_name}")
    else:
        # Existing supplier — fill gaps or replace bad data
        cur_addr = existing.get('address', '')
        new_addr = resolved_info.get('address', '')
        # Update address if missing, or if current is placeholder/malformed
        addr_is_bad = (not cur_addr or cur_addr == 'ADDRESS REQUIRED'
                       or '\n' in cur_addr)
        if addr_is_bad and new_addr:
            existing['address'] = new_addr
            modified = True
        new_country = resolved_info.get('country', '')
        cur_country = existing.get('country_code', '')
        # Update country if missing, or if DB has default 'US' but invoice says otherwise
        if new_country and (not cur_country or
                            (cur_country == 'US' and new_country != 'US')):
            existing['country_code'] = new_country
            existing['country'] = country_name_from_code(new_country)
            modified = True
            logger.info(f"Updated supplier {code_upper} country: {cur_country} → {new_country}")
        if not existing.get('full_name') and resolved_info.get('name'):
            existing['full_name'] = resolved_info['name']
            modified = True
        # Remove "NEEDS VERIFICATION" if we now have real data
        if existing.get('needs_verification') and new_addr:
            existing['needs_verification'] = False
            if '(NEEDS VERIFICATION)' in existing.get('full_name', ''):
                existing['full_name'] = existing['full_name'].replace(
                    ' (NEEDS VERIFICATION)', '')
            modified = True

        # ── Integrity enforcement ──
        # Cross-validate address vs country_code
        final_addr = existing.get('address', '')
        cc = existing.get('country_code', '')
        if final_addr and cc:
            if _address_implies_us(final_addr) and cc != 'US':
                logger.warning(f"Supplier {code_upper}: address looks US "
                               f"but country_code={cc}, correcting to US")
                existing['country_code'] = 'US'
                modified = True
            elif _address_implies_canada(final_addr) and cc != 'CA':
                logger.warning(f"Supplier {code_upper}: address looks Canadian "
                               f"but country_code={cc}, correcting to CA")
                existing['country_code'] = 'CA'
                modified = True

        # Ensure country name matches country_code (derive from map)
        cc = existing.get('country_code', '')
        expected_country = country_name_from_code(cc)
        if expected_country and existing.get('country', '') != expected_country:
            existing['country'] = expected_country
            modified = True

    return modified


# ── Supplier Info Resolution ───────────────────────────────────


def _resolve_supplier_code(name: str, supplier_db: Dict) -> Optional[str]:
    """Resolve a supplier name to its code in the supplier database."""
    if not name:
        return None
    for code, info in supplier_db.items():
        full_name = info.get('full_name', '')
        short_name = info.get('name', '')
        if (name == full_name or name == short_name or
                name.lower() in full_name.lower() or
                short_name.lower() in name.lower()):
            return info.get('code', code)
    return None


def _lookup_supplier_web(supplier_name: str) -> Dict:
    """
    Web search fallback for supplier info when invoice PDF data is incomplete.

    Searches DuckDuckGo for the supplier's address and country.
    Returns dict with keys: address, country (empty strings if not found).
    """
    if not supplier_name:
        return {'address': '', 'country': ''}

    try:
        try:
            import requests
            from urllib.parse import quote as url_quote
            use_requests = True
        except ImportError:
            from urllib.request import urlopen, Request
            from urllib.parse import quote as url_quote
            use_requests = False

        query = url_quote(f"{supplier_name} company headquarters address")
        url = f"https://html.duckduckgo.com/html/?q={query}"

        if use_requests:
            resp = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'},
                                timeout=10)
            text = resp.text
        else:
            req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urlopen(req, timeout=10) as resp:
                text = resp.read().decode('utf-8', errors='replace')

        import re as _re

        # Try to extract a US address pattern (street, city, state ZIP)
        addr_match = _re.search(
            r'(\d+\s+[A-Za-z0-9\s.,]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|'
            r'Boulevard|Blvd|Way|Parkway|Pkwy|Lane|Ln|Place|Pl|Court|Ct)'
            r'[A-Za-z0-9\s.,#-]*?'
            r'[A-Z]{2}\s+\d{5})',
            text
        )
        address = addr_match.group(1).strip() if addr_match else ''

        # Try to detect country from text context
        country = 'US'  # default
        text_lower = text.lower()
        # Caribbean / Latin America
        if 'panam' in text_lower or 'ciudad de panam' in text_lower:
            country = 'PA'
        elif 'sint maarten' in text_lower or 'st. maarten' in text_lower or 'st maarten' in text_lower or 'sxm' in text_lower:
            country = 'SX'
        elif 'curaçao' in text_lower or 'curacao' in text_lower:
            country = 'CW'
        elif 'aruba' in text_lower:
            country = 'AW'
        elif 'trinidad' in text_lower or 'tobago' in text_lower:
            country = 'TT'
        elif 'barbados' in text_lower:
            country = 'BB'
        elif 'jamaica' in text_lower:
            country = 'JM'
        elif 'grenada' in text_lower:
            country = 'GD'
        elif 'colombia' in text_lower:
            country = 'CO'
        elif 'mexico' in text_lower or 'méxico' in text_lower:
            country = 'MX'
        elif 'costa rica' in text_lower:
            country = 'CR'
        # Europe / Asia
        elif 'netherlands' in text_lower or 'dutch' in text_lower:
            country = 'NL'
        elif 'china' in text_lower or 'chinese' in text_lower:
            country = 'CN'
        elif 'japan' in text_lower or 'japanese' in text_lower:
            country = 'JP'
        elif 'singapore' in text_lower:
            country = 'SG'
        elif 'germany' in text_lower or 'german' in text_lower:
            country = 'DE'
        elif 'united kingdom' in text_lower or 'british' in text_lower:
            country = 'GB'
        elif 'canada' in text_lower or 'canadian' in text_lower:
            country = 'CA'

        # Cross-validate: if we found a US address, country MUST be US
        # This prevents the bug where a US subsidiary gets tagged with
        # its European/Asian parent company's country code.
        if address and _address_implies_us(address):
            if country != 'US':
                logger.info(f"Web lookup for '{supplier_name}': "
                            f"address is US but text suggested {country}, "
                            f"correcting to US")
            country = 'US'
        elif address and _address_implies_canada(address):
            country = 'CA'

        if address or country != 'US':
            logger.info(f"Web lookup for '{supplier_name}': "
                        f"address='{address[:50]}...' country={country}")

        return {'address': address, 'country': country}

    except Exception as e:
        logger.debug(f"Web supplier lookup failed for '{supplier_name}': {e}")
        return {'address': '', 'country': ''}


def get_supplier_info(supplier_name: str, supplier_db: Dict,
                      invoice_data: Dict = None) -> Dict:
    """
    Build supplier info with priority: invoice PDF data → suppliers.json → web search.

    Priority chain:
      1. Invoice PDF extracted data (supplier_name, supplier_address, country_code)
      2. suppliers.json database (fills gaps — especially supplier code)
      3. Web search fallback (for address/country when both above are empty)

    Args:
        supplier_name: Supplier display name (from invoice or PO)
        supplier_db: Supplier database from suppliers.json
        invoice_data: Parsed invoice metadata (may contain supplier, country_code,
                      supplier_address from format parser)

    Returns:
        Dict with keys: code, name, address, country
    """
    # ── Source 1: Invoice PDF extracted data (primary) ──
    inv_name = ''
    inv_country = ''
    inv_address = ''
    if invoice_data:
        inv_name = invoice_data.get('supplier_name', '')
        inv_country = invoice_data.get('country_code', '')
        inv_address = invoice_data.get('supplier_address', '')

    # ── Source 2: suppliers.json database (for code + gap filling) ──
    # Try matching on invoice supplier name first, then fall back to PO name
    db_info = {}
    names_to_try = [n for n in [inv_name, supplier_name] if n]
    for try_name in names_to_try:
        for code, info in supplier_db.items():
            full_name = info.get('full_name', '')
            name = info.get('name', '')
            if (try_name == full_name or try_name == name or
                    (full_name and try_name.lower() in full_name.lower()) or
                    (name and name.lower() in try_name.lower())):
                db_info = {
                    'code': info.get('code', code),
                    'name': info.get('full_name', info.get('name', '')),
                    'address': info.get('address', ''),
                    'country': info.get('country_code', 'US'),
                }
                break
        if db_info:
            break

    # Supplier code: from DB (invoices rarely contain internal codes)
    code = db_info.get('code', '')
    if not code:
        # Generate a short code from the first word of the supplier name
        if supplier_name:
            first_word = supplier_name.strip().split()[0].upper()
            # Use full first word if short enough, otherwise truncate to 8 chars
            code = first_word[:8] if len(first_word) > 8 else first_word
        else:
            code = 'UNK'

    # Resolve name: invoice PDF → PO data → DB
    resolved_name = inv_name or supplier_name or db_info.get('name', '')

    # Resolve address: DB (curated) → invoice PDF (may be HQ/registered address)
    resolved_address = db_info.get('address', '') or inv_address

    # Resolve country: invoice PDF → DB → name suffix inference
    resolved_country = inv_country or db_info.get('country', '')

    # If country is still default 'US' (or empty), try inferring from supplier name suffix
    # Company suffixes indicate origin: S.A. = Latin America, N.V. = Dutch Caribbean,
    # GmbH = Germany, Ltd = various, B.V. = Netherlands
    if not resolved_country or resolved_country == 'US':
        name_upper = (resolved_name or supplier_name or '').strip().upper()
        if name_upper.endswith(', S. A.') or name_upper.endswith(', S.A.') or name_upper.endswith(' S.A.'):
            # S.A. (Sociedad Anónima) — common in Panama, but also Spain/Latin America
            # Don't override if we have invoice data or a non-default DB entry
            if not inv_country and (not db_info.get('country') or db_info['country'] == 'US'):
                resolved_country = 'PA'  # Default S.A. to Panama for Caribbean trade
        elif name_upper.endswith(' N.V.') or name_upper.endswith(', N.V.'):
            # N.V. (Naamloze Vennootschap) — Dutch Caribbean (SX, CW, AW) or Netherlands
            if not inv_country and (not db_info.get('country') or db_info['country'] == 'US'):
                resolved_country = 'SX'  # Default N.V. to Sint Maarten for Caribbean trade
        elif name_upper.endswith(' GMBH') or name_upper.endswith(', GMBH'):
            if not inv_country and not db_info.get('country'):
                resolved_country = 'DE'

    # ── Source 3: Web search fallback (only if address or country still missing) ──
    if not resolved_address or not resolved_country:
        web_info = _lookup_supplier_web(resolved_name or supplier_name)
        if not resolved_address and web_info.get('address'):
            resolved_address = web_info['address']
        if not resolved_country and web_info.get('country'):
            resolved_country = web_info['country']

    return {
        'code': code,
        'name': resolved_name,
        'address': resolved_address,
        'country': resolved_country or 'US',
    }


# ── Classification ─────────────────────────────────────────────


def load_classification_rules() -> Tuple[List[Dict], set]:
    """Load classification rules and noise words from rules/classification_rules.json."""
    rules_path = os.path.join(_get_base_dir(), 'rules', 'classification_rules.json')
    if not os.path.exists(rules_path):
        logger.warning(f"Classification rules not found: {rules_path}")
        return [], set()

    try:
        with open(rules_path, 'r') as f:
            rules_data = json.load(f)

        rules = sorted(
            rules_data.get('rules', []),
            key=lambda r: r.get('priority', 0),
            reverse=True
        )
        noise_words = set(rules_data.get('word_analysis', {}).get('noise_words', []))
        return rules, noise_words
    except (json.JSONDecodeError, IOError) as e:
        logger.warning(f"Failed to load classification rules: {e}")
        return [], set()


def _normalize_product_type(desc: str) -> str:
    """
    Normalize a product description to its core product type for similarity grouping.

    Strips sizes, dimensions, quantities, colors, model numbers, and other variants
    so that 'Zinc Anode 3/4 inch' and 'Zinc Anode 1 inch' both normalize to 'zinc anode'.
    """
    text = desc.lower().strip()

    # Remove SKU/part numbers in brackets (e.g., "[EGL/11050-004]")
    text = re.sub(r'\[[A-Z0-9/\-]+\]', '', text, flags=re.IGNORECASE)

    # Remove dimension patterns like "260X95mm", "3" x 5-1/4""
    text = re.sub(r'\d+\s*x\s*\d+\s*(?:mm|cm)?', '', text)            # 260X95mm
    text = re.sub(r'\d+[\-/]\d+(?:[\-/]\d+)?[\"\']?', '', text)        # 3/4", 5-1/4"
    text = re.sub(r'\d+\.?\d*\s*(?:mm|cm|inch|inches|ft|foot|feet)\b', '', text)
    text = re.sub(r'\d+\.?\d*[\"\']\s*', '', text)                      # 3"
    # Remove orphaned measurement words and dimension separators left behind
    text = re.sub(r'\b(?:inch|inches|ft|foot|feet|mm|cm)\b', '', text)
    text = re.sub(r'\bx\b', '', text)  # leftover dimension separator

    # Remove quantities with units (must have unit word attached)
    text = re.sub(r'\b\d+\.?\d*\s*(?:gallon|gal|oz|lb|kg|liter|litre)s?\b', '', text)
    text = re.sub(r'\(\d+\s*items?\)', '', text)                        # (1 items)
    text = re.sub(r'\b\d+\s*(?:pcs?|pieces?|units?|pairs?|sets?|rolls?|sheets?)\b', '', text)

    # Remove color names (whole words only)
    colors = ['red', 'blue', 'green', 'yellow', 'black', 'white', 'grey', 'gray',
              'orange', 'purple', 'brown', 'pink', 'silver', 'gold', 'clear']
    for color in colors:
        text = re.sub(rf'\b{color}\b', '', text)

    # Remove remaining standalone numbers (but not words containing numbers)
    text = re.sub(r'\b\d+\.?\d*\b', '', text)

    # Remove stray punctuation left behind
    text = re.sub(r'[,\-\"\'/]+\s*', ' ', text)

    # Collapse whitespace and strip
    text = re.sub(r'\s+', ' ', text).strip()

    return text


def _get_cet_category(tariff_code: str, base_dir: str) -> str:
    """Look up CET description for a tariff code to use as category fallback.

    Returns a descriptive category label derived from the CET schedule,
    or '' if the code is not found.
    """
    if not tariff_code or tariff_code == '00000000' or len(tariff_code) != 8:
        return ''
    try:
        import sqlite3
        db_path = os.path.join(base_dir, 'data', 'cet.db')
        if not os.path.exists(db_path):
            return ''
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        # Try exact code first
        cur.execute('SELECT description FROM cet_codes WHERE hs_code = ?', (tariff_code,))
        row = cur.fetchone()
        if row and row[0]:
            conn.close()
            return row[0]
        # Try heading level (first 4 digits + 0000)
        heading = tariff_code[:4] + '0000'
        cur.execute('SELECT description FROM cet_codes WHERE hs_code = ?', (heading,))
        row = cur.fetchone()
        if row and row[0]:
            conn.close()
            return row[0]
        conn.close()
    except Exception as e:
        logger.debug(f"CET category lookup failed for {tariff_code}: {e}")

    # Fallback: try cet_master_codes.txt (tab-separated: code\tdescription)
    txt_path = os.path.join(base_dir, 'data', 'cet_master_codes.txt')
    if os.path.exists(txt_path):
        try:
            with open(txt_path, 'r') as f:
                for line in f:
                    parts = line.strip().split('\t', 1)
                    if len(parts) == 2 and parts[0] == tariff_code:
                        return parts[1].strip('- ').strip()
                # Try prefix match (e.g. 39233000 → 392330)
                prefix = tariff_code[:6]
                f.seek(0)
                for line in f:
                    parts = line.strip().split('\t', 1)
                    if len(parts) == 2 and parts[0].startswith(prefix):
                        return parts[1].strip('- ').strip()
        except Exception:
            pass
    return ''


def _llm_assign_categories(matched_items: List[Dict], config: Dict) -> int:
    """
    Phase 1: LLM category pre-assignment.

    Sends ALL items in ONE batch LLM call. The LLM assigns a 2-digit HS chapter
    and descriptive category label to each item based on its description.

    This runs BEFORE the classification passes so that the chapter can be used
    as a gate to reject bad assessed/cache matches.

    Returns the number of items that received a category assignment.
    """
    import sys
    pipeline_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if pipeline_dir not in sys.path:
        sys.path.insert(0, pipeline_dir)
    from core.llm_client import get_llm_client

    base_dir = config.get('base_dir', '.') if config else '.'

    # Build numbered list of items with both PO and supplier descriptions
    item_lines = []
    for i, item in enumerate(matched_items):
        po_desc = item.get('po_item_desc', '') or ''
        sup_desc = item.get('supplier_item_desc', '') or ''
        desc = po_desc
        if sup_desc and sup_desc != po_desc:
            desc = f"{po_desc} / {sup_desc}" if po_desc else sup_desc
        if not desc:
            desc = "(no description)"
        item_lines.append(f"{i+1}. {desc}")

    if not item_lines:
        return 0

    # Batch into chunks of 120 items max to stay within LLM context limits
    BATCH_SIZE = 120
    assigned = 0

    for batch_start in range(0, len(item_lines), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(item_lines))
        batch_lines = item_lines[batch_start:batch_end]
        item_list = "\n".join(batch_lines)

        system_prompt = """You are a CARICOM customs tariff expert. For each product, assign:
1. The 2-digit HS chapter number (e.g., "82" for hand tools)
2. A brief category label matching the tariff chapter description

Classification rules:
- Classify by PRIMARY MATERIAL for simple articles (zinc=79, plastic=39, steel=73, aluminum=76, rubber=40, copper=74)
- Classify by FUNCTION for machines/apparatus (pumps=84, electrical=85, tools=82, valves=84)
- Lubricants/chemicals by composition (paints=32, soaps/cleaning=34, resins=39, adhesives=35)
- Marine products: zinc anodes=79, antifouling paint=32, diving gear=90/95, ropes=56
- Fasteners: by material (steel screws/bolts=73, brass fittings=74)
- Hoses: rubber=40, plastic=39

Use the product description to determine what the product IS, not what it sounds like.

Respond with ONLY a JSON object mapping item numbers to {chapter, category}.
Example: {"1": {"chapter": "82", "category": "Hand tools"}, "2": {"chapter": "73", "category": "Steel fasteners"}}"""

        user_message = f"""Assign HS chapter and category to these {len(batch_lines)} products:

{item_list}

Respond with JSON only."""

        try:
            llm = get_llm_client()
            result = llm.call_json(
                user_message=user_message,
                system_prompt=system_prompt,
                max_tokens=4096,
                use_cache=True,
                cache_key_extra=f"category_assignment_v1_batch{batch_start}",
            )

            if not result or not isinstance(result, dict):
                logger.warning("LLM category assignment returned non-dict result")
                continue

            # Apply results to matched_items
            for key, value in result.items():
                try:
                    idx = int(key) - 1  # 1-based to 0-based
                    if idx < 0 or idx >= len(matched_items):
                        continue
                    if not isinstance(value, dict):
                        continue
                    chapter = str(value.get('chapter', '')).zfill(2)[:2]
                    category = value.get('category', '')
                    if chapter and chapter.isdigit():
                        matched_items[idx]['llm_chapter'] = chapter
                        matched_items[idx]['llm_category'] = category
                        assigned += 1
                except (ValueError, TypeError):
                    continue

        except Exception as e:
            logger.warning(f"LLM category assignment failed: {e}")

    if assigned > 0:
        logger.info(f"[PHASE 1] LLM assigned categories to {assigned}/{len(matched_items)} items")
        print(f"      Phase 1: LLM assigned categories to {assigned}/{len(matched_items)} items")

    return assigned


def _llm_group_items(descriptions: List[str], config: Dict) -> Optional[Dict[str, List[int]]]:
    """
    Use LLM to group product descriptions by material/function similarity
    based on CARICOM classification principles.

    Sends ALL unclassified item descriptions in ONE call. The LLM groups them
    by what would share the same tariff code (same material, same function, etc.).

    Args:
        descriptions: List of product description strings
        config: Pipeline config (needs base_dir for LLM settings)

    Returns:
        Dict mapping group_label -> list of indices into `descriptions`,
        or None if LLM call fails.
    """
    if len(descriptions) <= 1:
        return {"group_0": [0]} if descriptions else None

    import sys
    pipeline_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if pipeline_dir not in sys.path:
        sys.path.insert(0, pipeline_dir)
    from core.llm_client import get_llm_client

    # Build numbered list of items
    item_list = "\n".join(f"{i+1}. {d}" for i, d in enumerate(descriptions))

    system_prompt = """You are a CARICOM customs tariff classification expert.
Your task is to group product items that would share the SAME 8-digit HS tariff code.

GROUPING RULES (follow strictly):
1. GROUP BY PRIMARY MATERIAL for simple articles:
   - All zinc items together (anodes, plates, bars) → same chapter 79
   - All aluminum articles together → chapter 76
   - All iron/steel articles together → chapter 73
   - All plastic articles together → chapter 39
   - All rubber articles together → chapter 40

2. GROUP BY FUNCTION for machines/apparatus/complex parts:
   - All pumps/macerators together → chapter 84
   - All electrical equipment together → chapter 85
   - All valves together → heading 8481
   - All hand tools together → chapter 82

3. GROUP BY SPECIFIC PRODUCT TYPE within a material/function:
   - Different sizes/colors/models of the SAME product = SAME group
   - e.g., "Zinc Anode 3/4 inch" and "Zinc Anode 1 inch" = same group
   - e.g., "Impeller for pump X" and "Impeller for pump Y" = same group
   - But "Zinc Anode" and "Zinc Spray Paint" = DIFFERENT groups (different HS codes)

4. Items that are genuinely unique get their own group.

Respond with ONLY a JSON object mapping group labels to arrays of item numbers.
Use descriptive group labels based on the product type."""

    user_message = f"""Group these {len(descriptions)} product descriptions by tariff classification similarity:

{item_list}

Respond with JSON only. Example format:
{{"zinc_anodes": [1, 3, 7], "stainless_steel_fittings": [2, 5], "water_pumps": [4, 6]}}"""

    try:
        llm = get_llm_client()
        result = llm.call_json(
            user_message=user_message,
            system_prompt=system_prompt,
            max_tokens=2048,
            use_cache=True,
            cache_key_extra="similarity_grouping_v1",
        )

        if not result or not isinstance(result, dict):
            logger.warning("LLM grouping returned non-dict result")
            return None

        # Convert 1-based item numbers to 0-based indices
        groups = {}
        assigned = set()
        for label, items in result.items():
            if not isinstance(items, list):
                continue
            indices = []
            for item_num in items:
                try:
                    idx = int(item_num) - 1  # Convert 1-based to 0-based
                    if 0 <= idx < len(descriptions) and idx not in assigned:
                        indices.append(idx)
                        assigned.add(idx)
                except (ValueError, TypeError):
                    continue
            if indices:
                groups[str(label)] = indices

        # Catch any items the LLM missed and put them in singleton groups
        for i in range(len(descriptions)):
            if i not in assigned:
                groups[f"ungrouped_{i}"] = [i]

        if groups:
            total_groups = len(groups)
            total_items = len(descriptions)
            logger.info(f"LLM grouped {total_items} items into {total_groups} groups")
            return groups

    except Exception as e:
        logger.warning(f"LLM grouping failed: {e}")

    return None


def _final_consistency_check(matched_items: List[Dict], rules: List[Dict],
                             noise_words: set, config: Dict) -> int:
    """
    Phase 3: Final consistency check.

    Verifies that each item's tariff code chapter matches the LLM-assigned chapter.
    Items that fail the check are re-classified without assessed data.

    Returns the number of items that were reclassified.
    """
    import sys
    pipeline_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if pipeline_dir not in sys.path:
        sys.path.insert(0, pipeline_dir)
    from classifier import (classify_item, validate_and_correct_code,
                            _gather_web_context, classify_with_llm,
                            _extract_search_terms, _check_lookup_cache,
                            _save_to_cache)

    base_dir = config.get('base_dir', '.') if config else '.'
    reclassified = 0
    mismatches = []

    for item in matched_items:
        llm_ch = item.get('llm_chapter', '')
        code = item.get('tariff_code', '00000000')
        if not llm_ch or code == '00000000':
            continue

        # Skip items classified by high-confidence rules — rules are curated and
        # take priority over LLM chapter guesses
        src = item.get('classification_source', '')
        if src in ('rule', 'classification_rule', 'manual_correction'):
            continue

        if code[:2] != llm_ch:
            desc = item.get('po_item_desc', '') or item.get('supplier_item_desc', '')
            mismatches.append((item, desc, llm_ch, code))

    if not mismatches:
        return 0

    print(f"      Phase 3: {len(mismatches)} items with chapter mismatch, attempting reclassification")

    for item, desc, expected_ch, old_code in mismatches:
        # Try cache lookup filtered by expected chapter
        search_terms = _extract_search_terms(desc)
        if search_terms:
            cache_result = _check_lookup_cache(search_terms, base_dir)
            if (cache_result and cache_result.get('code')
                    and cache_result['code'] != 'UNKNOWN'
                    and cache_result['code'][:2] == expected_ch):
                new_code = validate_and_correct_code(cache_result['code'], base_dir)
                if new_code[:2] == expected_ch:
                    logger.info(
                        f"[CONSISTENCY] Reclassified '{desc[:50]}': "
                        f"{old_code} -> {new_code} (cache, matches ch{expected_ch})")
                    item['tariff_code'] = new_code
                    reclassified += 1
                    continue

        # Try web+LLM classification
        web_context = _gather_web_context(desc, config)
        if web_context:
            llm_result = classify_with_llm(desc, web_context, config)
            if llm_result and llm_result.get('code') and llm_result['code'] != 'UNKNOWN':
                new_code = validate_and_correct_code(llm_result['code'], base_dir)
                if new_code[:2] == expected_ch:
                    logger.info(
                        f"[CONSISTENCY] Reclassified '{desc[:50]}': "
                        f"{old_code} -> {new_code} (web+LLM, matches ch{expected_ch})")
                    item['tariff_code'] = new_code
                    item['category'] = item.get('llm_category') or llm_result.get('category', '')
                    if search_terms:
                        _save_to_cache(search_terms, desc, llm_result, base_dir)
                    reclassified += 1
                    continue

        # If reclassification also disagrees with LLM chapter, keep the original
        # (the LLM chapter assignment might be wrong)
        logger.warning(
            f"[CONSISTENCY] Keeping '{desc[:50]}': {old_code} (ch{old_code[:2]}) — "
            f"reclassification did not confirm ch{expected_ch}")

    if reclassified:
        print(f"      Phase 3: Reclassified {reclassified}/{len(mismatches)} mismatched items")

    return reclassified


def classify_matched_items(matched_items: List[Dict], rules: List[Dict],
                           noise_words: set, config: Dict = None) -> int:
    """
    Classify matched items using rule engine + LLM similarity grouping + web/LLM lookup.
    Adds 'tariff_code' and 'category' keys to each matched item dict.

    Three-pass approach:
      Pass 1: Rule-based classification (fast, local)
      Pass 2: Group unclassified items by similarity (LLM first, regex fallback)
      Pass 3: One web search + one LLM classification call per group

    Returns count of successfully classified items.
    """
    # Import here to avoid circular dependency at module level
    import sys
    pipeline_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if pipeline_dir not in sys.path:
        sys.path.insert(0, pipeline_dir)
    from classifier import (classify_item, lookup_hs_code_web,
                            validate_and_correct_code, _gather_web_context,
                            classify_with_llm, _extract_search_terms,
                            _check_lookup_cache, _save_to_cache,
                            lookup_assessed_classification)

    base_dir = config.get('base_dir', '.') if config else '.'

    classified = 0
    unclassified = []  # (index, desc, alt_desc) tuples for items needing LLM

    # ── Phase 1: LLM category pre-assignment (1 batch call) ──
    # Assigns llm_chapter and llm_category to every item BEFORE classification.
    # These are used as a gate to reject bad assessed/cache matches.
    if config:
        _llm_assign_categories(matched_items, config)

    # ── Pass 1: Rule-based classification (fast, local) ──
    for i, item in enumerate(matched_items):
        desc = item.get('po_item_desc', '') or item.get('supplier_item_desc', '')
        if not desc:
            item['tariff_code'] = '00000000'
            item['category'] = ''
            continue

        expected_chapter = item.get('llm_chapter')
        result = classify_item(desc, rules, noise_words, base_dir,
                               expected_chapter=expected_chapter)

        if not result:
            alt_desc = item.get('supplier_item_desc', '')
            if alt_desc and alt_desc != desc:
                result = classify_item(alt_desc, rules, noise_words, base_dir,
                                       expected_chapter=expected_chapter)

        if result and result.get('code') and result['code'] != 'UNKNOWN':
            item['tariff_code'] = result['code']
            item['classification_source'] = result.get('source', 'rule')
            # Use LLM category as primary, fall back to classification result
            item['category'] = item.get('llm_category') or result.get('category', '')
            classified += 1
        else:
            unclassified.append((i, desc, item.get('supplier_item_desc', '')))

    if not unclassified or not config:
        # Mark remaining as unclassified
        for idx, desc, _ in unclassified:
            matched_items[idx]['tariff_code'] = '00000000'
            matched_items[idx]['category'] = ''
            print(f"      UNCLASSIFIED: {desc[:60]} -> 00000000")
        return classified

    # ── Pass 2: Group similar items ──
    # First try assessed classifications (containment/token matching on alt_desc),
    # then check cache for any items we've seen before (skip those from grouping)
    still_unclassified = []
    for entry in unclassified:
        idx, desc, alt_desc = entry
        expected_chapter = matched_items[idx].get('llm_chapter')

        # Try assessed lookup with alt description (supplier_item_desc)
        # Pass 1 already tried exact match on primary desc via classify_item;
        # here we try the alt_desc which may have different wording
        assessed = None
        if alt_desc and alt_desc != desc:
            assessed = lookup_assessed_classification(alt_desc, base_dir,
                                                      expected_chapter=expected_chapter)
        if assessed and assessed.get('code') and assessed['code'] != 'UNKNOWN':
            assessed['code'] = validate_and_correct_code(assessed['code'], base_dir)
            # Chapter gate: reject if LLM chapter disagrees
            if expected_chapter and assessed['code'][:2] != expected_chapter:
                logger.warning(
                    f"[PHASE 2] Rejected assessed hit {assessed['code']} (ch{assessed['code'][:2]}) "
                    f"for '{desc[:50]}' — LLM expects ch{expected_chapter}")
            else:
                matched_items[idx]['tariff_code'] = assessed['code']
                matched_items[idx]['category'] = matched_items[idx].get('llm_category') or assessed.get('category', '')
                classified += 1
                print(f"      Assessed hit: {desc[:50]}... -> {assessed['code']} ({assessed['source']})")
                continue

        search_terms = _extract_search_terms(desc)
        cache_result = _check_lookup_cache(search_terms, base_dir) if search_terms else None
        if cache_result and cache_result.get('code') and cache_result['code'] != 'UNKNOWN':
            cache_result['code'] = validate_and_correct_code(cache_result['code'], base_dir)
            # Chapter gate: reject cache hit if LLM chapter disagrees
            if expected_chapter and cache_result['code'][:2] != expected_chapter:
                logger.warning(
                    f"[PHASE 2] Rejected cache hit {cache_result['code']} (ch{cache_result['code'][:2]}) "
                    f"for '{desc[:50]}' — LLM expects ch{expected_chapter}")
                still_unclassified.append(entry)
            else:
                matched_items[idx]['tariff_code'] = cache_result['code']
                matched_items[idx]['category'] = matched_items[idx].get('llm_category') or cache_result.get('category', '')
                classified += 1
                print(f"      Cache hit: {desc[:50]}... -> {cache_result['code']}")
        else:
            still_unclassified.append(entry)

    if not still_unclassified:
        return classified

    # Try LLM-powered semantic grouping first
    descriptions_for_grouping = [desc for _, desc, _ in still_unclassified]
    llm_groups = _llm_group_items(descriptions_for_grouping, config)

    if llm_groups:
        # LLM grouping succeeded — build groups using LLM's semantic clusters
        groups = {}  # group_label -> [(index, desc, alt_desc), ...]
        for label, desc_indices in llm_groups.items():
            group_entries = [still_unclassified[di] for di in desc_indices]
            groups[label] = group_entries

        total_groups = len(groups)
        total_items = len(still_unclassified)
        print(f"      LLM similarity grouping: {total_items} items -> {total_groups} groups"
              + (f" (saving {total_items - total_groups} API calls)" if total_groups < total_items else ""))
    else:
        # Fallback: regex-based normalization grouping
        print("      LLM grouping unavailable, falling back to regex similarity")
        groups = {}
        for entry in still_unclassified:
            idx, desc, alt_desc = entry
            product_type = _normalize_product_type(desc)
            if not product_type:
                product_type = _normalize_product_type(alt_desc) if alt_desc else desc.lower()[:30]
            if product_type not in groups:
                groups[product_type] = []
            groups[product_type].append(entry)

        total_groups = len(groups)
        total_items = len(still_unclassified)
        if total_groups < total_items:
            print(f"      Regex similarity grouping: {total_items} items -> {total_groups} groups"
                  + f" (saving {total_items - total_groups} API calls)")

    # ── Pass 3: One web search + one LLM classification per group ──
    for group_label, group_items in groups.items():
        # Use the longest description as the representative (most detail)
        representative = max(group_items, key=lambda x: len(x[1]))
        rep_idx, rep_desc, rep_alt = representative

        short_desc = rep_desc[:60].encode('ascii', 'replace').decode('ascii')
        similar_count = len(group_items) - 1
        print(f"      Classifying: {short_desc}..." +
              (f" (+ {similar_count} similar)" if similar_count > 0 else ""))

        # One web search for the group's representative
        web_context = _gather_web_context(rep_desc, config)

        # One LLM classification call using the web context
        result = None
        if web_context:
            result = classify_with_llm(rep_desc, web_context, config)
            if result:
                result['code'] = validate_and_correct_code(result['code'], base_dir)
                # Cache the result for future runs
                search_terms = _extract_search_terms(rep_desc)
                if search_terms:
                    _save_to_cache(search_terms, rep_desc, result, base_dir)

        # If LLM classification failed, try direct web search
        if not result or not result.get('code') or result['code'] == 'UNKNOWN':
            result = lookup_hs_code_web(rep_desc, config)
            if result:
                result['code'] = validate_and_correct_code(result['code'], base_dir)

        if result and result.get('code') and result['code'] != 'UNKNOWN':
            source = result.get('source', 'web')
            print(f"      -> {result['code']} ({source})")

        # Apply result to ALL items in this group
        for idx, desc, alt_desc in group_items:
            if result and result.get('code') and result['code'] != 'UNKNOWN':
                matched_items[idx]['tariff_code'] = result['code']
                # Use LLM category as primary, fall back to classification result
                matched_items[idx]['category'] = matched_items[idx].get('llm_category') or result.get('category', '')
                classified += 1
                # Cache each item's search terms pointing to this result
                item_terms = _extract_search_terms(desc)
                if item_terms and item_terms != _extract_search_terms(rep_desc):
                    _save_to_cache(item_terms, desc, result, base_dir)
            else:
                matched_items[idx]['tariff_code'] = '00000000'
                matched_items[idx]['category'] = matched_items[idx].get('llm_category', '')
                print(f"      UNCLASSIFIED: {desc[:60]} -> 00000000")

    # ── Phase 3: Final consistency check ──
    if config:
        reclassified = _final_consistency_check(matched_items, rules, noise_words, config)
        if reclassified:
            classified += reclassified

    # ── Guarantee: category column is NEVER null ──
    for item in matched_items:
        if not item.get('category'):
            # Fallback 1: LLM category
            if item.get('llm_category'):
                item['category'] = item['llm_category']
            # Fallback 2: CET database description
            elif item.get('tariff_code') and item['tariff_code'] != '00000000':
                cet_cat = _get_cet_category(item['tariff_code'], base_dir)
                if cet_cat:
                    item['category'] = cet_cat
            # Fallback 3: Generic label from chapter
            if not item.get('category'):
                item['category'] = 'PRODUCTS'

    # ── Phase 4: Tariff-Product Compatibility Check ──
    # Verify the CET tariff description is semantically compatible with the product.
    _check_tariff_product_compatibility(matched_items, base_dir)

    return classified


def _check_tariff_product_compatibility(items: List[Dict], base_dir: str) -> None:
    """Check that tariff code descriptions are compatible with product descriptions.

    Flags obvious mismatches like 'Carboys, bottles, flasks' assigned to
    'BIODEGRADABLE SHOPPING BAGS'. Uses keyword overlap to detect mismatches.
    """
    import re

    # Material/product-type keywords that indicate a fundamental mismatch
    # when present in one description but contradicted by the other
    _MATERIAL_KEYWORDS = {
        'plastic', 'steel', 'iron', 'wood', 'wooden', 'glass', 'ceramic',
        'cotton', 'leather', 'rubber', 'paper', 'cardboard', 'aluminium',
        'aluminum', 'copper', 'textile', 'fabric', 'silk', 'wool', 'stone',
        'marble', 'concrete', 'porcelain',
    }
    _PRODUCT_KEYWORDS = {
        'bag', 'bags', 'bottle', 'bottles', 'flask', 'flasks', 'carboy', 'carboys',
        'chair', 'chairs', 'table', 'tables', 'lamp', 'shoe', 'shoes', 'boot', 'boots',
        'shirt', 'pants', 'dress', 'hat', 'cap', 'glove', 'gloves', 'sock', 'socks',
        'cup', 'plate', 'bowl', 'fork', 'knife', 'spoon', 'pan', 'pot',
        'wire', 'cable', 'pipe', 'tube', 'hose', 'nail', 'screw', 'bolt',
        'motor', 'engine', 'pump', 'fan', 'heater', 'cooler', 'oven',
        'soap', 'shampoo', 'lotion', 'cream', 'oil', 'wax', 'gel',
        'hair', 'skin', 'nail', 'lip', 'eye', 'perfume', 'fragrance',
        'jewelry', 'earring', 'necklace', 'bracelet', 'ring', 'stud',
        'comb', 'brush', 'wig', 'razor', 'clipper', 'shaver',
    }

    mismatches = []
    for item in items:
        tariff = item.get('tariff_code', '00000000')
        if tariff == '00000000':
            continue
        product_desc = (item.get('description') or item.get('supplier_item_desc')
                        or item.get('po_item_desc') or '')
        if not product_desc:
            continue

        cet_desc = _get_cet_category(tariff, base_dir)
        if not cet_desc:
            continue

        # Tokenize both descriptions
        product_tokens = set(re.findall(r'[a-z]+', product_desc.lower()))
        tariff_tokens = set(re.findall(r'[a-z]+', cet_desc.lower()))

        # Check for product-type keyword conflicts
        product_types = product_tokens & _PRODUCT_KEYWORDS
        tariff_types = tariff_tokens & _PRODUCT_KEYWORDS

        if product_types and tariff_types and not (product_types & tariff_types):
            # Both have product-type keywords but no overlap = likely mismatch
            mismatches.append({
                'product': product_desc[:60],
                'tariff_code': tariff,
                'tariff_desc': cet_desc[:60],
                'product_keywords': product_types,
                'tariff_keywords': tariff_types,
            })

        # Check for material conflicts
        product_materials = product_tokens & _MATERIAL_KEYWORDS
        tariff_materials = tariff_tokens & _MATERIAL_KEYWORDS
        if product_materials and tariff_materials and not (product_materials & tariff_materials):
            mismatches.append({
                'product': product_desc[:60],
                'tariff_code': tariff,
                'tariff_desc': cet_desc[:60],
                'product_keywords': product_materials,
                'tariff_keywords': tariff_materials,
            })

    if mismatches:
        # Deduplicate
        seen = set()
        unique = []
        for m in mismatches:
            key = (m['product'][:30], m['tariff_code'])
            if key not in seen:
                seen.add(key)
                unique.append(m)

        print(f"\n    TARIFF COMPATIBILITY WARNINGS ({len(unique)}):")
        for m in unique:
            print(f"      WARNING: \"{m['tariff_desc']}\" [{m['tariff_code']}] "
                  f"vs \"{m['product']}\"")
            print(f"        Tariff keywords: {m['tariff_keywords']}  "
                  f"Product keywords: {m['product_keywords']}")


# ── PDF Text Extraction ────────────────────────────────────────


def extract_pdf_text(pdf_path: str, ocr_config: dict = None) -> str:
    """Extract full text from a PDF via the unified hybrid OCR pipeline.

    The ``.txt`` sidecar cache (including the ``# MANUAL`` override) is
    still honoured because those are hand-authored corrections that must
    never be overwritten by re-OCR.

    The legacy ``ocr_config`` parameter (``dpi``/``psm``/``preprocessing``)
    is accepted for signature compatibility but now ignored — the hybrid
    multi_ocr pipeline runs the full preprocessing × engine matrix and
    reconciles via consensus, so format-specific Tesseract tuning is no
    longer meaningful.
    """
    # Check for OCR text sidecar (.txt file saved during PDF splitting)
    # Sidecars starting with "# MANUAL" are hand-written corrections that must
    # always be preferred and never overwritten by re-OCR.
    txt_sidecar = pdf_path.rsplit('.', 1)[0] + '.txt'
    if os.path.exists(txt_sidecar):
        try:
            with open(txt_sidecar, 'r', encoding='utf-8') as f:
                cached_text = f.read()
            is_manual = cached_text.startswith('# MANUAL')
            if is_manual:
                # Strip the marker line before returning
                cached_text = cached_text.split('\n', 1)[1] if '\n' in cached_text else cached_text
                logger.info(f"Using MANUAL sidecar for {os.path.basename(pdf_path)} ({len(cached_text)} chars)")
                return cached_text
            if len(cached_text.strip()) >= 50:
                logger.info(f"Using cached OCR text for {os.path.basename(pdf_path)} ({len(cached_text)} chars)")
                return cached_text
        except Exception:
            pass

    try:
        import multi_ocr  # noqa: WPS433
    except ImportError:
        logger.error("multi_ocr unavailable, cannot extract PDF text")
        return ""

    try:
        result = multi_ocr.extract_text(pdf_path)
        text = result.text or ""
        if text:
            logger.info(
                f"multi_ocr[{result.engine_used}] extracted {len(text)} chars "
                f"from {os.path.basename(pdf_path)}"
            )
        return text
    except Exception as e:
        logger.error(f"multi_ocr extract_text failed for {pdf_path}: {e}")
        return ""


# ── Invoice Data Normalization ─────────────────────────────────


def normalize_parse_result(result: Dict) -> Dict:
    """
    Normalize FormatParser output to the standard format expected
    by POMatcher and bl_xlsx_generator.

    FormatParser returns:
        {'status': 'success', 'format': 'harken', 'invoices': [{
            'invoice_number': '...', 'date': '...', 'total': ...,
            'freight': ..., 'items': [{'sku': ..., 'description': ...,
            'quantity': ..., 'unit_cost': ..., 'total_cost': ...}]
        }]}

    We normalize to:
        {'invoice_num': '...', 'invoice_date': '...', 'invoice_total': ...,
         'freight': ..., 'format': '...', 'items': [{'supplier_item': ...,
         'description': ..., 'quantity': ..., 'unit_price': ..., 'total': ...}]}
    """
    if not result or result.get('status') != 'success':
        return {
            'invoice_num': '', 'invoice_date': '', 'invoice_total': 0,
            'freight': 0, 'po_number': '', 'format': 'unknown', 'items': []
        }

    format_name = result.get('format', 'unknown')
    invoices = result.get('invoices', [])
    if not invoices:
        return {
            'invoice_num': '', 'invoice_date': '', 'invoice_total': 0,
            'freight': 0, 'po_number': '', 'format': format_name, 'items': []
        }

    inv = invoices[0]

    # Normalize items (skip zero-qty items with no dollar value)
    items = []
    for item in inv.get('items', []):
        qty = item.get('quantity', 0)
        total_cost = item.get('total_cost', item.get('total', 0))
        unit_price = item.get('unit_cost', item.get('unit_price', 0))
        if qty == 0 and (total_cost or 0) == 0:
            continue
        items.append({
            'supplier_item': item.get('sku', ''),
            'description': item.get('description', ''),
            'quantity': qty,
            'unit_price': item.get('unit_cost', item.get('unit_price', 0)),
            'total': total_cost,
            'catalog_num': item.get('catalog_num', ''),
            'legacy_sku': item.get('legacy_sku', ''),
            'data_quality': item.get('data_quality', ''),
        })

    return {
        'invoice_num': inv.get('invoice_number', ''),
        'invoice_date': inv.get('date', ''),
        'invoice_total': inv.get('total', 0) or 0,
        'sub_total': inv.get('sub_total', 0) or 0,
        'freight': inv.get('freight', 0) or 0,
        'tax': inv.get('tax', 0) or 0,
        'discount': inv.get('discount', 0) or 0,
        'savings': inv.get('savings', 0) or 0,
        'credits': inv.get('credits', 0) or 0,
        'free_shipping': inv.get('free_shipping', 0) or 0,
        'other_cost': inv.get('other_cost', 0) or 0,
        'skipped_items_total': inv.get('skipped_items_total', 0) or 0,
        'po_number': inv.get('po_number', ''),
        'format': format_name,
        'country_code': inv.get('country_code', ''),
        'supplier_name': inv.get('supplier', ''),
        'supplier_address': inv.get('supplier_address', ''),
        'items': items,
        'raw_text': inv.get('raw_text', ''),
        'ocr_quality': inv.get('ocr_quality', {}),
        'data_quality_notes': inv.get('data_quality_notes', []),
        'invoice_total_uncertain': inv.get('invoice_total_uncertain', False),
    }


# ── File Discovery Helpers ─────────────────────────────────────


def find_po_file(input_dir: str, bl_number: str = '') -> Optional[str]:
    """
    Find the PO XLSX file by examining contents — matches invoice references
    inside the Order Reference column against PDF filenames in the input dir.

    Search locations: input_dir first, then workspace/input/ as fallback.
    """
    import re
    try:
        import openpyxl
    except ImportError:
        logger.warning("openpyxl not installed — cannot search PO files by content")
        return None

    # Collect invoice reference numbers from PDF filenames in input_dir
    invoice_refs = set()
    try:
        for f in os.listdir(input_dir):
            if f.lower().endswith('.pdf'):
                # Strip extension; normalize dashes (2026-001763 → 2026001763)
                stem = os.path.splitext(f)[0]
                invoice_refs.add(stem.lower())
                invoice_refs.add(stem.replace('-', '').lower())
    except OSError:
        pass

    if not invoice_refs:
        return None

    # Search directories
    search_dirs = [input_dir]
    base = _get_base_dir()
    workspace_input = os.path.join(base, 'workspace', 'input')
    if os.path.isdir(workspace_input) and workspace_input != input_dir:
        search_dirs.append(workspace_input)

    # Also search the matching workspace/documents/ folder
    # Shipment folders are named "Shipment_ <email_subject>", documents folders are "<email_subject>"
    input_basename = os.path.basename(input_dir.rstrip('/\\'))
    documents_dir = os.path.join(base, 'workspace', 'documents')
    if os.path.isdir(documents_dir):
        # Try exact match first, then strip "Shipment_ " prefix
        for candidate_name in [input_basename, input_basename.replace('Shipment_ ', '', 1)]:
            candidate = os.path.join(documents_dir, candidate_name)
            if os.path.isdir(candidate) and candidate != input_dir:
                search_dirs.append(candidate)
                break

    best_match = None
    best_hit_count = 0

    for search_dir in search_dirs:
        try:
            files = os.listdir(search_dir)
        except OSError:
            continue

        for f in files:
            if not f.lower().endswith('.xlsx'):
                continue
            xlsx_path = os.path.join(search_dir, f)
            try:
                wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
                ws = wb.active
                hit_count = 0

                # Check Order Reference column (col 8) for invoice refs
                # Format: "PO/GD/07155 (2026-001763)" or "PO/GD/07178 (0098813384)"
                # Also check col 0 for invoice numbers (commercial invoice export format)
                # Format: "INV/SX/2026/004235" which maps to PDF "bmgrn INV_SX_2026_004235.pdf"
                for row in ws.iter_rows(min_row=2, max_col=12, values_only=True):
                    # Strategy 1: Order Reference in col 7 with (ref) pattern
                    order_ref = str(row[7]) if len(row) > 7 and row[7] else ''
                    if order_ref:
                        ref_match = re.search(r'\(([^)]+)\)', order_ref)
                        if ref_match:
                            refs_str = ref_match.group(1)
                            for ref in refs_str.split('/'):
                                ref_clean = ref.strip().lower()
                                ref_nodash = ref_clean.replace('-', '')
                                if ref_clean in invoice_refs or ref_nodash in invoice_refs:
                                    hit_count += 1
                                    break  # One hit per row is enough
                            continue

                    # Strategy 2: Invoice number in col 0 (e.g. INV/SX/2026/004235)
                    inv_num = str(row[0]) if row[0] else ''
                    if inv_num:
                        inv_normalized = inv_num.replace('/', '_').lower()
                        for ref in invoice_refs:
                            if inv_normalized in ref or ref.endswith(inv_normalized):
                                hit_count += 1
                                break

                wb.close()

                if hit_count > best_hit_count:
                    best_hit_count = hit_count
                    best_match = xlsx_path
                    logger.info(f"PO file candidate: {f} ({hit_count} invoice refs matched)")

            except Exception as e:
                logger.debug(f"Could not read {f} as PO XLSX: {e}")
                continue

    if best_match:
        logger.info(f"PO file selected: {os.path.basename(best_match)} ({best_hit_count} invoice refs matched)")
    return best_match


# ── Content-Based Document Classification ─────────────────────

# Bill of Lading keywords (checked against PDF text content)
_BL_KEYWORDS = [
    'BILL OF LADING', 'BILL OF LADEN', 'B/L NO', 'B/L NUMBER', 'BL NUMBER',
    'SHIPPER', 'CONSIGNEE', 'NOTIFY PARTY',
    'PORT OF LOADING', 'PORT OF DISCHARGE',
    'VESSEL', 'VOYAGE', 'CONTAINER NO',
    'FREIGHT PREPAID', 'FREIGHT COLLECT',
    'SHIPPED ON BOARD', 'PLACE OF RECEIPT',
    'OCEAN BILL', 'SEA WAYBILL',
    'OCEAN FREIGHT', 'HBL', 'MBL',
    'CFS/CY', 'CY/CY', 'CFS/CFS',
    'SEAL#', 'SEAL NO',
]

_PACKING_LIST_KEYWORDS = [
    'PACKING LIST', 'PACKING SLIP',
    'PALLET LIST', 'PALLETLIST',
    'CARTON NO', 'CTN NO', 'GROSS WEIGHT', 'NET WEIGHT',
    'TOTAL CARTONS', 'TOTAL PACKAGES',
]

_MANIFEST_KEYWORDS = [
    'ASYCUDA WORLD', 'ASYCUDA WORLD WAYBILL',
    'MAN REG NUMBER', 'MAN REG DATE',
    'WAYBILL', 'CUSTOMS OFFICE',
    'VOYAGE', 'VESSEL NAME',
    'PORT OF LOADING', 'PORT OF DISCHARGE',
    'CONSIGNEE', 'NOTIFY',
    'NO AND TYPE OF', 'PACKAGE',
    'GROSS', 'CUBIC MEA',
]

# Freight invoice keywords — standalone shipping/freight charges document
_FREIGHT_INVOICE_KEYWORDS = [
    'FREIGHT FROM', 'LANDING CHARGES', 'SHIPPING CHARGES',
    'FREIGHT CHARGES', 'OCEAN FREIGHT', 'AIR FREIGHT',
    'TOTAL DUE', 'PALLET',
]


def _is_freight_invoice(text: str) -> bool:
    """
    Detect if a PDF is a standalone freight/shipping charges invoice.

    These are short documents (typically 1 page) with freight line items
    like "FREIGHT FROM X TO Y" and "LANDING CHARGES" but no product items.
    Must NOT be a multi-page manifest/declaration that happens to mention freight.
    """
    text_upper = text.upper()

    # Exclude documents that are clearly manifests or declarations
    if 'ASYCUDA' in text_upper or 'MAN REG NUMBER' in text_upper:
        return False
    if 'SIMPLIFIED DECLARATION' in text_upper:
        return False

    # Must have a freight-specific keyword
    has_freight_line = ('FREIGHT FROM' in text_upper or
                        'FREIGHT CHARGES' in text_upper or
                        'SHIPPING CHARGES' in text_upper)
    has_landing = 'LANDING CHARGES' in text_upper

    if not (has_freight_line or has_landing):
        return False

    # Must have a total line
    has_total = bool(re.search(r'TOTAL\s+(?:DUE|AMOUNT|PAYABLE)', text_upper))
    if not has_total:
        return False

    # Must have amounts
    has_amount = bool(re.search(r'\d{1,3}(?:,\d{3})*\.\d{2}', text))

    # Freight invoices are short — reject if text is too long (multi-page manifest)
    if len(text.strip()) > 2000:
        return False

    return has_amount


def _classify_pdf_content(text: str) -> str:
    """
    Classify a PDF by its text content. Returns one of:
      'consolidation_report', 'bill_of_lading', 'manifest', 'declaration',
      'packing_list', 'invoice', 'unknown'
    """
    if not text or len(text.strip()) < 20:
        return 'unknown'

    text_upper = text.upper()

    # Score each document type
    bl_score = sum(1 for kw in _BL_KEYWORDS if kw in text_upper)
    packing_score = sum(1 for kw in _PACKING_LIST_KEYWORDS if kw in text_upper)

    # Reuse pdf_splitter keywords for declaration vs invoice
    try:
        from pdf_splitter import detect_document_type as _splitter_detect
        doc_type, confidence, _ = _splitter_detect(text)
    except ImportError:
        doc_type, confidence = 'unknown', 0.0

    # Consolidation report: freight forwarder recap with per-invoice packages
    # Checked before BL — contains BL keywords but is a distinct document type
    if 'CONSOLIDATION RECAP' in text_upper or 'CONSOLIDATION REPORT' in text_upper:
        return 'consolidation_report'

    # Freight invoice: standalone shipping/freight charges document
    # Checked early — these look like invoices but are just freight charges
    if _is_freight_invoice(text):
        return 'freight_invoice'

    # Simplified Declaration Form (Grenada etc.): must be checked before
    # the manifest gate because these forms also include "Man Reg Number"
    # which would otherwise trigger the ASYCUDA marker.
    if 'SIMPLIFIED DECLARATION' in text_upper:
        return 'declaration'

    # Manifest (ASYCUDA World Waybill): checked first — ASYCUDA marker is unique
    manifest_score = sum(1 for kw in _MANIFEST_KEYWORDS if kw in text_upper)
    has_asycuda_marker = 'ASYCUDA' in text_upper or 'MAN REG NUMBER' in text_upper
    if manifest_score >= 4 and has_asycuda_marker:
        return 'manifest'

    # BL: strong signal if 3+ BL keywords match
    if bl_score >= 3:
        return 'bill_of_lading'

    # Declaration: from pdf_splitter detection
    if doc_type == 'declaration' and confidence > 0.2:
        return 'declaration'

    # Packing list: 3+ keywords
    if packing_score >= 3:
        return 'packing_list'

    # Invoice: pdf_splitter says invoice, or default if text is present
    if doc_type == 'invoice' and confidence > 0.15:
        return 'invoice'

    # If there's meaningful text but we can't classify, call it invoice
    # (most PDFs in shipment emails are invoices)
    if len(text.strip()) >= 50:
        return 'invoice'

    return 'unknown'


def _extract_zips(input_dir: str) -> List[str]:
    """Extract PDFs from any ZIP files in the input directory.

    Extracts PDF and XLSX files from ZIPs directly into input_dir,
    skipping files that already exist.  Returns list of extracted filenames.
    """
    import zipfile
    extracted = []
    try:
        zips = [f for f in os.listdir(input_dir) if f.lower().endswith('.zip')]
    except OSError:
        return extracted

    for zf_name in zips:
        zf_path = os.path.join(input_dir, zf_name)
        try:
            with zipfile.ZipFile(zf_path, 'r') as zf:
                for member in zf.namelist():
                    # Skip directories and __MACOSX junk
                    if member.endswith('/') or '__MACOSX' in member:
                        continue
                    ml = member.lower()
                    if not (ml.endswith('.pdf') or ml.endswith('.xlsx')):
                        continue
                    # Use only the basename (flatten nested paths)
                    basename = os.path.basename(member)
                    if not basename:
                        continue
                    dest = os.path.join(input_dir, basename)
                    if os.path.exists(dest):
                        continue  # don't overwrite existing files
                    with zf.open(member) as src, open(dest, 'wb') as dst:
                        dst.write(src.read())
                    extracted.append(basename)
                    logger.info(f"Extracted from {zf_name}: {basename}")
        except (zipfile.BadZipFile, Exception) as e:
            logger.warning(f"Failed to extract {zf_name}: {e}")

    return extracted


def classify_input_pdfs(input_dir: str) -> Dict[str, List[str]]:
    """
    Classify all PDFs in a directory by reading their content.

    Extracts any ZIP archives first, then classifies.
    Returns dict with keys: 'bill_of_lading', 'manifest', 'declaration',
    'invoice', 'packing_list', 'unknown'. Values are lists of filenames.
    """
    # Extract PDFs/XLSX from ZIP archives before scanning
    zip_extracted = _extract_zips(input_dir)
    if zip_extracted:
        logger.info(f"Extracted {len(zip_extracted)} files from ZIP archives: {zip_extracted}")

    result = {
        'bill_of_lading': [],
        'consolidation_report': [],
        'manifest': [],
        'declaration': [],
        'freight_invoice': [],
        'invoice': [],
        'packing_list': [],
        'unknown': [],
    }

    try:
        all_pdfs = sorted(f for f in os.listdir(input_dir) if f.lower().endswith('.pdf'))
    except OSError:
        return result

    # Skip split artifacts from prior runs
    _SPLIT_SUFFIXES = ('_Invoice.pdf', '_Declaration.pdf', '_Unknown.pdf')
    originals = set(all_pdfs)
    pdfs_to_check = []
    for f in all_pdfs:
        if any(f.endswith(suf) for suf in _SPLIT_SUFFIXES):
            base = f
            for suf in _SPLIT_SUFFIXES:
                if f.endswith(suf):
                    base = f[: -len(suf)] + '.pdf'
                    break
            if base in originals:
                continue
        pdfs_to_check.append(f)

    # Filename-based pre-classification for non-invoice documents
    _NON_INVOICE_FILENAME_PATTERNS = {
        'consolidation_report': ['RECAP', 'CONSOLIDATION'],
        'bill_of_lading': ['BOL', 'B/L', 'BILL OF LADING', 'BILL OF LADEN',
                           'BILLOFLADING'],
        'packing_list': ['PACKINGLIST', 'PACKING LIST', 'PACKING_LIST',
                         'PALLET LIST', 'PALLETLIST', 'PALLET_LIST'],
        'declaration':  ['CARICOM'],
    }

    for f in pdfs_to_check:
        # Check filename first — skip content extraction for known non-invoices
        f_upper = f.upper()
        filename_classified = False
        for doc_type_key, patterns in _NON_INVOICE_FILENAME_PATTERNS.items():
            if any(pat in f_upper for pat in patterns):
                # Declaration filename match is weak — verify with content since
                # multi-document PDFs (e.g. "Tropical doc & Caricom") may contain a BL
                if doc_type_key == 'declaration':
                    pdf_path = os.path.join(input_dir, f)
                    text = extract_pdf_text(pdf_path)
                    content_type = _classify_pdf_content(text)
                    if content_type == 'bill_of_lading':
                        result['bill_of_lading'].append(f)
                        logger.info(f"Classified {f} → bill_of_lading (content override, filename said declaration)")
                    else:
                        result[doc_type_key].append(f)
                        logger.info(f"Classified {f} → {doc_type_key} (filename match, content confirmed)")
                else:
                    result[doc_type_key].append(f)
                    logger.info(f"Classified {f} → {doc_type_key} (filename match)")
                filename_classified = True
                break
        if filename_classified:
            continue

        pdf_path = os.path.join(input_dir, f)
        text = extract_pdf_text(pdf_path)
        doc_type = _classify_pdf_content(text)
        result[doc_type].append(f)
        logger.info(f"Classified {f} → {doc_type}")

    return result


def find_bl_pdf(input_dir: str) -> Optional[str]:
    """Find the Bill of Lading PDF by reading content of each PDF.

    Falls back to filename hints if content classification finds nothing.
    """
    classification = classify_input_pdfs(input_dir)
    if classification['bill_of_lading']:
        return os.path.join(input_dir, classification['bill_of_lading'][0])

    # Filename fallback for edge cases (e.g. scanned BL that OCR can't read)
    try:
        for f in os.listdir(input_dir):
            fl = f.lower()
            if not fl.endswith('.pdf'):
                continue
            fl_norm = fl.replace('_', ' ')
            if 'bill of lading' in fl_norm or fl.endswith('-bl.pdf'):
                return os.path.join(input_dir, f)
    except OSError:
        pass
    return None


def get_pdf_files(input_dir: str, classification: Optional[Dict] = None) -> List[str]:
    """Get sorted list of invoice PDF files using content-based classification.

    Uses classify_input_pdfs() to determine which PDFs are invoices.
    Non-invoice documents (BL, packing lists, declarations) are excluded.
    """
    if classification is None:
        classification = classify_input_pdfs(input_dir)
    return sorted(classification.get('invoice', []))


# ── Manifest (ASYCUDA World Waybill) Metadata Extraction ──────


def _parse_simplified_declaration(text: str) -> Dict:
    """Parse Grenada Simplified Declaration Form.

    OCR reads the two-column form as labels-block then values-block,
    separated by "Description of Goods". The values appear in the same
    order as the labels: consignee, office, man_reg, waybill, packages, weight.
    """
    meta = {}

    # Find the values block after "Description of Goods"
    m = re.search(r'Description\s+of\s+Goods\s*\n(.+?)(?:Particulars|undersigned|Customs\s+Value)',
                  text, re.DOTALL | re.IGNORECASE)
    if not m:
        return {}

    values_block = m.group(1).strip()
    values = [v.strip() for v in values_block.split('\n') if v.strip()]

    # Expected order: consignee, office, man_reg, waybill, packages, weight
    # Example values:
    #   AARON WILSON (FREIGHT 17.00 US)
    #   GDWBS
    #   2024/28
    #   HAWB9595443
    #   1 Package
    #   8.0 Freight/Insuranc...
    for val in values:
        # Consignee with embedded freight: "NAME (FREIGHT 17.00 US)"
        freight_m = re.search(r'\(FREIGHT\s+([\d.]+)', val, re.IGNORECASE)
        if freight_m and 'consignee_name' not in meta:
            meta['consignee_name'] = re.sub(r'\s*\(FREIGHT.*?\)', '', val).strip()
            meta['freight'] = freight_m.group(1)
            continue

        # Office code: short all-caps (e.g. GDWBS, GDSGO)
        if re.match(r'^[A-Z]{4,6}$', val) and 'office' not in meta:
            meta['office'] = val
            continue

        # Man Reg: "2024/28" or "2024 28"
        if re.match(r'^\d{4}[/\s]\d+$', val) and 'man_reg' not in meta:
            meta['man_reg'] = val
            continue

        # Waybill: starts with letter+digits (e.g. HAWB9595443)
        if re.match(r'^[A-Z]+\d+', val) and 'waybill' not in meta:
            meta['waybill'] = val
            continue

        # Packages: "1 Package" or "3 Packages"
        pkg_m = re.match(r'^(\d+)\s+Package', val, re.IGNORECASE)
        if pkg_m and 'packages' not in meta:
            meta['packages'] = pkg_m.group(1)
            continue

        # Weight: "8.0 Freight" or "8.0"
        wt_m = re.match(r'^([\d.]+)\s*(?:Frei|kg|KG|$)', val)
        if wt_m and 'weight' not in meta:
            meta['weight'] = wt_m.group(1)
            continue

        # Consignee without freight (just a name)
        if 'consignee_name' not in meta and re.match(r'^[A-Z][A-Z\s]+$', val) and len(val) > 5:
            meta['consignee_name'] = val
            continue

    return meta


def extract_manifest_metadata(pdf_path: str) -> Dict:
    """Extract metadata from ASYCUDA World Waybill PDF.

    Returns dict with keys: waybill, man_reg, consignee_name, consignee_address,
    packages, package_type, weight, freight, office, voyage, vessel.
    Empty dict if not a manifest or extraction fails.
    """
    text = extract_pdf_text(pdf_path)
    if not text:
        return {}

    text_upper = text.upper()
    is_asycuda = 'ASYCUDA' in text_upper
    is_simplified_decl = 'SIMPLIFIED DECLARATION' in text_upper
    if not is_asycuda and not is_simplified_decl:
        return {}

    meta = {}

    # Simplified Declaration Forms have a two-column layout that OCR reads as
    # labels-block then values-block, separated by "Description of Goods".
    # Parse values from the block after that marker.
    if is_simplified_decl:
        meta = _parse_simplified_declaration(text)
        # Fall through to standard patterns if simplified parsing got nothing
        if meta:
            return meta

    # ── Standard ASYCUDA manifest patterns ──

    # Man Reg Number: "Man Reg Number: 2025 529" or "Man Reg Number: 2024/28"
    m = re.search(r'Man\s*Reg\s*Number[:\s]*([\d]{4}[\s/]+\d+)', text)
    if m:
        meta['man_reg'] = m.group(1).strip()

    # Bill of Lading Number: prefer the BL header over ASYCUDA waybill field
    # "Bill of Lading Number\n\n04C" or "Bill of Lading Number:\s*TSCW123"
    bl_num_match = re.search(r'Bill\s+of\s+Lading\s+Number[:\s]*\n\s*\n?\s*([A-Z0-9]+)', text, re.IGNORECASE)
    if not bl_num_match:
        bl_num_match = re.search(r'Bill\s+of\s+Lading\s+Number[:\s]+([A-Z0-9]+)', text, re.IGNORECASE)

    # WayBill: "WayBill 173811" or "WayBill Number: HAWB9595443"
    m = re.search(r'WayBill(?:\s*Number)?[:\s]+(\S+)', text)
    if m:
        val = m.group(1).strip()
        # Skip if matched a label word like "No" (from "No and Type")
        if val.upper() not in ('NO', 'NUMBER', 'NUMBER:'):
            meta['waybill'] = val

    # BL number from the Bill of Lading header takes precedence over ASYCUDA waybill
    if bl_num_match:
        meta['waybill'] = bl_num_match.group(1).strip().upper()

    # Customs Office: "Customs Office: ST GEORGES"
    m = re.search(r'Customs\s*Office[:\s]+([A-Z\s]+?)(?:\s*Man\s*Reg|\n)', text, re.IGNORECASE)
    if m:
        meta['office'] = m.group(1).strip()

    # Consignee name + address block
    # Layouts vary: "Consignee:\nNotify:\nSAME AS CONSIGNEE\nNAME\n..."
    #           or: "Consignee: SAME AS CONSIGNEE\n\nNAME\n..."
    #           or: "Consignee:\nNotify: SAME AS CONSIGNEE\nNAME\n..."
    #           or: "Consignee: NAME (FREIGHT 37.00 US)"  (inline, simplified declarations)

    # Try inline consignee first: "Consignee: NAME (FREIGHT xx.xx US)"
    inline_m = re.search(r'Consignee[:\s]+([A-Z][A-Z\s]+?)\s*\(\s*FREIGHT\s+([\d.]+)', text, re.IGNORECASE)
    if inline_m:
        meta['consignee_name'] = inline_m.group(1).strip()
        meta['consignee_freight'] = inline_m.group(2)
    else:
        # Try inline consignee without freight: "Consignee: NAME\n"
        inline_m2 = re.search(r'Consignee[:\s]+([A-Z][A-Z\s]{3,}?)(?:\n|$)', text)
        if inline_m2 and 'consignee_name' not in meta:
            name = inline_m2.group(1).strip()
            if name.upper() not in ('SAME AS CONSIGNEE', 'SAME AS SHIPPER') and not name.upper().startswith('NOTIFY'):
                meta['consignee_name'] = name

    # Multi-line consignee block (ASYCUDA manifests)
    # Handles: "Consignee: Notify: CARIBCONEX...\nHILLS AND VALLEY PHARMACY\n..."
    #      or: "Consignee:\nNotify:\nSAME AS CONSIGNEE\nNAME\n..."
    if 'consignee_name' not in meta:
        m = re.search(r'Consignee[:\s]*(?:SAME\s+AS\s+CONSIGNEE)?(?:\s*Notify[:\s]*[^\n]*)?\n(.+?)(?:\nNo and Type|\nLine Number|\nCustoms|\nWayBill|\nMan Reg)', text, re.DOTALL)
        if m:
            # Filter out non-name lines: anything starting with "Notify", "SAME AS CONSIGNEE", short OCR noise
            skip_re = re.compile(r'^(Notify\b.*|SAME\s+AS\s+CONSIGNEE.*|any|e,?)$', re.IGNORECASE)
            lines = [l.strip().rstrip(',') for l in m.group(1).strip().split('\n')
                     if l.strip() and not skip_re.match(l.strip())]
            if lines:
                # Strip embedded freight info: "NAME (FREIGHT 17.00 US)"
                name = re.sub(r'\s*\(FREIGHT.*?\)', '', lines[0]).strip()
                meta['consignee_name'] = name
                if len(lines) > 1:
                    meta['consignee_address'] = ', '.join(lines[1:])

    # Packages: "No and Type of 80 Package" or OCR-garbled "No and Type of #* 3\npackage:"
    m = re.search(r'No\s+and\s+Type\s+of\s+(\d+)\s+(\w+)', text, re.IGNORECASE)
    if not m:
        # OCR fallback: digits after noise chars on same line or next line
        m = re.search(r'No\s+and\s+Type\s+of\s+[^0-9\n]*(\d+)\s*\n?\s*(\w+)', text, re.IGNORECASE)
    if m:
        meta['packages'] = m.group(1)
        meta['package_type'] = m.group(2)

    # Gross weight: "Gross 1228.82" or "Gross Mass: 18.0"
    m = re.search(r'Gross(?:\s+Mass)?[:\s]+([\d,.]+)', text)
    if m:
        w = m.group(1).replace(',', '')
        try:
            if float(w) > 0:
                meta['weight'] = w
        except ValueError:
            pass  # OCR garble like "." — skip

    # Freight: prefer embedded consignee freight (simplified declarations)
    # over "Freight 0.00" from gross mass line
    if 'consignee_freight' in meta:
        meta['freight'] = meta.pop('consignee_freight')
    else:
        m = re.search(r'(?<!\()Freight\s+([\d,.]+)', text)
        if m:
            meta['freight'] = m.group(1).replace(',', '')

    # Voyage: "Voyage WND012"
    m = re.search(r'Voyage\s+(\S+)', text)
    if m:
        meta['voyage'] = m.group(1)

    # Vessel: "Vessel Name: WINDING BAY"
    m = re.search(r'Vessel\s*Name[:\s]+(.+?)(?:\n|$)', text)
    if m:
        meta['vessel'] = m.group(1).strip()

    return meta


def extract_freight_invoice_data(pdf_path: str) -> Dict:
    """Extract freight and landing charges from a standalone freight invoice.

    Returns dict with keys: freight, landing_charges, total, packages,
    invoice_number, date, consignee.
    Empty dict if not a freight invoice or extraction fails.
    """
    text = extract_pdf_text(pdf_path)
    if not text:
        return {}

    if not _is_freight_invoice(text):
        return {}

    data = {}

    # Invoice number: "INVOICE 340" or "Invoice No: 340" or "Invoice #340"
    m = re.search(r'INVOICE\s*(?:NO\.?|#)?\s*[:\s]*(\d+)', text, re.IGNORECASE)
    if m:
        data['invoice_number'] = m.group(1)

    # Date: various formats
    m = re.search(r'DATE[:\s]*([\d./]+\d{4}|\d{4}[./\-]\d{2}[./\-]\d{2})', text, re.IGNORECASE)
    if m:
        data['date'] = m.group(1)

    # Landing charges: "LANDING CHARGES FOR 03 PALLETS 600.00"
    # or "LANDING CHARGES FOR 03 PALLETS" (amount on same line or missing)
    m = re.search(r'LANDING\s+CHARGES\s+FOR\s+(\d+)\s+PALLET', text, re.IGNORECASE)
    if m:
        data['packages'] = m.group(1)
    m2 = re.search(r'LANDING\s+CHARGES.*?([\d,]+\.\d{2})', text, re.IGNORECASE)
    if m2:
        data['landing_charges'] = m2.group(1).replace(',', '')

    # Freight amount: "FREIGHT FROM X TO Y FOR 3,911.25" or "FREIGHT ... 3,911.25"
    m = re.search(r'(?:TO\s+)?FREIGHT\s+(?:FROM\s+)?.*?([\d,]+\.\d{2})\s*$', text, re.IGNORECASE | re.MULTILINE)
    if m:
        data['freight'] = m.group(1).replace(',', '')

    # Total: "TOTAL DUE 4,511.25"
    m = re.search(r'TOTAL\s+(?:DUE|AMOUNT|PAYABLE)\s+([\d,]+\.\d{2})', text, re.IGNORECASE)
    if m:
        data['total'] = m.group(1).replace(',', '')

    # Consignee: line after "To:" or before "AGENT:"
    m = re.search(r'(?:^|\n)\s*(.+?)\s*\n.*?(?:AGENT|DESCRIPTION)', text, re.IGNORECASE)
    if m:
        name = m.group(1).strip()
        if name and not re.match(r'^(?:TO|DATE|INVOICE)', name, re.IGNORECASE):
            data['consignee'] = name

    # Origin country: extract from "FREIGHT FROM {place} TO {place}"
    m = re.search(r'FREIGHT\s+FROM\s+(.+?)\s+TO\s+', text, re.IGNORECASE)
    if m:
        origin_place = m.group(1).strip().upper()
        # Map common place names to ISO country codes
        _PLACE_TO_COUNTRY = {
            'ST. MARTIN': 'SX', 'ST MARTIN': 'SX', 'SAINT MARTIN': 'SX',
            'SINT MAARTEN': 'SX', 'ST. MAARTEN': 'SX', 'SXM': 'SX',
            'CURACAO': 'CW', 'CURAÇAO': 'CW',
            'ARUBA': 'AW',
            'TRINIDAD': 'TT', 'PORT OF SPAIN': 'TT',
            'BARBADOS': 'BB', 'BRIDGETOWN': 'BB',
            'GRENADA': 'GD', 'ST. GEORGE': 'GD',
            'JAMAICA': 'JM', 'KINGSTON': 'JM',
            'PANAMA': 'PA', 'COLON': 'PA',
            'MIAMI': 'US', 'FLORIDA': 'US', 'NEW YORK': 'US',
            'COLOMBIA': 'CO', 'CARTAGENA': 'CO',
            'GUYANA': 'GY', 'GEORGETOWN': 'GY',
            'SURINAME': 'SR', 'PARAMARIBO': 'SR',
        }
        for place, code in _PLACE_TO_COUNTRY.items():
            if place in origin_place:
                data['origin_country'] = code
                break

    return data
