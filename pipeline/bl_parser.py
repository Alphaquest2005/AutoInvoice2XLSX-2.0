#!/usr/bin/env python3
"""
Bill of Lading PDF Parser

Parses Tropical Shipping BL PDFs to extract:
  - Cost breakdown (freight, landing, insurance, total)
  - Per-shipment receipts (packages, weight, customer invoice refs)
  - Grand totals

Pure data extraction — no side effects, no file writing.

Usage:
    from bl_parser import parse_bl_pdf, match_invoice_to_bl
    bl_data = parse_bl_pdf('/path/to/bl.pdf')
    match = match_invoice_to_bl('718123', '121202', bl_data)
"""

import logging
import re
from typing import Dict, List, Optional

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

logger = logging.getLogger(__name__)

# Known cost line items in BL PDFs (key → normalized field name)
COST_LINE_ITEMS = {
    'OCEAN FREIGHT': 'ocean_freight',
    'TERMINAL HANDLING': 'terminal_handling',
    'LANDING': 'landing',
    'BILL OF LADING PROCESSING': 'bl_processing',
    'BILL OF LADING': 'bl_processing',
    'ELECTRONIC EXPORT INFORMATION': 'eei',
    'BUNKER SURCHARGE': 'bunker_surcharge',
    'HAZARDOUS CARGO SURCHARGE': 'hazardous_cargo',
    'OPERATIONAL SERVICE': 'operational_service',
    'PEAK SEASON SURCHARGE': 'peak_season',
    'PALLET SURCHARGE': 'pallet_surcharge',
    'SECURITY SURCHARGE': 'security_surcharge',
    'INSURANCE PREMIUM': 'insurance_premium',
    'SP HANDLING FEE': 'sp_handling_fee',
    'TOTAL USD': 'total_usd',
    'USD': 'total_usd',
}


def _parse_bl_pdf_impl(pdf_path: str) -> Dict:
    """
    Parse a Tropical Shipping BL PDF and return structured data.

    Args:
        pdf_path: Path to the BL PDF file

    Returns:
        Dict with keys: bl_number, consignee, cost_breakdown,
        shipments (list), grand_total
    """
    if not pdfplumber:
        raise ImportError("pdfplumber is required. Run: pip install pdfplumber")

    text = _extract_text(pdf_path)
    if not text:
        logger.error(f"No text extracted from BL PDF: {pdf_path}")
        return _empty_bl_data()

    bl_number = _extract_bl_number(text)
    consignee = _extract_consignee(text)
    cost_breakdown = _extract_cost_breakdown(text)
    shipments = _extract_shipments(text)
    grand_total = _extract_grand_total(text)

    # Fallback: if GRAND TOTAL returned 0s but shipments have data, sum shipments
    if grand_total.get('weight_kg', 0) == 0 and shipments:
        sum_pkgs = sum(s.get('packages', 0) for s in shipments)
        sum_lbs = sum(s.get('weight_lbs', 0) for s in shipments)
        sum_kg = sum(s.get('weight_kg', 0) for s in shipments)
        if sum_kg > 0 or sum_lbs > 0:
            grand_total = {
                'packages': sum_pkgs,
                'weight_lbs': sum_lbs,
                'weight_kg': sum_kg,
            }
            logger.info(f"Grand total from shipments: {sum_pkgs} pkg, {sum_kg} kg")

    # Fallback: extract piece count from text (e.g. "165 PIECE(S)")
    if grand_total.get('packages', 0) <= 1 and shipments:
        import re
        pieces_match = re.search(r'(\d+)\s+PIECE\(S\)', text, re.IGNORECASE)
        if pieces_match:
            piece_count = int(pieces_match.group(1))
            if piece_count > grand_total.get('packages', 0):
                grand_total['pieces'] = piece_count
                logger.info(f"Pieces from BL text: {piece_count}")

    logger.info(f"BL {bl_number}: {len(shipments)} shipments, "
                f"total ${cost_breakdown.get('total_usd', 0):.2f}")

    return {
        'bl_number': bl_number,
        'consignee': consignee,
        'cost_breakdown': cost_breakdown,
        'shipments': shipments,
        'grand_total': grand_total,
    }


def parse_bl_pdf(pdf_path: str) -> Dict:
    """Public wrapper that times :func:`_parse_bl_pdf_impl` via ``perf_log``."""
    try:
        import perf_log as _perf
    except Exception:
        _perf = None
    import os as _os
    import time as _time
    t0 = _time.monotonic()
    base = _os.path.basename(pdf_path) if pdf_path else ""
    err = None
    n_shipments = 0
    try:
        result = _parse_bl_pdf_impl(pdf_path)
        try:
            n_shipments = len((result or {}).get('shipments', []) or [])
        except Exception:
            pass
        return result
    except BaseException as e:  # noqa: BLE001
        err = type(e).__name__
        raise
    finally:
        dur = _time.monotonic() - t0
        if _perf is not None:
            try:
                _perf.event(
                    "bl_parser.parse_bl_pdf", dur,
                    pdf=base, n_shipments=n_shipments,
                    error=err if err else None,
                )
            except Exception:
                pass


def match_invoice_to_bl(invoice_num: str, po_refs: List[str],
                         bl_data: Dict) -> Dict:
    """
    Match an invoice to BL shipments by invoice number or PO refs.

    Searches all BL shipment customer invoice refs for a match.
    If an invoice appears in multiple shipments, sums packages/weight.

    Args:
        invoice_num: Parsed invoice number (e.g. '718123', 'INV000443447')
        po_refs: List of PO reference numbers to try (e.g. ['121202'])
        bl_data: Parsed BL data from parse_bl_pdf()

    Returns:
        Dict with keys: packages (int), weight_kg (int), matched (bool)
    """
    shipments = bl_data.get('shipments', [])
    if not shipments:
        return {'packages': 1, 'weight_kg': 0, 'matched': False}

    # Build a flat list of all (normalized_ref, shipment_index) pairs
    ref_to_shipments = _build_ref_index(shipments)

    # Try invoice number first, then PO refs
    candidates_to_try = [invoice_num] + (po_refs or [])
    matched_indices = set()

    for candidate in candidates_to_try:
        if not candidate:
            continue
        normalized = _normalize_invoice_ref(candidate)
        for ref, idx in ref_to_shipments:
            if normalized == ref:
                matched_indices.add(idx)

    if not matched_indices:
        return {'packages': 1, 'weight_kg': 0, 'matched': False,
                'matched_indices': set()}

    # Sum across all matching shipments
    total_packages = 0
    total_kg = 0
    for idx in matched_indices:
        s = shipments[idx]
        total_packages += s.get('packages', 0)
        total_kg += s.get('weight_kg', 0)

    return {
        'packages': total_packages or 1,
        'weight_kg': total_kg,
        'matched': True,
        'matched_indices': matched_indices,
    }


# ─── Internal helpers ─────────────────────────────────────


def _extract_text(pdf_path: str) -> str:
    """Extract full text from a BL PDF via the unified hybrid OCR pipeline.

    Delegates to ``multi_ocr.extract_text`` which handles the full
    pdfplumber → (preprocess × engine) matrix → consensus workflow with
    caching. The digital-PDF short-circuit inside ``extract_text`` makes
    this path cheap for already-digital BLs.
    """
    try:
        import sys
        import os as _os
        script_dir = _os.path.dirname(_os.path.abspath(__file__))
        if script_dir not in sys.path:
            sys.path.insert(0, script_dir)
        import multi_ocr
    except ImportError as e:
        logger.error(f"multi_ocr unavailable, cannot extract BL text: {e}")
        return ""

    try:
        result = multi_ocr.extract_text(pdf_path)
        return result.text or ""
    except Exception as e:
        logger.error(f"multi_ocr extract_text failed for {pdf_path}: {e}")
        return ""


def _extract_bl_number(text: str) -> str:
    """Extract BL number from header (e.g. 'TSCW18489131', 'HBL198142')."""
    # Try specific carrier patterns
    for pattern in [
        r'(TSCW\d+)',               # Tropical Shipping container
        r'(TS\s*\d{7,})',           # Tropical Shipping
        r'(HBL\d+)',                # Caribconex house BL
        r'(MEDU\d+[A-Z]*\d*)',      # Mediterranean Shipping
        r'(HDMU[A-Z0-9]+)',         # Hyundai
        r'(MAEU\d+)',               # Maersk
        r'(COSU\d+)',               # COSCO
    ]:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    # Generic: B/L NO. XXXX or BL NO. XXXX
    for pattern in [
        r'B/L\s*(?:NO\.?|NUMBER|#)[:\s]*([A-Z0-9]+)',
        r'BL\s*(?:NO\.?|NUMBER|#)[:\s]*([A-Z0-9]+)',
    ]:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return ""


def _extract_consignee(text: str) -> str:
    """Extract consignee name from BL text."""
    # Tropical format: "CONSIGNEE(NOT NEGOTIABLE...) 101692566 FORWARDING AGENT...\nATLANTIC HEALTH PHARMACY"
    # The consignee label and extra info are on the same line, name is on the NEXT line.
    m = re.search(
        r'CONSIGNEE\s*\(NOT NEGOTIABLE[^)]*\)[^\n]*\n\s*(.+)',
        text, re.IGNORECASE
    )
    if m:
        name = m.group(1).strip().split('\n')[0].strip()
        if name and len(name) > 2 and not re.match(r'^\d', name):
            return name

    # Tropical format: CONSIGNEE header on its own line followed by name
    m = re.search(r'CONSIGNEE\s*\n(.+?)(?:\n|PO BOX)', text, re.DOTALL)
    if m:
        name = m.group(1).strip()
        # Skip if it's an address (starts with digits)
        if not re.match(r'^\d', name):
            return name

    # Caribconex: consignee name after INV: line(s)
    # INV refs may span multiple lines before the consignee name
    # "INV:...\n113-...\nHILLS AND VALLEY PHARMACY CARIBCONEX. LLC"
    inv_match = re.search(r'INV:[^\n]+(?:\n[\d][^\n]*)*\n([A-Z][A-Z &.\'-]+?)(?:\s+CARIBCONEX|\s+CAR\b|\s+Tel:|\n)', text)
    if inv_match:
        return inv_match.group(1).strip()

    return ""


def _extract_cost_breakdown(text: str) -> Dict:
    """
    Extract the cost breakdown table from the BL.

    Matches lines like:
        OCEAN FREIGHT - LCL    370.14
        LANDING                298.50
        TOTAL USD            1,959.22
    Or split across lines (Caribconex):
        USD
        244.67
    """
    costs = {}

    lines = text.split('\n')
    for i, line in enumerate(lines):
        line = line.strip()
        # Match pattern: TEXT  amount (with possible commas)
        m = re.match(r'^(.+?)\s+([\d,]+\.\d{2})\s*$', line)
        if m:
            desc = m.group(1).strip()
            amount = float(m.group(2).replace(',', ''))
            for key, field in COST_LINE_ITEMS.items():
                if key in desc.upper():
                    costs[field] = amount
                    break
            continue

        # Check for "USD 244.67 ..." or "TOTAL USD 244.67 ..." with trailing text
        usd_match = re.match(r'^(?:TOTAL\s+)?USD\s+([\d,]+\.\d{2})', line, re.IGNORECASE)
        if usd_match:
            costs['total_usd'] = float(usd_match.group(1).replace(',', ''))
            continue

        # Check for split lines: "USD" on one line, "244.67" on the next
        if line.upper() in ('USD', 'TOTAL USD') and i + 1 < len(lines):
            next_line = lines[i + 1].strip()
            amt_match = re.match(r'^([\d,]+\.\d{2})', next_line)
            if amt_match:
                costs['total_usd'] = float(amt_match.group(1).replace(',', ''))

    return costs


def _extract_shipments(text: str) -> List[Dict]:
    """
    Extract per-shipment receipt data.

    Tropical format: Each shipment starts with 'SHIPMENT NO NNNNNNNN'.
    Caribconex/generic: Single shipment with INV: refs in header and WR# entries.
    """
    shipments = []

    # Split text into shipment sections (Tropical format)
    sections = re.split(r'(?=SHIPMENT NO \d+)', text)

    for section in sections:
        shipment_match = re.match(r'SHIPMENT NO (\d+)', section)
        if not shipment_match:
            continue

        shipment_no = shipment_match.group(1)
        shipper = _extract_shipper(text, shipment_no)

        # Extract TOTAL line: "TOTAL  NNN  NNN  NN.N  N.NNN"
        total_match = re.search(
            r'TOTAL\s+(\d+)\s+(\d+)\s+([\d.]+)\s+([\d.]+)',
            section
        )
        if total_match:
            weight_lbs = int(total_match.group(1))
            weight_kg = int(total_match.group(2))
        else:
            weight_lbs = 0
            weight_kg = 0

        packages = _count_packages(section)
        invoice_refs = _extract_invoice_refs(section)

        shipments.append({
            'shipment_no': shipment_no,
            'shipper': shipper,
            'packages': packages,
            'weight_lbs': weight_lbs,
            'weight_kg': weight_kg,
            'invoice_refs': invoice_refs,
        })

    # Fallback for non-Tropical BOLs (Caribconex, etc.)
    # If no SHIPMENT NO sections found, build a single shipment from the whole BOL
    if not shipments:
        shipments = _extract_generic_shipments(text)

    return shipments


def _extract_generic_shipments(text: str) -> List[Dict]:
    """
    Extract shipment data from non-Tropical BOLs (Caribconex, etc.).

    Looks for:
    - INV: refs in header
    - WR# entries with package counts
    - Weight from "NNN.NN kg" or "NNN.NN lb" patterns
    - Total packages from "N TOTAL" line
    """
    # Extract invoice refs from "INV:" line
    invoice_refs = []
    inv_match = re.search(r'INV:\s*(.+)', text)
    if inv_match:
        ref_text = inv_match.group(1).strip()
        for ref in re.split(r'[,\s]+', ref_text):
            ref = ref.strip().rstrip(',')
            if ref and re.match(r'^[A-Za-z0-9-]+$', ref):
                invoice_refs.append(ref)

    # Extract packages from "N TOTAL" line
    packages = 0
    total_match = re.search(r'(\d+)\s+TOTAL\b', text)
    if total_match:
        packages = int(total_match.group(1))

    # Fallback: count BOX/CARTON lines
    if not packages:
        for m in re.finditer(r'(\d+)\s+(?:BOX|CARTON|PALLET|BUNDLE)', text, re.IGNORECASE):
            packages += int(m.group(1))

    # Extract weight in kg
    weight_kg = 0
    weight_lbs = 0
    kg_match = re.search(r'([\d,.]+)\s*kg\b', text, re.IGNORECASE)
    if kg_match:
        weight_kg = int(float(kg_match.group(1).replace(',', '')))
    lb_match = re.search(r'([\d,.]+)\s*lb', text, re.IGNORECASE)
    if lb_match:
        weight_lbs = int(float(lb_match.group(1).replace(',', '')))

    # If only lbs, convert
    if weight_lbs and not weight_kg:
        weight_kg = int(weight_lbs * 0.453592)
    if weight_kg and not weight_lbs:
        weight_lbs = int(weight_kg / 0.453592)

    if not packages:
        packages = 1

    return [{
        'shipment_no': '1',
        'shipper': '',
        'packages': packages,
        'weight_lbs': weight_lbs,
        'weight_kg': weight_kg,
        'invoice_refs': invoice_refs,
    }]


def _extract_shipper(text: str, shipment_no: str) -> str:
    """
    Extract shipper name for a given shipment.

    The shipper always appears 1-2 lines before SHIPMENT NO:
        SHIPPER TOHATSU
        AMERICA CORP
        SHIPMENT NO 14106181 ...
    """
    pos = text.find(f'SHIPMENT NO {shipment_no}')
    if pos < 0:
        return ""

    preceding = text[:pos]
    lines = [l.strip() for l in preceding.split('\n') if l.strip()]

    # The SHIPPER line is within the last few lines before SHIPMENT NO
    # Walk backwards, collect name parts until we hit "SHIPPER X"
    # Skip page headers and BL boilerplate
    skip_prefixes = ("SHIPPER'S", "CARRIER", "NOEEI", "NON-NEGOTIABLE",
                     "www.", "Company Limited", "Page ", "Tropical",
                     "BILL OF LADING", "TSCW")
    name_parts = []
    for line in reversed(lines[-5:]):
        if any(line.startswith(p) for p in skip_prefixes):
            continue
        if line.startswith('SHIPPER '):
            name_after = line[len('SHIPPER '):].strip()
            if name_after:
                name_parts.insert(0, name_after)
            break
        else:
            # Continuation line (e.g., "AMERICA CORP", "MARINE INC", "LLC")
            name_parts.insert(0, line)

    return ' '.join(name_parts).strip().rstrip(',')


def _count_packages(section: str) -> int:
    """
    Count packages in a shipment section.

    Package lines have a quantity before the package type keyword:
        SHIPMENT NO 14106181 1 CARTON(S) MARINE SUPPLIES 15 7 ...
        BSIU8260634 1 BOX(S) MARINE PARTS
        1323105 1 BOX(S) MARINE PARTS
        SHIPMENT NO 14140206 2 BANDED BUNDLE(S) MARINE SUPPLIES

    We look for "N PACKAGE_TYPE" patterns, where PACKAGE_TYPE is
    CARTON/BOX/BANDED/SW/PALLET/SW-SKID.
    Only count lines BEFORE the TOTAL line.
    """
    pkg_pattern = re.compile(
        r'(\d+)\s+(?:CARTON\(S\)|BOX\(S\)|BANDED\s+(?:PALLET|BUNDLE)\(S\)|'
        r'SW/SKID\(S\)|SW/PLT\(S\)|SW/BANDED/PLT\(S\)|PALLET\(S\)|'
        r'(?:BLACK|WHITE|BROWN)?/?SW/?BND/?PLT|'
        r'(?:BLACK|WHITE|BROWN)?/?SW/?BNDL/?PLT|'
        r'BUNDLE\(S\)|CRATE\(S\)|DRUM\(S\)|SKID\(S\)|PKG\(S\))',
        re.IGNORECASE
    )
    count = 0
    for line in section.split('\n'):
        line = line.strip()
        # Stop at TOTAL line
        if re.match(r'.*\bTOTAL\s+\d+', line):
            break
        # Find package quantities on this line
        for m in pkg_pattern.finditer(line):
            count += int(m.group(1))
    return count or 1  # Default to 1 if no packages found


def _extract_invoice_refs(section: str) -> List[str]:
    """
    Extract CUSTOMER INVOICE NO. references from a shipment section.

    Handles:
    - Single: "CUSTOMER INVOICE NO. 8440870"
    - Multiple comma-separated: "CUSTOMER INVOICE NO. 8444174, 8436710"
    - Spanning lines: "CUSTOMER INVOICE NO. 4003163, 4004727, 4003146,\n4003101, 4003863,"
    - Continuation: "CUSTOMER INVOICE NO. 4003469" (on next line)
    """
    refs = []

    # Find all CUSTOMER INVOICE NO. lines and their continuation
    lines = section.split('\n')
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        m = re.match(r'CUSTOMER INVOICE NO\.\s*(.+)', line)
        if m:
            ref_text = m.group(1).strip()

            # Check for continuation on next lines (comma-separated refs
            # that wrap to next line, or another CUSTOMER INVOICE NO. line)
            while i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                # If next line starts with CUSTOMER INVOICE NO., break
                if next_line.startswith('CUSTOMER INVOICE NO.'):
                    break
                # If next line looks like continuation of refs (starts with
                # digits or comma)
                if re.match(r'^[\d,\s]', next_line) and not next_line.startswith('CLAUSES'):
                    ref_text += ' ' + next_line
                    i += 1
                else:
                    break

            # Parse individual refs from the collected text
            # Remove trailing commas, split on comma/space
            for ref in re.split(r'[,\s]+', ref_text):
                ref = ref.strip().rstrip(',')
                if ref and re.match(r'^[A-Za-z0-9]+$', ref):
                    refs.append(ref)
        i += 1

    return refs


def _extract_grand_total(text: str) -> Dict:
    """Extract GRAND TOTAL line: packages, LBS, KG."""
    # Try standard GRAND TOTAL format first (Tropical)
    m = re.search(r'GRAND TOTAL\s+(\d+)\s+(\d+)\s+(\d+)', text)
    if m:
        return {
            'packages': int(m.group(1)),
            'weight_lbs': int(m.group(2)),
            'weight_kg': int(m.group(3)),
        }

    # Try Tropical Shipping format where pieces and label are on separate lines
    pieces_match = re.search(r'(\d+)\s+PCS', text, re.IGNORECASE)
    if pieces_match and 'total pieces' in text.lower():
        packages = int(pieces_match.group(1))
        weight_match = re.search(r'GROSS WEIGHT\s+(\d+)', text, re.IGNORECASE)
        weight_lbs = int(weight_match.group(1)) if weight_match else 0
        weight_kg = int(weight_lbs * 0.453592) if weight_lbs else 0
        return {
            'packages': packages,
            'weight_lbs': weight_lbs,
            'weight_kg': weight_kg,
        }

    # Caribconex / generic: "N TOTAL" + weight in kg/lb
    total_match = re.search(r'(\d+)\s+TOTAL\b', text)
    if total_match:
        packages = int(total_match.group(1))
        weight_kg = 0
        weight_lbs = 0
        kg_match = re.search(r'([\d,.]+)\s*kg\b', text, re.IGNORECASE)
        if kg_match:
            try:
                weight_kg = int(float(kg_match.group(1).replace(',', '')))
            except ValueError:
                weight_kg = 0
        lb_match = re.search(r'([\d,.]+)\s*lb', text, re.IGNORECASE)
        if lb_match:
            try:
                weight_lbs = int(float(lb_match.group(1).replace(',', '')))
            except ValueError:
                weight_lbs = 0
        if weight_lbs and not weight_kg:
            weight_kg = int(weight_lbs * 0.453592)
        if weight_kg and not weight_lbs:
            weight_lbs = int(weight_kg / 0.453592)
        return {
            'packages': packages,
            'weight_lbs': weight_lbs,
            'weight_kg': weight_kg,
        }

    return {'packages': 0, 'weight_lbs': 0, 'weight_kg': 0}


def _normalize_invoice_ref(ref: str) -> str:
    """
    Normalize an invoice reference for matching.

    Strips common prefixes: INV0, INV000, INV
    So 'INV0718123' → '718123', 'INV31668' → '31668'
    But 'INV000443447' stays as-is if it matches directly.
    """
    if not ref:
        return ""
    ref = ref.strip()
    # Strip leading INV prefix and any leading zeros after it
    stripped = re.sub(r'^INV0*', '', ref)
    return stripped or ref


def _build_ref_index(shipments: List[Dict]) -> List[tuple]:
    """
    Build a flat list of (normalized_ref, shipment_index) for fast lookup.
    """
    index = []
    for i, shipment in enumerate(shipments):
        for ref in shipment.get('invoice_refs', []):
            normalized = _normalize_invoice_ref(ref)
            index.append((normalized, i))
    return index


def _empty_bl_data() -> Dict:
    """Return empty BL data structure."""
    return {
        'bl_number': '',
        'consignee': '',
        'cost_breakdown': {},
        'shipments': [],
        'grand_total': {'packages': 0, 'weight_lbs': 0, 'weight_kg': 0},
    }
