#!/usr/bin/env python3
"""
Stage 2: Item Parser
Parses extracted text into structured line items.
"""

import json
import os
import re
from typing import Any, Dict, List


def run(input_path: str, output_path: str, config: Dict = None, context: Dict = None) -> Dict:
    """Parse extracted data into structured line items."""
    if not input_path or not os.path.exists(input_path):
        return {'status': 'error', 'error': f'Input not found: {input_path}'}

    with open(input_path) as f:
        data = json.load(f)

    result = {
        'status': 'success',
        'items': [],
        'invoice_metadata': {}
    }

    invoices = data.get('invoices', [data])
    for invoice in invoices:
        result['invoice_metadata'] = {
            'invoice_number': invoice.get('invoice_number'),
            'date': invoice.get('date'),
            'supplier': invoice.get('supplier'),
            'supplier_address': invoice.get('supplier_address'),
            'country_code': invoice.get('country_code'),
            'customer_name': invoice.get('customer_name'),
            'customer_code': invoice.get('customer_code'),
            'shipped_via': invoice.get('shipped_via'),
            'total': invoice.get('total'),
            'freight': invoice.get('freight', 0),
            'tax': invoice.get('tax', 0),
            'other_cost': invoice.get('tax', 0),  # Tax goes to other_cost for XLSX
            'discount': invoice.get('discount', 0),
        }

        raw_items = invoice.get('items', [])
        for i, raw in enumerate(raw_items):
            item = normalize_item(raw, i)
            if item:
                result['items'].append(item)

    result['total_items'] = len(result['items'])

    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(result, f, indent=2)

    return result


def normalize_item(raw: Dict, index: int) -> Dict:
    """Normalize a raw item into a standard format."""
    desc = raw.get('description', '')
    if not desc or not desc.strip():
        return None

    sku = raw.get('sku', '')
    desc_upper = desc.upper()

    # Detect bundled items (sets, displays, testers)
    bundle_info = detect_bundle(sku, desc_upper)

    item = {
        'index': index,
        'description': desc.strip(),
        'quantity': parse_number(raw.get('quantity', 1)),
        'unit_cost': parse_number(raw.get('unit_cost', 0)),
        'total_cost': parse_number(raw.get('total_cost', 0)),
        'sku': sku,
        'original': raw,
    }

    # Add bundle metadata if detected
    if bundle_info:
        item['is_bundle'] = True
        item['bundle_type'] = bundle_info['type']
        item['billable'] = bundle_info['billable']
        if bundle_info.get('references'):
            item['bundle_references'] = bundle_info['references']

    return item


def detect_bundle(sku: str, desc_upper: str) -> Dict:
    """
    Detect if an item is a bundled item (set, display, tester).

    Bundled items are:
    - ST-*: Starter kits/sets - contain same items as individual SKUs, NOT billable
    - DP-*: Display units - promotional displays, typically $0
    - TST-*: Tester sets - samples for display, typically $0
    - T-*: Individual testers

    Returns bundle info dict or None if not a bundle.
    """
    sku_upper = sku.upper()

    # Check SKU prefixes for bundle types
    bundle_prefixes = {
        'ST-': {'type': 'starter_kit', 'billable': False},  # Starter kits duplicate individual items
        'DP-': {'type': 'display', 'billable': True},       # Displays are usually $0 anyway
        'TST-': {'type': 'tester_set', 'billable': True},   # Tester sets are usually $0
        'T-': {'type': 'tester', 'billable': True},         # Individual testers
    }

    for prefix, info in bundle_prefixes.items():
        if sku_upper.startswith(prefix):
            result = info.copy()
            # Try to extract referenced item codes from description
            refs = extract_bundle_references(desc_upper, sku_upper)
            if refs:
                result['references'] = refs
            return result

    # Check description patterns for sets/kits not caught by SKU prefix
    set_patterns = ['SET FOR ', 'KIT FOR ', 'STARTER KIT', 'DISPLAY SET']
    for pattern in set_patterns:
        if pattern in desc_upper:
            refs = extract_bundle_references(desc_upper, sku_upper)
            return {
                'type': 'set',
                'billable': False,  # Sets that bundle other items shouldn't add to total
                'references': refs if refs else []
            }

    return None


def extract_bundle_references(desc_upper: str, sku_upper: str) -> List[str]:
    """
    Extract referenced item codes from bundle descriptions.

    Examples:
    - "SET FOR MFHG01-04" -> ["MFHG01", "MFHG02", "MFHG03", "MFHG04"]
    - "DISPLAY FOR SBGS11-16" -> ["SBGS11", "SBGS12", ..., "SBGS16"]
    - "SET FOR I-BROW LAMINATOR" -> try to match MEBG* codes
    """
    refs = []

    # Pattern 1: Explicit range like "MFHG01-04" or "SBGS11-16"
    range_pattern = r'\b([A-Z]+)(\d{2})-(\d{2})\b'
    matches = re.findall(range_pattern, desc_upper)
    for prefix, start, end in matches:
        try:
            start_num = int(start)
            end_num = int(end)
            for i in range(start_num, end_num + 1):
                refs.append(f"{prefix}{i:02d}")
        except ValueError:
            pass

    # Pattern 2: If SKU is like "ST-MFHG", look for items like "MFHG01", "MFHG02"
    if sku_upper.startswith(('ST-', 'DP-', 'TST-', 'T-')):
        # Extract the base code after the prefix
        base_match = re.match(r'^(?:ST-|DP-|TST-|T-)([A-Z]+\d*)', sku_upper)
        if base_match:
            base_code = base_match.group(1)
            # The references would be items like {base_code}01, {base_code}02, etc.
            # We can't know the exact items without the full item list, so just store the base
            if not refs:
                refs.append(f"{base_code}*")  # Wildcard to match during classification

    return refs


def parse_number(val) -> float:
    """Parse a value to float, handling various formats."""
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        cleaned = val.replace(',', '').replace('$', '').replace(' ', '').strip()
        try:
            return float(cleaned)
        except ValueError:
            return 0.0
    return 0.0
