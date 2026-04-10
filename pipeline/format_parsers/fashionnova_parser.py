#!/usr/bin/env python3
"""
Custom format parser for FashionNova invoices
Handles block-based extraction with Price: and Qty: on separate lines
"""

import re
from typing import Dict, List, Any


def parse(text: str) -> Dict[str, Any]:
    """
    Parse FashionNova invoice with block-based item structure.

    Format:
        Product Name
        Price: $XX.XX
        Size: XX
        Qty: X

    Args:
        text: Raw invoice text

    Returns:
        Parsed invoice data with metadata and items
    """
    result = {
        'status': 'success',
        'invoices': [{
            'items': [],
            'raw_text': text[:5000],
            'tables': []
        }]
    }

    # Extract metadata
    invoice_number = extract_invoice_number(text)
    date = extract_date(text)
    total = extract_total(text)
    supplier = "FashionNova"

    result['invoices'][0].update({
        'invoice_number': invoice_number,
        'date': date,
        'supplier': supplier,
        'total': total,
        'freight': 0,
        'tax': 0,
        'discount': 0
    })

    # Extract items using block strategy
    items = extract_items_block(text)
    result['invoices'][0]['items'] = items

    return result


def extract_invoice_number(text: str) -> str:
    match = re.search(r'Order #\s*(\d+)', text)
    return match.group(1) if match else None


def extract_date(text: str) -> str:
    match = re.search(r'Date Placed:\s*([A-Za-z]+\s+\d+,\s+\d{4})', text)
    return match.group(1) if match else None


def extract_total(text: str) -> float:
    match = re.search(r'Total:\s*\$?([\d,]+\.\d{2})', text)
    if match:
        return float(match.group(1).replace(',', ''))
    return 0.0


def extract_items_block(text: str) -> List[Dict[str, Any]]:
    """
    Extract items using block-based pattern matching.

    Finds patterns like:
        Line i:   Product Name
        Line i+1: Price: $XX.XX
        Line i+2: Size: XX
        Line i+3: Qty: X
    """
    items = []
    lines = text.split('\n')
    item_idx = 0

    i = 0
    while i < len(lines):
        line = lines[i].strip()

        # Skip empty lines and headers
        if not line or should_skip_line(line):
            i += 1
            continue

        # Check if this looks like a product name line
        # - Starts with capital letter
        # - No colons (not metadata)
        # - At least 10 characters
        if looks_like_product(line):
            # Check if next line is a Price: line
            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if next_line.startswith('Price:'):
                    # Extract price
                    price_match = re.search(r'Price:\s*\$?([\d,]+\.\d{2})', next_line)
                    if price_match:
                        unit_price = float(price_match.group(1))

                        # Look for Qty 2 lines down
                        quantity = 1
                        if i + 3 < len(lines):
                            qty_line = lines[i + 3].strip()
                            if qty_line.startswith('Qty:'):
                                qty_match = re.search(r'Qty:\s*(\d+)', qty_line)
                                if qty_match:
                                    quantity = int(qty_match.group(1))

                        # Create item
                        items.append({
                            'description': line.strip(),
                            'unit_cost': unit_price,
                            'quantity': quantity,
                            'total_cost': round(unit_price * quantity, 2),
                            'sku': f'FN-{item_idx + 1}'
                        })
                        item_idx += 1

        i += 1

    return items


def should_skip_line(line: str) -> bool:
    """Check if line should be skipped."""
    skip_prefixes = [
        'Order #', 'Date Placed:', 'Subtotal:', 'Total:',
        'Shipping:', 'Discount:', 'Payment method:',
        'Delivery Address:', 'Billing Address:',
        'FASHION', 'Visit:', 'Shop:',
    ]

    for prefix in skip_prefixes:
        if line.startswith(prefix):
            return True

    return False


def looks_like_product(line: str) -> bool:
    """Check if line looks like a product name."""
    # Must be at least 10 characters
    if len(line) < 10:
        return False

    # Must not contain colon (not metadata)
    if ':' in line:
        return False

    # Must start with letter
    if not line[0].isalpha():
        return False

    # Should contain at least one letter
    if not re.search(r'[A-Za-z]', line):
        return False

    # Should not be price-only
    if re.match(r'^\$\d+\.\d{2}$', line):
        return False

    return True
