#!/usr/bin/env python3
"""
Stage 5: Grouping Engine
Groups items by tariff code per grouping configuration.
"""

import json
import os
from collections import OrderedDict
from typing import Any, Dict, List


def run(input_path: str, output_path: str, config: Dict = None, context: Dict = None) -> Dict:
    """Group items by tariff code."""
    if not input_path or not os.path.exists(input_path):
        return {'status': 'error', 'error': f'Input not found: {input_path}'}

    with open(input_path) as f:
        data = json.load(f)

    items = data.get('items', data if isinstance(data, list) else [])
    metadata = data.get('invoice_metadata', {})

    # Group items by tariff code, preserving order of first appearance
    groups = OrderedDict()
    bundle_count = 0
    non_billable_total = 0.0

    for item in items:
        code = item.get('classification', {}).get('code', 'UNKNOWN')
        if code not in groups:
            groups[code] = {
                'tariff_code': code,
                'category': item.get('classification', {}).get('category', 'PRODUCTS'),
                'items': [],
                'sum_quantity': 0,
                'sum_total_cost': 0,
            }

        groups[code]['items'].append(item)

        # Check if item is billable (default True if not specified)
        is_billable = item.get('billable', True)
        is_bundle = item.get('is_bundle', False)

        if is_bundle:
            bundle_count += 1

        # Only add to totals if item is billable
        if is_billable:
            groups[code]['sum_quantity'] += item.get('quantity', 0)
            groups[code]['sum_total_cost'] += item.get('total_cost', 0)
        else:
            non_billable_total += item.get('total_cost', 0)

    # Calculate group averages
    for group in groups.values():
        qty = group['sum_quantity']
        total = group['sum_total_cost']
        group['average_unit_cost'] = total / qty if qty > 0 else 0
        group['item_count'] = len(group['items'])

    result = {
        'status': 'success',
        'invoice_metadata': metadata,
        'groups': list(groups.values()),
        'total_groups': len(groups),
        'total_items': len(items),
        'bundle_count': bundle_count,
        'non_billable_total': round(non_billable_total, 2),
    }

    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(result, f, indent=2)

    return result
