#!/usr/bin/env python3
"""
Stage 4: Code Validator
Validates tariff codes against CET and applies auto-fixes.
"""

import json
import os
from typing import Any, Dict, List, Optional


def run(input_path: str, output_path: str, config: Dict = None, context: Dict = None) -> Dict:
    """Validate classification codes against CET reference."""
    if not input_path or not os.path.exists(input_path):
        return {'status': 'error', 'error': f'Input not found: {input_path}'}

    base_dir = context.get('base_dir', '.') if context else '.'

    # Load invalid codes mapping
    invalid_codes_path = os.path.join(base_dir, 'rules', 'invalid_codes.json')
    invalid_codes = {}
    if os.path.exists(invalid_codes_path):
        with open(invalid_codes_path) as f:
            invalid_codes = json.load(f)

    with open(input_path) as f:
        data = json.load(f)

    items = data.get('items', data if isinstance(data, list) else [])
    fixes_applied = 0
    validation_errors = []

    for item in items:
        classification = item.get('classification', {})
        code = classification.get('code', '')

        if not code or code == 'UNKNOWN':
            validation_errors.append({
                'index': item.get('index', -1),
                'description': item.get('description', ''),
                'issue': 'No classification code',
            })
            continue

        # Check format: must be 8 digits
        if not code.isdigit() or len(code) != 8:
            validation_errors.append({
                'index': item.get('index', -1),
                'code': code,
                'issue': f'Invalid code format: {code} (expected 8 digits)',
            })

        # Check against invalid codes mapping
        if code in invalid_codes:
            fix = invalid_codes[code]
            if isinstance(fix, str):
                classification['code'] = fix
                classification['auto_fixed'] = True
                classification['original_code'] = code
                fixes_applied += 1
            elif isinstance(fix, dict):
                classification['code'] = fix.get('correct_code', code)
                classification['auto_fixed'] = True
                classification['original_code'] = code
                classification['fix_reason'] = fix.get('reason', '')
                fixes_applied += 1

    result = {
        'status': 'success',
        'total_items': len(items),
        'fixes_applied': fixes_applied,
        'validation_errors': len(validation_errors),
        'errors': validation_errors[:20],  # Limit detail
    }

    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        output_data = data if isinstance(data, dict) else {'items': data}
        if isinstance(data, dict):
            output_data['items'] = items
        with open(output_path, 'w') as f:
            json.dump(output_data, f, indent=2)

    return result
