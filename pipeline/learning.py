#!/usr/bin/env python3
"""
Stage 8: Learning
Records classifications and extracts new rules from corrections.
"""

import json
import os
from datetime import datetime
from typing import Any, Dict, List


def run(input_path: str, output_path: str = None, config: Dict = None, context: Dict = None) -> Dict:
    """Record classification results and check for learnable patterns."""
    base_dir = context.get('base_dir', '.') if context else '.'
    corrections_path = os.path.join(base_dir, 'data', 'corrections.json')
    rules_path = os.path.join(base_dir, 'rules', 'classification_rules.json')

    # Load existing corrections
    corrections = []
    if os.path.exists(corrections_path):
        try:
            with open(corrections_path) as f:
                corrections = json.load(f)
        except (json.JSONDecodeError, IOError):
            corrections = []

    # Check threshold for auto-rule extraction
    threshold = 3
    if config and config.get('options'):
        threshold = config['options'].get('extract_rules_threshold', 3)

    # Analyze corrections for repeating patterns
    new_rules = extract_rules(corrections, threshold)

    result = {
        'status': 'success',
        'total_corrections': len(corrections),
        'new_rules_extracted': len(new_rules),
    }

    # If new rules found, add to classification_rules.json
    if new_rules and os.path.exists(rules_path):
        try:
            with open(rules_path) as f:
                rules_data = json.load(f)

            existing_ids = {r.get('id') for r in rules_data.get('rules', [])}

            added = 0
            for rule in new_rules:
                if rule['id'] not in existing_ids:
                    rules_data['rules'].append(rule)
                    added += 1

            if added > 0:
                with open(rules_path, 'w') as f:
                    json.dump(rules_data, f, indent=2)
                result['rules_added'] = added

        except Exception as e:
            result['warnings'] = [f"Could not update rules: {e}"]

    return result


def extract_rules(corrections: List[Dict], threshold: int = 3) -> List[Dict]:
    """Extract classification rules from repeated corrections."""
    # Group corrections by target code
    code_groups = {}
    for corr in corrections:
        target_code = corr.get('corrected_code', corr.get('new_code'))
        if not target_code:
            continue

        if target_code not in code_groups:
            code_groups[target_code] = []
        code_groups[target_code].append(corr)

    new_rules = []
    for code, group in code_groups.items():
        if len(group) >= threshold:
            # Extract common patterns from descriptions
            descriptions = [c.get('description', c.get('item_pattern', '')).upper() for c in group]
            common_words = find_common_words(descriptions)

            if common_words:
                rule = {
                    'id': f'learned_{code}_{len(new_rules)}',
                    'code': code,
                    'category': group[0].get('category', 'PRODUCTS'),
                    'patterns': common_words,
                    'confidence': 0.85,
                    'priority': 5,
                    'source': 'auto_learned',
                    'learned_from': len(group),
                    'created_at': datetime.now().isoformat(),
                }
                new_rules.append(rule)

    return new_rules


def find_common_words(descriptions: List[str]) -> List[str]:
    """Find words common to most descriptions."""
    if not descriptions:
        return []

    # Split all descriptions into words
    word_counts = {}
    for desc in descriptions:
        words = set(desc.split())
        for word in words:
            # Skip short words and numbers
            if len(word) < 3 or word.isdigit():
                continue
            word_counts[word] = word_counts.get(word, 0) + 1

    # Find words appearing in majority of descriptions
    threshold = len(descriptions) * 0.6
    common = [w for w, c in word_counts.items() if c >= threshold]

    return common[:5]  # Limit to top 5 patterns
