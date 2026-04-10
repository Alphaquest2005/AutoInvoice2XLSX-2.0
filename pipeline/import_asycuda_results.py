#!/usr/bin/env python3
"""
Import ASYCUDA export XMLs and compare against generated test XMLs.

After round-tripping through ASYCUDA:
  - Identifies which tariff codes were accepted vs rejected
  - Extracts tax rates, official descriptions, and precision codes
  - Updates the CET valid codes database and HS lookup cache

Usage:
    python import_asycuda_results.py --generated-dir DIR --exported-dir DIR [--update]

    --generated-dir   Directory with original CET-TEST-*.xml files + manifest.json
    --exported-dir    Directory with ASYCUDA-exported XML files
    --update          Actually update data files (without this flag, dry-run only)
"""

import argparse
import json
import os
import sys
from pathlib import Path

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(__file__))
from asycuda_xml_parser import parse_asycuda_xml, extract_classifications


def load_manifest(generated_dir: str) -> dict:
    """Load the manifest.json from the generated directory."""
    manifest_path = os.path.join(generated_dir, 'manifest.json')
    if not os.path.exists(manifest_path):
        print(f"ERROR: manifest.json not found in {generated_dir}")
        sys.exit(1)
    with open(manifest_path, 'r') as f:
        return json.load(f)


def load_generated_codes(generated_dir: str) -> dict:
    """
    Parse all generated XMLs to build a map of product_id -> tariff_code.

    Returns: {product_id: {'code': tariff_code, 'description': desc}}
    """
    import xml.etree.ElementTree as ET

    code_map = {}
    for xml_file in sorted(Path(generated_dir).glob('CET-TEST-*.xml')):
        tree = ET.parse(str(xml_file))
        root = tree.getroot()

        for item_elem in root.findall('Item'):
            tarification = item_elem.find('Tarification')
            if tarification is None:
                continue
            hs_code = tarification.find('HScode')
            if hs_code is None:
                continue

            commodity = hs_code.find('Commodity_code')
            precision4 = hs_code.find('Precision_4')

            if commodity is not None and commodity.text:
                code = commodity.text.strip()
                product_id = precision4.text.strip() if precision4 is not None and precision4.text else None

                goods_desc = item_elem.find('Goods_description')
                desc = None
                if goods_desc is not None:
                    desc_elem = goods_desc.find('Description_of_goods')
                    if desc_elem is not None and desc_elem.text:
                        desc = desc_elem.text.strip()

                if product_id:
                    code_map[product_id] = {'code': code, 'description': desc}

    print(f"Loaded {len(code_map)} generated codes from {generated_dir}")
    return code_map


def parse_exported_xmls(exported_dir: str) -> dict:
    """
    Parse all exported ASYCUDA XMLs and extract item data.

    Returns: {product_id: AsycudaItem} for all items found in exports.
    """
    exported_items = {}

    xml_files = list(Path(exported_dir).glob('*.xml'))
    if not xml_files:
        print(f"ERROR: No XML files found in {exported_dir}")
        sys.exit(1)

    print(f"Parsing {len(xml_files)} exported XML files...")

    for xml_file in sorted(xml_files):
        try:
            decl = parse_asycuda_xml(str(xml_file))
            for item in decl.items:
                # Match by Precision_4 (product ID reference)
                if item.precision_4:
                    exported_items[item.precision_4] = item
        except Exception as e:
            print(f"  WARNING: Failed to parse {xml_file.name}: {e}")

    print(f"Found {len(exported_items)} items in exported XMLs")
    return exported_items


def compare_results(generated_codes: dict, exported_items: dict) -> dict:
    """
    Compare generated codes against exported results.

    Returns summary dict with accepted/rejected codes and extracted data.
    """
    accepted = []
    rejected = []
    modified = []  # Codes that ASYCUDA changed

    for product_id, gen_info in sorted(generated_codes.items()):
        orig_code = gen_info['code']

        if product_id in exported_items:
            item = exported_items[product_id]
            exp_code = item.commodity_code

            entry = {
                'product_id': product_id,
                'original_code': orig_code,
                'exported_code': exp_code,
                'description': item.description_of_goods,
                'commercial_desc': item.commercial_description,
                'taxes': [],
            }

            # Extract tax info
            for tax in item.taxes:
                entry['taxes'].append({
                    'code': tax.tax_code,
                    'rate': tax.tax_rate,
                    'amount': tax.tax_amount,
                    'base': tax.tax_base,
                    'payment_mode': tax.mode_of_payment,
                })

            if orig_code == exp_code:
                accepted.append(entry)
            else:
                entry['status'] = 'modified'
                modified.append(entry)
        else:
            rejected.append({
                'product_id': product_id,
                'original_code': orig_code,
                'description': gen_info.get('description'),
            })

    return {
        'total_generated': len(generated_codes),
        'total_exported': len(exported_items),
        'accepted': accepted,
        'rejected': rejected,
        'modified': modified,
        'accepted_count': len(accepted),
        'rejected_count': len(rejected),
        'modified_count': len(modified),
    }


def print_report(results: dict):
    """Print a comprehensive comparison report."""
    print()
    print('=' * 72)
    print('ASYCUDA CET TARIFF CODE VALIDATION REPORT')
    print('=' * 72)
    print()
    print(f"  Generated codes:  {results['total_generated']}")
    print(f"  Exported items:   {results['total_exported']}")
    print()
    print(f"  Accepted (exact): {results['accepted_count']}")
    print(f"  Modified by ASYCUDA: {results['modified_count']}")
    print(f"  Rejected/missing: {results['rejected_count']}")
    print()

    # Show modified codes
    if results['modified']:
        print('─' * 72)
        print('MODIFIED CODES (ASYCUDA changed the tariff code)')
        print('─' * 72)
        for entry in results['modified'][:50]:  # Show first 50
            print(f"  {entry['product_id']}: {entry['original_code']} → {entry['exported_code']}"
                  f"  {entry.get('description', '')}")
        if len(results['modified']) > 50:
            print(f"  ... and {len(results['modified']) - 50} more")
        print()

    # Show rejected codes
    if results['rejected']:
        print('─' * 72)
        print('REJECTED CODES (not in ASYCUDA export)')
        print('─' * 72)
        for entry in results['rejected'][:50]:  # Show first 50
            print(f"  {entry['product_id']}: {entry['original_code']}"
                  f"  {entry.get('description', '')}")
        if len(results['rejected']) > 50:
            print(f"  ... and {len(results['rejected']) - 50} more")
        print()

    # Show tax rate summary for accepted codes
    if results['accepted']:
        tax_summary = {}
        for entry in results['accepted']:
            for tax in entry.get('taxes', []):
                tc = tax['code']
                if tc not in tax_summary:
                    tax_summary[tc] = {'rates': set(), 'count': 0}
                tax_summary[tc]['count'] += 1
                if tax['rate'] is not None:
                    tax_summary[tc]['rates'].add(tax['rate'])

        if tax_summary:
            print('─' * 72)
            print('TAX RATE SUMMARY (from accepted codes)')
            print('─' * 72)
            for tc in sorted(tax_summary.keys()):
                info = tax_summary[tc]
                rates_str = ', '.join(f'{r}%' for r in sorted(info['rates']))
                print(f"  {tc}: {info['count']} items, rates: {rates_str}")
            print()

    print('=' * 72)


def update_cet_valid_codes(results: dict, base_dir: str, dry_run: bool = True):
    """
    Update data/cet_valid_codes.txt with results from ASYCUDA.

    - Remove rejected codes
    - Update descriptions from ASYCUDA's official descriptions
    - Add any new codes that ASYCUDA provided (from modified entries)
    """
    cet_path = os.path.join(base_dir, 'data', 'cet_valid_codes.txt')

    # Load current codes
    current = {}
    with open(cet_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.rstrip('\n\r')
            if not line.strip():
                continue
            parts = line.split('\t', 1)
            code = parts[0].strip()
            desc = parts[1].strip() if len(parts) > 1 else ''
            current[code] = desc

    original_count = len(current)

    # Collect rejected codes
    rejected_codes = set(e['original_code'] for e in results['rejected'])

    # Collect accepted descriptions (update existing)
    updated_descs = 0
    for entry in results['accepted']:
        code = entry['exported_code']
        asycuda_desc = entry.get('description', '')
        if code in current and asycuda_desc and asycuda_desc != current[code]:
            if not asycuda_desc.startswith('CET TEST ITEM'):
                current[code] = asycuda_desc
                updated_descs += 1

    # Handle modified codes — add the ASYCUDA version if not present
    added_codes = 0
    for entry in results['modified']:
        new_code = entry['exported_code']
        if new_code not in current:
            desc = entry.get('description', '')
            if not desc or desc.startswith('CET TEST ITEM'):
                desc = ''
            current[new_code] = desc
            added_codes += 1

    # Remove rejected codes
    removed = 0
    for code in rejected_codes:
        if code in current:
            del current[code]
            removed += 1

    print()
    print('CET VALID CODES UPDATE')
    print(f"  Original codes: {original_count}")
    print(f"  Rejected (removed): {removed}")
    print(f"  Modified (added new): {added_codes}")
    print(f"  Descriptions updated: {updated_descs}")
    print(f"  Final count: {len(current)}")

    if dry_run:
        print("  [DRY RUN] No files modified. Use --update to apply changes.")
    else:
        # Write updated file
        with open(cet_path, 'w', encoding='utf-8') as f:
            for code in sorted(current.keys()):
                desc = current[code]
                if desc:
                    f.write(f'{code}\t{desc}\n')
                else:
                    f.write(f'{code}\t\n')
        print(f"  Written to {cet_path}")

    return current


def update_hs_cache(results: dict, base_dir: str, dry_run: bool = True):
    """
    Update data/hs_lookup_cache.json with tax rates and descriptions from ASYCUDA.
    """
    cache_path = os.path.join(base_dir, 'data', 'hs_lookup_cache.json')

    cache = {}
    if os.path.exists(cache_path):
        with open(cache_path, 'r', encoding='utf-8') as f:
            cache = json.load(f)

    updates = 0

    # Update from accepted codes — add tax rate info
    for entry in results['accepted'] + results['modified']:
        code = entry.get('exported_code', entry.get('original_code'))
        if not code:
            continue

        # Find or create cache entry
        # Cache is keyed by description, so we need a reverse lookup
        # For now, store in a special section
        tax_key = f'__asycuda_tax_{code}'
        if entry.get('taxes'):
            tax_data = {
                'code': code,
                'description': entry.get('description', ''),
                'taxes': entry['taxes'],
                'source': 'asycuda_import',
            }
            if tax_key not in cache or cache[tax_key] != tax_data:
                cache[tax_key] = tax_data
                updates += 1

    print()
    print('HS CACHE UPDATE')
    print(f"  Tax rate entries added/updated: {updates}")

    if dry_run:
        print("  [DRY RUN] No files modified. Use --update to apply changes.")
    else:
        with open(cache_path, 'w', encoding='utf-8') as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
        print(f"  Written to {cache_path}")


def update_invalid_codes(results: dict, base_dir: str, dry_run: bool = True):
    """
    Update rules/invalid_codes.json with rejected codes and correction mappings.

    For modified codes, add mapping: original_code -> corrected_code (ASYCUDA's version).
    For rejected codes with no correction, add mapping: code -> null.
    """
    invalid_path = os.path.join(base_dir, 'rules', 'invalid_codes.json')

    invalid_map = {}
    if os.path.exists(invalid_path):
        with open(invalid_path, 'r', encoding='utf-8') as f:
            invalid_map = json.load(f)

    original_count = len(invalid_map)
    added = 0

    # Add modified codes: original -> ASYCUDA's correction
    for entry in results['modified']:
        orig = entry['original_code']
        corrected = entry['exported_code']
        if orig not in invalid_map:
            invalid_map[orig] = corrected
            added += 1

    # Add rejected codes (no correction available)
    for entry in results['rejected']:
        code = entry['original_code']
        if code not in invalid_map:
            invalid_map[code] = None
            added += 1

    print()
    print('INVALID CODES UPDATE')
    print(f"  Original entries: {original_count}")
    print(f"  New entries added: {added}")
    print(f"  Final count: {len(invalid_map)}")

    if dry_run:
        print("  [DRY RUN] No files modified. Use --update to apply changes.")
    else:
        with open(invalid_path, 'w', encoding='utf-8') as f:
            json.dump(invalid_map, f, indent=2, ensure_ascii=False)
        print(f"  Written to {invalid_path}")


def save_full_report(results: dict, output_dir: str):
    """Save the full comparison results as JSON."""
    report_path = os.path.join(output_dir, 'asycuda_validation_report.json')
    with open(report_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"Full report saved to {report_path}")
    return report_path


def main():
    parser = argparse.ArgumentParser(
        description='Import ASYCUDA export XMLs and compare against generated test XMLs')
    parser.add_argument('--generated-dir', required=True,
                        help='Directory with generated CET-TEST-*.xml files')
    parser.add_argument('--exported-dir', required=True,
                        help='Directory with ASYCUDA-exported XML files')
    parser.add_argument('--base-dir', default=os.path.join(os.path.dirname(__file__), '..'),
                        help='Project base directory')
    parser.add_argument('--update', action='store_true',
                        help='Actually update data files (default: dry-run)')

    args = parser.parse_args()
    base_dir = os.path.abspath(args.base_dir)
    dry_run = not args.update

    if dry_run:
        print("*** DRY RUN MODE — no files will be modified. Use --update to apply. ***")
        print()

    # Load generated codes
    generated_codes = load_generated_codes(args.generated_dir)

    # Parse exported XMLs
    exported_items = parse_exported_xmls(args.exported_dir)

    # Compare
    results = compare_results(generated_codes, exported_items)

    # Print report
    print_report(results)

    # Save full report JSON
    save_full_report(results, args.generated_dir)

    # Update databases
    update_cet_valid_codes(results, base_dir, dry_run)
    update_hs_cache(results, base_dir, dry_run)
    update_invalid_codes(results, base_dir, dry_run)

    print()
    if dry_run:
        print("Re-run with --update to apply all changes to data files.")
    else:
        print("All data files have been updated.")


if __name__ == '__main__':
    main()
