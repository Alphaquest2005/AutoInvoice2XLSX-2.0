#!/usr/bin/env python3
"""
Extract assessed tariff classifications from ASYCUDA SQL Server databases.

Connects to BudgetMarine-AutoBot and IWW-DiscoveryDB, pulls all assessed customs
entries, deduplicates by normalized commercial description, and writes a local
JSON lookup file used as the first classification layer in the pipeline.

Usage:
    python pipeline/extract_assessed_codes.py                    # full extraction
    python pipeline/extract_assessed_codes.py --dry-run          # show stats only
    python pipeline/extract_assessed_codes.py --min-count 2      # require ≥2 occurrences
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)

# ── Database configs ──
SQL_SERVER = r'MINIJOE\SQLDEVELOPER2022'
DATABASES = [
    {'name': 'BudgetMarine-AutoBot', 'database': 'BudgetMarine-AutoBot'},
    {'name': 'IWW-DiscoveryDB', 'database': 'IWW-DiscoveryDB'},
]

# SQL query to extract assessed items with HS codes and descriptions.
# Uses ISNULL to guarantee 4 columns per row; output is space-delimited
# by sqlcmd -W (trimmed) with -h-1 (no headers).
EXTRACT_SQL = """\
SET NOCOUNT ON
SELECT
    hs.Commodity_code,
    gd.Commercial_Description,
    gd.Description_of_goods,
    ISNULL(inv.ItemNumber, '') AS inventory_item_number
FROM xcuda_Item i
JOIN xcuda_HScode hs ON hs.Item_Id = i.Item_Id
JOIN xcuda_Goods_description gd ON gd.Item_Id = i.Item_Id
LEFT JOIN xcuda_Inventory_Item xi ON xi.Item_Id = i.Item_Id
LEFT JOIN InventoryItems inv ON inv.Id = xi.InventoryItemId
WHERE i.IsAssessed = 1
  AND gd.Commercial_Description IS NOT NULL
  AND LEN(LTRIM(RTRIM(gd.Commercial_Description))) > 3
  AND hs.Commodity_code IS NOT NULL
  AND LEN(hs.Commodity_code) >= 6
"""

# ── Normalization constants ──
NOISE_WORDS = {
    'the', 'a', 'an', 'and', 'or', 'for', 'with', 'in', 'on', 'to', 'of',
    'no', 'nr', 'num', 'pcs', 'pc', 'ea', 'each', 'qty', 'quantity',
    'size', 'color', 'colour', 'pack', 'set', 'pieces', 'item', 'items',
    'new', 'used', 'unit', 'units', 'assorted', 'various', 'other',
}

GENERIC_DESCRIPTIONS = {
    'parts', 'accessories', 'other', 'parts accessories',
    'miscellaneous', 'misc', 'sundry', 'sundries', 'goods',
    'general cargo', 'articles', 'samples', 'sample',
    'spare parts', 'spares', 'components', 'supplies',
    'hardware', 'materials', 'items', 'products',
}


def normalize_description(desc: str) -> str:
    """Normalize a commercial description for matching."""
    if not desc:
        return ''
    text = unicodedata.normalize('NFKD', desc)
    text = ''.join(c for c in text if not unicodedata.combining(c))
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    words = [w for w in text.split() if w not in NOISE_WORDS and len(w) > 1]
    return ' '.join(words)


def is_specific_enough(normalized_desc: str) -> bool:
    """Check if a description has enough specificity to be a useful lookup key."""
    if len(normalized_desc) < 4:
        return False
    if normalized_desc.replace(' ', '').isdigit():
        return False
    if normalized_desc in GENERIC_DESCRIPTIONS:
        return False
    words = normalized_desc.split()
    if len(words) < 2:
        return len(words[0]) >= 6 if words else False
    return True


def load_cet_valid_codes() -> set:
    """Load valid 8-digit CET codes from data/cet_valid_codes.txt."""
    codes = set()
    cet_path = os.path.join(BASE_DIR, 'data', 'cet_valid_codes.txt')
    try:
        with open(cet_path, 'r', encoding='utf-8') as f:
            for line in f:
                code = line.strip().split('\t')[0]
                if len(code) == 8 and code.isdigit():
                    codes.add(code)
    except FileNotFoundError:
        print(f"  WARNING: CET codes file not found: {cet_path}")
    print(f"  Loaded {len(codes)} valid CET codes")
    return codes


def _run_sqlcmd(database: str, sql: str, output_path: str) -> bool:
    """Run a SQL query via Windows sqlcmd and save output to a file."""
    # Write query to a temp file on the Windows filesystem
    sql_path = os.path.join('/mnt/c/Temp', f'_extract_{database}.sql')
    os.makedirs('/mnt/c/Temp', exist_ok=True)
    with open(sql_path, 'w', encoding='utf-8') as f:
        f.write(sql)

    win_sql = sql_path.replace('/mnt/c/', 'C:\\\\').replace('/', '\\\\')
    win_out = output_path.replace('/mnt/c/', 'C:\\\\').replace('/', '\\\\')

    cmd = (
        f'sqlcmd -S {SQL_SERVER} -d {database} '
        f'-i {win_sql} -E -W -h-1 -o {win_out}'
    )
    result = subprocess.run(
        ['cmd.exe', '/c', cmd],
        capture_output=True, text=True, timeout=600,
    )
    if result.returncode != 0:
        print(f"  sqlcmd error: {result.stderr.strip()}")
        return False
    return True


def _parse_sqlcmd_output(output_path: str, db_name: str) -> list:
    """
    Parse sqlcmd space-delimited output.

    Each line has: <8-digit-code> <commercial_desc> <desc_of_goods> <inv_item>
    The first token is always an 8-digit code. The last token is the inventory
    item number (or empty). The two middle fields can contain spaces, so we
    use the code (first 8 chars) and inventory item (last non-space token with
    a slash or known pattern) to split.

    Because sqlcmd uses fixed-width columns, the format is actually:
      col1 (code, ~14 chars) + space + col2 (commercial) + space + col3 (desc_of_goods) + space + col4 (inv_item)
    But columns are variable-width with -W trim.  The reliable approach:
      - First token: always the 6-8 digit code
      - Last token: inventory item (often PREFIX/CODE or empty)
      - Everything between first token and description_of_goods boundary: commercial desc

    We use a regex-based approach: the description_of_goods field uses ALL CAPS
    and is typically a standard HS heading description.  We'll split on the
    known column structure from the query output.
    """
    rows = []
    with open(output_path, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            line = line.rstrip('\n\r')
            if not line or line.startswith('(') or line.startswith('-'):
                continue

            # The first token is always the commodity code (6-8 digits)
            parts = line.split()
            if not parts:
                continue

            commodity_code = parts[0].strip().replace('.', '')
            if not commodity_code or not commodity_code.isdigit():
                continue

            # Pad to 8 digits
            if len(commodity_code) == 6:
                commodity_code += '00'
            elif len(commodity_code) == 7:
                commodity_code += '0'
            if len(commodity_code) != 8:
                continue

            # Rest of line after the code
            rest = line[len(parts[0]):].strip()
            if not rest:
                continue

            # The inventory item is the last token (often has a / like GLF/1815-4)
            # The description_of_goods is typically ALL CAPS HS heading text
            # Strategy: find the last token, treat it as inv_item if it contains
            # / or is a known pattern; otherwise treat the whole rest as descriptions
            rest_parts = rest.rsplit(None, 1)
            inv_item = ''
            desc_block = rest

            if len(rest_parts) == 2:
                candidate_inv = rest_parts[1]
                # Inventory items typically contain / or are short alphanumeric codes
                if ('/' in candidate_inv or
                    (len(candidate_inv) <= 20 and re.match(r'^[A-Z0-9][A-Z0-9\-\.]+$', candidate_inv))):
                    inv_item = candidate_inv
                    desc_block = rest_parts[0]

            # Split desc_block into commercial_desc and description_of_goods
            # description_of_goods is the HS heading (ALL CAPS, typically 15+ chars)
            # We look for a run of ALL-CAPS words that is a plausible HS description
            # The commercial description comes first, HS description comes after
            commercial_desc = desc_block
            desc_of_goods = ''

            # Try to find the HS heading boundary: a sequence of ALL-CAPS words
            # at the end of the desc_block (before inv_item)
            # Common HS headings: "LUBRICATING OILS", "CHECK (NONRETURN) VALVES", etc.
            hs_match = re.search(
                r'\s([A-Z][A-Z\s,\(\)\-/]+(?:THEREOF|OTHER|ETC\.?)?)\s*$',
                desc_block
            )
            if hs_match and len(hs_match.group(1)) >= 10:
                desc_of_goods = hs_match.group(1).strip()
                commercial_desc = desc_block[:hs_match.start(1)].strip()

            if not commercial_desc or len(commercial_desc) < 3:
                continue

            rows.append({
                'commodity_code': commodity_code,
                'commercial_description': commercial_desc,
                'description_of_goods': desc_of_goods,
                'inventory_item_number': inv_item,
                'source_db': db_name,
            })

    return rows


def extract_from_database(db_config: dict) -> list:
    """Extract assessed classifications from a SQL Server database via sqlcmd."""
    name = db_config['name']
    database = db_config['database']
    print(f"\n  Extracting from {name}...")

    output_path = os.path.join('/mnt/c/Temp', f'{name}_assessed.txt')

    ok = _run_sqlcmd(database, EXTRACT_SQL, output_path)
    if not ok:
        print(f"  ERROR: sqlcmd failed for {name}")
        return []

    if not os.path.exists(output_path):
        print(f"  ERROR: Output file not created for {name}")
        return []

    file_size = os.path.getsize(output_path) / (1024 * 1024)
    print(f"  Output file: {file_size:.1f} MB")

    rows = _parse_sqlcmd_output(output_path, name)
    print(f"  Parsed {len(rows):,} valid rows from {name}")
    return rows


def deduplicate_entries(raw_entries: list, cet_valid: set, min_count: int = 1) -> dict:
    """Group by normalized description, majority-vote on tariff code."""
    groups = defaultdict(list)
    filtered_generic = 0

    for entry in raw_entries:
        norm = normalize_description(entry['commercial_description'])
        if not is_specific_enough(norm):
            filtered_generic += 1
            continue
        groups[norm].append(entry)

    result = {}
    for norm_desc, entries in groups.items():
        code_counter = Counter()
        sources = set()
        sample_desc = ''
        inventory_refs = set()
        hs_descriptions = Counter()

        for e in entries:
            code = e['commodity_code']
            code_counter[code] += 1
            sources.add(e['source_db'])
            if not sample_desc or len(e['commercial_description']) > len(sample_desc):
                sample_desc = e['commercial_description']
            if e.get('inventory_item_number'):
                inventory_refs.add(e['inventory_item_number'])
            if e.get('description_of_goods'):
                hs_descriptions[e['description_of_goods']] += 1

        if not code_counter:
            continue

        # Majority vote with CET validity tie-breaking
        top_codes = code_counter.most_common()
        best_code = top_codes[0][0]
        best_count = top_codes[0][1]

        if best_count < min_count:
            continue

        # Tie-break: prefer CET-valid code
        if len(top_codes) > 1 and top_codes[0][1] == top_codes[1][1]:
            for code, cnt in top_codes:
                if cnt == best_count and code in cet_valid:
                    best_code = code
                    break

        total = sum(code_counter.values())

        result[norm_desc] = {
            'code': best_code,
            'category': hs_descriptions.most_common(1)[0][0] if hs_descriptions else '',
            'count': best_count,
            'total': total,
            'confidence': round(best_count / total, 3),
            'sources': sorted(sources),
            'sample_desc': sample_desc,
            'inventory_refs': sorted(inventory_refs)[:10],
        }

    return result, filtered_generic


def main():
    parser = argparse.ArgumentParser(
        description='Extract assessed tariff classifications from ASYCUDA SQL Server databases')
    parser.add_argument('--output',
                        default=os.path.join(BASE_DIR, 'data', 'assessed_classifications.json'),
                        help='Output JSON file path')
    parser.add_argument('--dry-run', action='store_true',
                        help='Query databases but do not write output file')
    parser.add_argument('--min-count', type=int, default=1,
                        help='Minimum occurrence count to include (default: 1)')
    args = parser.parse_args()

    print("=" * 70)
    print("  ASSESSED TARIFF CLASSIFICATION EXTRACTION")
    print("=" * 70)

    # Load CET valid codes for validation
    cet_valid = load_cet_valid_codes()

    # Extract from all databases
    all_rows = []
    db_stats = {}
    for db_config in DATABASES:
        rows = extract_from_database(db_config)
        all_rows.extend(rows)
        db_stats[db_config['name']] = len(rows)

    if not all_rows:
        print("\nERROR: No rows extracted from any database")
        sys.exit(1)

    print(f"\n  Total raw rows across all databases: {len(all_rows):,}")

    # Deduplicate and aggregate
    print(f"\n  Deduplicating by normalized commercial description...")
    entries, filtered_generic = deduplicate_entries(all_rows, cet_valid, args.min_count)
    print(f"  Unique descriptions: {len(entries):,}")
    print(f"  Filtered (too generic): {filtered_generic:,}")

    # Validate codes against CET
    valid_count = 0
    invalid_codes = []
    for norm_desc, entry in entries.items():
        if entry['code'] in cet_valid:
            valid_count += 1
        else:
            invalid_codes.append((entry['code'], entry['sample_desc'][:60]))

    print(f"\n  CET validation:")
    print(f"    Valid:   {valid_count:,}")
    print(f"    Invalid: {len(invalid_codes):,}")
    if invalid_codes[:5]:
        print(f"    Sample invalid codes:")
        for code, desc in invalid_codes[:5]:
            print(f"      {code} — {desc}")

    # Stats by source
    source_counts = Counter()
    for entry in entries.values():
        for src in entry['sources']:
            source_counts[src] += 1
    print(f"\n  Entries by source:")
    for src, cnt in source_counts.most_common():
        print(f"    {src}: {cnt:,}")

    # Confidence distribution
    conf_buckets = Counter()
    for entry in entries.values():
        c = entry['confidence']
        if c >= 0.95:
            conf_buckets['≥95%'] += 1
        elif c >= 0.80:
            conf_buckets['80-95%'] += 1
        elif c >= 0.50:
            conf_buckets['50-80%'] += 1
        else:
            conf_buckets['<50%'] += 1
    print(f"\n  Confidence distribution:")
    for bucket in ['≥95%', '80-95%', '50-80%', '<50%']:
        print(f"    {bucket}: {conf_buckets.get(bucket, 0):,}")

    if args.dry_run:
        print(f"\n  DRY RUN — not writing output file")
        # Show some sample entries
        print(f"\n  Sample entries:")
        for i, (norm_desc, entry) in enumerate(list(entries.items())[:10]):
            print(f"    [{entry['code']}] {entry['sample_desc'][:70]} "
                  f"(count={entry['count']}/{entry['total']}, "
                  f"conf={entry['confidence']:.0%})")
        return

    # Write output
    output = {
        '_metadata': {
            'version': '1.0',
            'extracted_at': datetime.now().isoformat(),
            'databases': [db['name'] for db in DATABASES],
            'database_rows': db_stats,
            'total_raw_rows': len(all_rows),
            'total_unique_descriptions': len(entries),
            'filtered_generic': filtered_generic,
            'min_count': args.min_count,
            'cet_valid_count': valid_count,
            'cet_invalid_count': len(invalid_codes),
        },
        'entries': entries,
    }

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    file_size = os.path.getsize(args.output) / (1024 * 1024)
    print(f"\n  Written: {args.output}")
    print(f"  File size: {file_size:.1f} MB")
    print(f"  Entries: {len(entries):,}")
    print(f"\n{'=' * 70}")


if __name__ == '__main__':
    main()
