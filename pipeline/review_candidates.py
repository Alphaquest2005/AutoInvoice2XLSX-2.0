#!/usr/bin/env python3
"""
review_candidates — summarise the learned-fixes journal.

When ``apply_fixes.log_fix_candidates`` writes JSONL records to
``data/learned_fixes/candidates/YYYY-MM/YYYY-MM-DD.jsonl``, this CLI
reads them back and prints a human-friendly summary grouped by format,
supplier, and the kinds of changes that were applied.

This is intentionally a read-only tool — it does NOT mutate format
specs or the candidate files.  Its job is to surface patterns worth
promoting by hand:

    $ python pipeline/review_candidates.py
    $ python pipeline/review_candidates.py --month 2026-04
    $ python pipeline/review_candidates.py --format shein_us_invoice

Typical workflow: run it weekly, look for a format that shows up with
the same override pattern across multiple invoices, then go patch the
format spec (or add a product DB entry) so that pattern is handled
deterministically on the next run.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter, defaultdict
from typing import Dict, Iterable, List, Optional

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))


def _iter_journal_files(root: str, month: Optional[str] = None) -> Iterable[str]:
    """Yield every JSONL journal file under the candidates directory."""
    if not os.path.isdir(root):
        return
    months = sorted(os.listdir(root)) if month is None else [month]
    for m in months:
        mdir = os.path.join(root, m)
        if not os.path.isdir(mdir):
            continue
        for name in sorted(os.listdir(mdir)):
            if name.endswith('.jsonl'):
                yield os.path.join(mdir, name)


def load_candidates(
    base_dir: str = BASE_DIR,
    month: Optional[str] = None,
) -> List[Dict]:
    """Read every candidate record from the journal, newest last."""
    root = os.path.join(base_dir, 'data', 'learned_fixes', 'candidates')
    out: List[Dict] = []
    for path in _iter_journal_files(root, month=month):
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    out.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return out


def summarise(records: List[Dict]) -> Dict[str, object]:
    """Compute a summary dict suitable for printing or JSON output."""
    by_format: Counter = Counter()
    by_supplier: Counter = Counter()
    by_change_kind: Counter = Counter()
    totals = {'updated': 0, 'added': 0, 'deleted': 0}

    for rec in records:
        by_format[rec.get('format_name') or '(unknown)'] += 1
        by_supplier[rec.get('supplier') or '(unknown)'] += 1
        totals['updated'] += int(rec.get('items_updated') or 0)
        totals['added'] += int(rec.get('items_added') or 0)
        totals['deleted'] += int(rec.get('items_deleted') or 0)
        for ch in rec.get('changes') or []:
            # First token of each change string is the change kind / sku
            kind = ch.split(':', 1)[0].strip().split(' ', 1)[0]
            if kind:
                by_change_kind[kind] += 1

    return {
        'total_records': len(records),
        'totals': totals,
        'by_format': by_format.most_common(),
        'by_supplier': by_supplier.most_common(10),
        'top_change_skus': by_change_kind.most_common(10),
    }


def _print_summary(summary: Dict[str, object]) -> None:
    print(f"Total fix records: {summary['total_records']}")
    totals = summary['totals']
    print(
        f"  Items updated: {totals['updated']}  "
        f"added: {totals['added']}  "
        f"deleted: {totals['deleted']}"
    )
    print("\nBy format:")
    for name, count in summary['by_format']:
        print(f"  {count:>4}  {name}")
    print("\nTop suppliers:")
    for name, count in summary['by_supplier']:
        print(f"  {count:>4}  {name}")
    print("\nTop touched SKUs / change kinds:")
    for name, count in summary['top_change_skus']:
        print(f"  {count:>4}  {name}")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description='Review learned-fixes candidates')
    parser.add_argument('--base-dir', default=BASE_DIR,
                        help='Project base directory (default: repo root)')
    parser.add_argument('--month', help='Filter to a single month (e.g. 2026-04)')
    parser.add_argument('--format', dest='format_name',
                        help='Filter to a single format spec name')
    parser.add_argument('--json', action='store_true',
                        help='Emit JSON summary instead of plain text')
    args = parser.parse_args(argv)

    records = load_candidates(base_dir=args.base_dir, month=args.month)
    if args.format_name:
        records = [r for r in records if r.get('format_name') == args.format_name]

    summary = summarise(records)
    if args.json:
        print(json.dumps(summary, indent=2, default=str))
    else:
        _print_summary(summary)
    return 0


if __name__ == '__main__':
    sys.exit(main())
