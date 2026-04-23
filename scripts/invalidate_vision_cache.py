#!/usr/bin/env python3
"""Invalidate entries in ``data/vision_cache/``.

Use cases
---------
1. Remove v1 false-negative caches (has_handwriting=false, blank customs values)
   so they re-extract under the v2 retry logic. This is the default action.
2. Remove caches for a specific waybill when the user knows the pencil value
   is wrong: ``--waybill HAWB9603312``.
3. Remove ALL caches (full re-extraction of the whole corpus):
   ``--all``.

The script prints each removed file and a summary at the end. Use ``--dry-run``
to preview without deleting.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CACHE_DIR = REPO_ROOT / "data" / "vision_cache"


def _iter_cache_files():
    if not CACHE_DIR.is_dir():
        return
    for p in CACHE_DIR.rglob("*.json"):
        yield p


def _is_v1_false_negative(cached: dict) -> bool:
    """Match the same predicate as pdf_splitter._cache_is_trustworthy's inverse."""
    if cached.get("_cache_version", 1) >= 2:
        return False
    hw = cached.get("handwritten") or {}
    ec = str(hw.get("customs_value_ec") or "").strip()
    usd = str(hw.get("customs_value_usd") or "").strip()
    return cached.get("has_handwriting") is False and not ec and not usd


def main() -> int:
    ap = argparse.ArgumentParser(description="Invalidate vision-cache entries")
    ap.add_argument(
        "--all",
        action="store_true",
        help="Delete every file in data/vision_cache/ (full re-extract)",
    )
    ap.add_argument(
        "--waybill",
        action="append",
        default=[],
        help="Delete caches whose printed.waybill matches (repeatable)",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be deleted, don't actually delete",
    )
    args = ap.parse_args()

    if not CACHE_DIR.is_dir():
        print(f"No cache dir at {CACHE_DIR} — nothing to do.")
        return 0

    targets = []
    for p in _iter_cache_files():
        try:
            with open(p, "r") as f:
                data = json.load(f)
        except Exception as e:
            print(f"[skip] {p.relative_to(REPO_ROOT)}: {e}")
            continue

        waybill = (data.get("printed") or {}).get("waybill") or ""

        if args.all:
            targets.append((p, "ALL", waybill))
        elif args.waybill and waybill in args.waybill:
            targets.append((p, "waybill-match", waybill))
        elif not args.all and not args.waybill and _is_v1_false_negative(data):
            targets.append((p, "v1-false-negative", waybill))

    if not targets:
        print("No matching cache entries found.")
        return 0

    print(f"{'Would delete' if args.dry_run else 'Deleting'} {len(targets)} cache entr(ies):")
    for p, reason, waybill in targets:
        rel = p.relative_to(REPO_ROOT)
        print(f"  - {rel}  [{reason}]  waybill={waybill or '?'}")
        if not args.dry_run:
            try:
                p.unlink()
            except Exception as e:
                print(f"    ERROR removing: {e}")

    print(f"\n{'DRY RUN — no files removed.' if args.dry_run else 'Done.'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
