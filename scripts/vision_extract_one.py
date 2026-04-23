#!/usr/bin/env python3
"""One-shot driver for pipeline.pdf_splitter.extract_declaration_handwriting.

Deletes any existing disk-cache entry for the given PDF, invokes the vision
extractor (Z.AI glm-4.6v), prints the result, and re-stores to cache.

Usage:
    python scripts/vision_extract_one.py <path-to-pdf> [--keep-cache]

Exit codes:
    0 — vision returned a result (with or without handwriting)
    1 — invocation failed / empty result / missing API key

Notes:
    - This bypasses run.py and pdf_splitter.run(); it calls the extractor
      directly so you can iterate on prompt / preprocessing tweaks without
      running the full pipeline.
    - ``base_dir`` is resolved to the repository root (parent of scripts/)
      so ``data/settings.json`` and ``data/vision_cache/`` are found.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys


def _repo_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))


def _bust_cache(pdf_path: str, base_dir: str) -> None:
    """Delete the disk-cache entry for this PDF, if any, so the next call
    goes to the vision API."""
    # Import lazily so that argparse errors surface cleanly.
    sys.path.insert(0, os.path.join(base_dir, 'pipeline'))
    from pdf_splitter import _vision_cache_path  # type: ignore

    cache_path = _vision_cache_path(pdf_path, base_dir)
    if cache_path and os.path.exists(cache_path):
        os.remove(cache_path)
        print(f"[cache] removed {cache_path}")
    else:
        print("[cache] no existing entry to remove")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('pdf_path', help='Path to a Grenada declaration PDF')
    parser.add_argument(
        '--keep-cache', action='store_true',
        help='Do NOT bust the cache before extraction (for trust-predicate tests)',
    )
    parser.add_argument(
        '-v', '--verbose', action='store_true',
        help='Enable DEBUG logging from pipeline.pdf_splitter',
    )
    args = parser.parse_args(argv)

    pdf_path = os.path.abspath(args.pdf_path)
    if not os.path.exists(pdf_path):
        print(f"ERROR: PDF not found: {pdf_path}", file=sys.stderr)
        return 1

    base_dir = _repo_root()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(asctime)s %(name)s %(levelname)s %(message)s',
    )

    sys.path.insert(0, os.path.join(base_dir, 'pipeline'))
    from pdf_splitter import extract_declaration_handwriting  # type: ignore

    if not args.keep_cache:
        _bust_cache(pdf_path, base_dir)

    print(f"[run] extract_declaration_handwriting({pdf_path!r}, base_dir={base_dir!r})")
    result = extract_declaration_handwriting(pdf_path, base_dir=base_dir)
    print('[result]')
    print(json.dumps(result, indent=2, default=str))

    if not result:
        print('[verdict] EMPTY — vision API unavailable or extraction failed', file=sys.stderr)
        return 1

    hw = (result.get('handwritten') or {}) if isinstance(result, dict) else {}
    ec = hw.get('customs_value_ec')
    usd = hw.get('customs_value_usd')
    has_hw = bool(result.get('has_handwriting')) if isinstance(result, dict) else False
    print(
        f"[verdict] has_handwriting={has_hw} customs_value_ec={ec!r} "
        f"customs_value_usd={usd!r}"
    )
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
