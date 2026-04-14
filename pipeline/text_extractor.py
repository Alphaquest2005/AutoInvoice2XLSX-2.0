#!/usr/bin/env python3
"""Legacy CLI shim — forwards to ``multi_ocr._cli_main``.

The original ``text_extractor.py`` maintained its own pdfplumber →
PaddleOCR → Vision API fallback chain. That chain has been superseded by
the unified hybrid OCR pipeline in ``multi_ocr.py`` (Phase A). This file
exists only so that any stray subprocess invocation of
``python pipeline/text_extractor.py --input … --output …`` keeps working
and routes through the single source of truth.

The ``--api-key``, ``--base-url``, and ``--model`` flags are still
accepted for compatibility but are now ignored — vision-API usage is
controlled via the ``--quality deep`` matrix inside ``multi_ocr``.
"""

from __future__ import annotations

import argparse
import os
import sys

# Ensure sibling imports work when invoked as a script
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if _THIS_DIR not in sys.path:
    sys.path.insert(0, _THIS_DIR)

import multi_ocr  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Legacy text extractor — forwards to multi_ocr"
    )
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--quality", default="standard",
                        choices=("fast", "standard", "deep"))
    parser.add_argument("--no-cache", action="store_true")
    # Accepted but ignored (back-compat)
    parser.add_argument("--api-key", default="")
    parser.add_argument("--base-url", default="")
    parser.add_argument("--model", default="")
    args = parser.parse_args()

    forwarded = ["--input", args.input, "--output", args.output,
                 "--quality", args.quality]
    if args.no_cache:
        forwarded.append("--no-cache")
    return multi_ocr._cli_main(forwarded)


if __name__ == "__main__":
    sys.exit(main())
