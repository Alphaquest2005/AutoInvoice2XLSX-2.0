#!/usr/bin/env python3
"""
Backward-compatibility shim.

All parsing logic has been consolidated into format_parser.py.
This module re-exports the legacy functions so existing imports continue to work.
"""

from format_parser import (
    parse_text_file,
    parse_with_legacy_format as parse_with_format,
    parse_generic_invoice,
    parse_tsv_format,
    parse_columnar_format,
)

__all__ = [
    'parse_text_file',
    'parse_with_format',
    'parse_generic_invoice',
    'parse_tsv_format',
    'parse_columnar_format',
]
