#!/usr/bin/env python3
"""
Quick PDF classifier for email routing.

Classifies all PDFs in a directory and extracts BL metadata if a Bill of Lading
is found. Used by the TypeScript email service to decide whether to link emails.

Usage:
    python classify_pdfs.py --dir /path/to/email/folder

Output (JSON to stdout):
    {
      "classification": { "bill_of_lading": [...], "invoice": [...], ... },
      "has_bl": true,
      "has_invoices": false,
      "bl_metadata": { "bl_number": "TSCW18496806", "consignee": "...", ... }
    }
"""

import argparse
import json
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from stages.supplier_resolver import classify_input_pdfs, extract_pdf_text


def extract_bl_metadata(bl_pdf_path: str) -> dict:
    """Extract metadata from a BL PDF for matching purposes."""
    metadata = {
        'bl_number': '',
        'consignee': '',
        'invoice_refs': [],
        'shipper_names': [],
    }

    try:
        from bl_parser import parse_bl_pdf
        bl_data = parse_bl_pdf(bl_pdf_path)

        metadata['bl_number'] = bl_data.get('bl_number', '')
        metadata['consignee'] = bl_data.get('consignee', '')

        # Collect all invoice refs and shipper names from shipments
        for shipment in bl_data.get('shipments', []):
            metadata['invoice_refs'].extend(shipment.get('invoice_refs', []))
            shipper = shipment.get('shipper', '').strip()
            if shipper and shipper not in metadata['shipper_names']:
                metadata['shipper_names'].append(shipper)

    except Exception as e:
        # Fallback: try basic text extraction for BL number
        try:
            import re
            text = extract_pdf_text(bl_pdf_path)
            if text:
                # BL number patterns
                for pattern in [
                    r'(TSCW\d+)', r'(MEDU\d+[A-Z]*\d*)', r'(HDMU[A-Z0-9]+)',
                    r'(MAEU\d+)', r'(COSU\d+)',
                    r'B/L\s*(?:NO\.?|NUMBER|#)[:\s]*([A-Z0-9]+)',
                    r'BL\s*(?:NO\.?|NUMBER|#)[:\s]*([A-Z0-9]+)',
                ]:
                    m = re.search(pattern, text.upper())
                    if m:
                        metadata['bl_number'] = m.group(1).strip()
                        break

                # Consignee
                m = re.search(r'CONSIGNEE.*?\n(.+?)(?:\n|PO BOX)', text, re.DOTALL)
                if m:
                    metadata['consignee'] = m.group(1).strip()
        except Exception:
            pass

    return metadata


def main():
    parser = argparse.ArgumentParser(description='Classify PDFs in a directory')
    parser.add_argument('--dir', required=True, help='Directory containing PDFs')
    args = parser.parse_args()

    if not os.path.isdir(args.dir):
        print(json.dumps({'error': f'Directory not found: {args.dir}'}))
        sys.exit(1)

    classification = classify_input_pdfs(args.dir)

    has_bl = len(classification.get('bill_of_lading', [])) > 0
    has_invoices = len(classification.get('invoice', [])) > 0

    bl_metadata = {}
    if has_bl:
        bl_file = classification['bill_of_lading'][0]
        bl_path = os.path.join(args.dir, bl_file)
        bl_metadata = extract_bl_metadata(bl_path)

    result = {
        'classification': classification,
        'has_bl': has_bl,
        'has_invoices': has_invoices,
        'bl_metadata': bl_metadata,
    }

    print(json.dumps(result))


if __name__ == '__main__':
    main()
