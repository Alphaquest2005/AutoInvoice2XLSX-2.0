#!/usr/bin/env python3
"""
BL (Bill of Lading) Processing Script — Thin CLI Wrapper

Delegates to modular stage modules for DRY reuse:
  - stages/supplier_resolver.py  → supplier DB, classification, file discovery
  - stages/invoice_processor.py  → single-invoice pipeline (parse → match → classify → XLSX)
  - stages/bl_allocator.py       → BL parsing, invoice matching, package allocation

For unified entry point with auto-detection, use: python pipeline/run.py

Usage:
    python pipeline/process_bl.py --bl TSCW18489131
    python pipeline/process_bl.py --bl TSCW18489131 --input-dir workspace/documents --output-dir workspace/shipments
    python pipeline/process_bl.py --bl TSCW18489131 --send-email
"""

import argparse
import logging
import os
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add pipeline directory to path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

# Import stage modules
from stages.supplier_resolver import (
    init as init_resolver,
    load_supplier_db,
    save_supplier_db,
    load_classification_rules,
    find_po_file,
    find_bl_pdf,
    get_pdf_files,
)
from stages.invoice_processor import process_single_invoice
from stages.bl_allocator import allocate_bl_packages

# Import pipeline modules
try:
    from format_registry import FormatRegistry
except ImportError:
    print("ERROR: format_registry module not found")
    sys.exit(1)

from po_matcher import POReader, POMatcher
from workflow.email import compose_email, send_email as do_send_email


def main():
    parser = argparse.ArgumentParser(
        description='Process BL shipment - match invoices to PO and generate CARICOM XLSX'
    )
    parser.add_argument('--bl', required=True,
                        help='Bill of Lading number (e.g., TSCW18489131)')
    parser.add_argument('--input-dir',
                        default=os.path.join(BASE_DIR, 'workspace', 'documents'),
                        help='Directory containing PDF invoices and PO XLSX')
    parser.add_argument('--output-dir',
                        default=os.path.join(BASE_DIR, 'workspace', 'shipments'),
                        help='Directory for output XLSX files')
    parser.add_argument('--po-file',
                        help='Path to PO XLSX file (auto-detected if not specified)')
    parser.add_argument('--doc-type', default='7400-000',
                        help='CARICOM document type (e.g., 7400-000, 4000-000)')
    parser.add_argument('--send-email', action='store_true',
                        help='Send email for each invoice (same as simplified declaration)')
    parser.add_argument('--consignee', default='BUDGET MARINE (GRENADA)',
                        help='Consignee name for the shipment')
    parser.add_argument('--consignee-code', default='07290940003049',
                        help='Consignee code')
    parser.add_argument('--man-reg',
                        help='Manifest registration (e.g. "2026 148")')
    parser.add_argument('--location', default='STG01',
                        help='Location of goods code')
    parser.add_argument('--office', default='GDSGO',
                        help='Office code')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Enable verbose logging')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Initialize the resolver with project base directory
    init_resolver(BASE_DIR)

    print("=" * 80)
    print(f"Processing BL #{args.bl}  |  Document Type: {args.doc_type}")
    print("=" * 80)

    # ── Stage 1: Setup ──

    # Find and load PO XLSX
    po_file = args.po_file or find_po_file(args.input_dir, args.bl)
    if not po_file or not os.path.exists(po_file):
        print(f"ERROR: PO XLSX file not found in {args.input_dir}")
        sys.exit(1)

    print(f"\n[1] Loading PO data from {os.path.basename(po_file)}...")
    po_items = POReader.read_po_xlsx(po_file)
    print(f"    {len(po_items)} PO line items loaded")

    # Initialize format registry
    print("\n[2] Initializing format registry...")
    registry = FormatRegistry(BASE_DIR)
    print(f"    {len(registry.list_formats())} format specs loaded")

    # Load supplier database and classification rules
    supplier_db = load_supplier_db()
    rules, noise_words = load_classification_rules()
    print(f"    {len(rules)} classification rules loaded")

    # Initialize PO matcher
    matcher = POMatcher(po_items, base_dir=BASE_DIR)

    # Get PDF files
    pdf_files = get_pdf_files(args.input_dir)
    print(f"\n[3] Processing {len(pdf_files)} PDF invoices...\n")

    # Clean output directory
    if os.path.exists(args.output_dir):
        for f in os.listdir(args.output_dir):
            fp = os.path.join(args.output_dir, f)
            if os.path.isfile(fp):
                os.remove(fp)
    os.makedirs(args.output_dir, exist_ok=True)

    # ── Stage 2: Process each invoice ──

    results = []
    all_attachments = []
    for pdf_file in pdf_files:
        pdf_path = os.path.join(args.input_dir, pdf_file)
        result = process_single_invoice(
            pdf_path=pdf_path,
            registry=registry,
            matcher=matcher,
            rules=rules,
            noise_words=noise_words,
            supplier_db=supplier_db,
            output_dir=args.output_dir,
            document_type=args.doc_type,
            verbose=args.verbose,
        )
        if result:
            results.append(result)
            all_attachments.extend([result.pdf_output_path, result.xlsx_path])

    # Save updated supplier database
    save_supplier_db(supplier_db)

    # ── Stage 3: BL allocation ──

    bl_pdf_path = find_bl_pdf(args.input_dir)
    bl_alloc = allocate_bl_packages(
        bl_pdf_path=bl_pdf_path,
        invoice_results=results,
        output_dir=args.output_dir,
        bl_number=args.bl,
    )

    # Add BL PDF to attachments
    if bl_alloc and bl_alloc.bl_output_path:
        all_attachments.append(bl_alloc.bl_output_path)

    # ── Print summary ──

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"{'PDF File':<30} {'Supplier':<30} {'Format':<12} "
          f"{'Items':>6} {'Match':>6} {'Class':>6} {'Total':>12}")
    print("-" * 108)
    for r in results:
        pdf_items = len(r.invoice_data.get('items', []))
        print(f"{r.pdf_file:<30} {r.supplier_info.get('name', ''):<30} "
              f"{r.format_name:<12} "
              f"{pdf_items:>6} {r.matched_count:>6} "
              f"{r.classified_count:>6} ${r.invoice_data.get('invoice_total', 0):>10.2f}")
    print("-" * 108)
    total_items = sum(len(r.matched_items) for r in results)
    total_matched = sum(r.matched_count for r in results)
    total_classified = sum(r.classified_count for r in results)
    print(f"{'TOTAL':<30} {'':<30} {'':<12} "
          f"{total_items:>6} {total_matched:>6} {total_classified:>6}")
    print(f"\nOutput directory: {args.output_dir}")

    # ── Stage 4: Email ──

    if args.send_email and results:
        # Determine predominant country of origin
        countries = [r.supplier_info.get('country', 'US') for r in results]
        country_origin = max(set(countries), key=countries.count)

        # Use BL freight if available, else sum of invoice freight
        if bl_alloc:
            email_freight = bl_alloc.freight
            email_packages = bl_alloc.packages
            email_weight = bl_alloc.weight
        else:
            email_freight = sum(r.freight for r in results)
            email_packages = str(len(results))
            email_weight = '0'

        email_draft = compose_email(
            waybill=args.bl,
            consignee_name=args.consignee,
            consignee_code=args.consignee_code,
            total_invoices=len(results),
            packages=email_packages,
            weight=email_weight,
            country_origin=country_origin,
            freight=str(email_freight),
            man_reg=args.man_reg,
            attachment_paths=all_attachments,
            location=args.location,
            office=args.office,
        )

        email_sent = do_send_email(
            subject=email_draft['subject'],
            body=email_draft['body'],
            attachments=email_draft['attachments'],
        )

        if email_sent:
            print(f"\nEmail sent: {email_draft['subject']} "
                  f"({len(email_draft['attachments'])} attachments)")
        else:
            print(f"\nEmail FAILED to send")


if __name__ == "__main__":
    main()
