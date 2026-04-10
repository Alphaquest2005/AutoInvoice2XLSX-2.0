#!/usr/bin/env python3
"""
CLI entry point for folder/email batch processing.

Called by Electron's folder:process IPC handler with:
    python test_email_workflow.py <folder_path> --output-dir <dir> [--limit N] [--all] [--start N]

Delegates to workflow.batch for actual processing and writes a
processing_summary.json to the output directory.
"""

import argparse
import glob
import json
import os
import sys
import traceback

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PIPELINE_DIR = os.path.dirname(os.path.abspath(__file__))

# Ensure pipeline is on path
if PIPELINE_DIR not in sys.path:
    sys.path.insert(0, PIPELINE_DIR)


def main():
    parser = argparse.ArgumentParser(description='Process folder of PDF invoices')
    parser.add_argument('folder', help='Input folder containing PDF files')
    parser.add_argument('--output-dir', required=True, help='Output directory for results')
    parser.add_argument('--limit', type=int, help='Maximum number of PDFs to process')
    parser.add_argument('--all', action='store_true', help='Process all PDFs (no limit)')
    parser.add_argument('--start', type=int, default=0, help='Start index (skip first N)')
    parser.add_argument('--no-email', action='store_true', help='Skip email sending')
    args = parser.parse_args()

    folder_path = os.path.abspath(args.folder)
    output_dir = os.path.abspath(args.output_dir)

    if not os.path.isdir(folder_path):
        print(json.dumps({'success': False, 'error': f'Folder not found: {folder_path}'}))
        sys.exit(1)

    os.makedirs(output_dir, exist_ok=True)

    # Discover PDFs
    pdf_files = sorted(glob.glob(os.path.join(folder_path, '*.pdf')))
    if not pdf_files:
        pdf_files = sorted(glob.glob(os.path.join(folder_path, '**', '*.pdf'), recursive=True))

    if not pdf_files:
        print(json.dumps({'success': False, 'error': 'No PDF files found'}))
        sys.exit(1)

    # Apply start/limit
    if args.start > 0:
        pdf_files = pdf_files[args.start:]

    if not args.all and args.limit:
        pdf_files = pdf_files[:args.limit]

    print(f"Processing {len(pdf_files)} PDF(s) from {folder_path}")
    print(f"Output: {output_dir}")

    from workflow.batch import process_pdf

    results = []
    total = len(pdf_files)
    success_count = 0
    error_count = 0
    emails_sent = 0

    for i, pdf_path in enumerate(pdf_files, 1):
        try:
            result = process_pdf(
                pdf_path=pdf_path,
                output_dir=output_dir,
                base_dir=BASE_DIR,
                index=i,
                send_email=not args.no_email,
            )
            results.append(result)

            if result.get('status') == 'success':
                success_count += 1
            elif result.get('errors'):
                error_count += 1

            emails_sent += result.get('emails_sent', 0)

        except Exception as e:
            traceback.print_exc()
            results.append({
                'input_file': pdf_path,
                'status': 'error',
                'errors': [str(e)],
            })
            error_count += 1

    # Write summary
    summary = {
        'success': True,
        'total_files': total,
        'processed': len(results),
        'successful': success_count,
        'errors': error_count,
        'emails_sent': emails_sent,
        'output_dir': output_dir,
        'results': results,
    }

    summary_path = os.path.join(output_dir, 'processing_summary.json')
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2, default=str)

    print(f"\n{'='*60}")
    print(f"COMPLETE: {success_count}/{total} successful, {error_count} errors, {emails_sent} emails")
    print(f"Summary: {summary_path}")

    # Print JSON for IPC parsing
    print(json.dumps(summary, default=str))


if __name__ == '__main__':
    main()
