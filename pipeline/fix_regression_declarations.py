#!/usr/bin/env python3
"""
One-shot fix for downloads-regression-emails shipments.

Retroactively applies OCR-based declaration splitting and metadata extraction
to the 103 pre-processed shipment folders that were created without working OCR.

For each shipment with a manifest PDF:
1. OCR + analyze the manifest to classify pages (invoice vs declaration)
2. Split declaration pages into individual PDFs
3. Extract metadata from each declaration (waybill, consignee, freight, etc.)
4. Update _email_params.json with extracted metadata
5. Add declaration PDF(s) as attachments
6. For multi-declaration manifests: create _email_params_2.json, etc.

Usage:
    python pipeline/fix_regression_declarations.py [--send] [--folder FOLDER]
"""

import argparse
import copy
import json
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from pdf_splitter import analyze_pdf, extract_declaration_metadata, split_pdf_multi_invoice


def fix_shipment(fpath: str) -> dict:
    """Fix a single shipment folder. Returns result dict."""
    files = os.listdir(fpath)
    manifests = [x for x in files if 'Manifest' in x and x.endswith('.pdf')]

    if not manifests:
        return {'status': 'no_manifest'}

    mpath = os.path.join(fpath, manifests[0])

    # Analyze with OCR
    pages, used_ocr, _, page_texts = analyze_pdf(mpath)
    decl_pages = [p for p in pages if p.doc_type == 'declaration']

    if not decl_pages:
        return {'status': 'no_decl_detected'}

    # Split the manifest
    split_dir = os.path.join(fpath, '_split_temp')
    os.makedirs(split_dir, exist_ok=True)
    split_result = split_pdf_multi_invoice(mpath, pages, split_dir, page_texts=page_texts)

    # Extract metadata from each declaration
    declarations = []
    decl_pdf_paths = []
    for decl_path in split_result.get('declarations', []):
        meta = extract_declaration_metadata(decl_path)
        if meta and any(v for v in meta.values()):
            declarations.append(meta)
            decl_pdf_paths.append(os.path.abspath(decl_path))

    if not declarations:
        return {'status': 'no_meta_extracted'}

    # Load original params
    ep_path = os.path.join(fpath, '_email_params.json')
    with open(ep_path) as fh:
        orig_params = json.load(fh)

    def _apply_meta(params, meta, decl_pdf):
        """Apply declaration metadata to email params."""
        if meta.get('waybill'):
            params['waybill'] = meta['waybill']
        if meta.get('consignee'):
            params['consignee_name'] = meta['consignee']
        if meta.get('man_reg'):
            mr = meta['man_reg']
            if ' ' in mr and '/' not in mr:
                mr = mr.replace(' ', '/')
            params['man_reg'] = mr
        if meta.get('office'):
            params['office'] = meta['office']
        if meta.get('freight'):
            params['freight'] = meta['freight']
        if meta.get('weight'):
            params['weight'] = meta['weight']
        if meta.get('packages'):
            params['packages'] = meta['packages']
        # Add declaration PDF to attachments
        existing = set(params.get('attachment_paths', []))
        if decl_pdf and decl_pdf not in existing:
            params['attachment_paths'].append(decl_pdf)

    if len(declarations) == 1:
        _apply_meta(orig_params, declarations[0], decl_pdf_paths[0] if decl_pdf_paths else None)
        with open(ep_path, 'w') as fh:
            json.dump(orig_params, fh, indent=2)
        return {
            'status': 'single_decl',
            'waybill': declarations[0].get('waybill'),
            'consignee': declarations[0].get('consignee'),
            'freight': declarations[0].get('freight'),
        }
    else:
        # Strip any prior declaration PDFs from base params before creating per-decl copies
        base_attachments = [a for a in orig_params.get('attachment_paths', [])
                            if 'Declaration' not in os.path.basename(a)]
        waybills = []
        for idx, meta in enumerate(declarations):
            params = copy.deepcopy(orig_params)
            params['attachment_paths'] = list(base_attachments)
            dp = decl_pdf_paths[idx] if idx < len(decl_pdf_paths) else None
            _apply_meta(params, meta, dp)

            if idx == 0:
                out_path = os.path.join(fpath, '_email_params.json')
            else:
                out_path = os.path.join(fpath, f'_email_params_{idx + 1}.json')

            with open(out_path, 'w') as fh:
                json.dump(params, fh, indent=2)
            waybills.append(meta.get('waybill'))

        return {
            'status': 'multi_decl',
            'count': len(declarations),
            'waybills': waybills,
        }


def main():
    parser = argparse.ArgumentParser(description='Fix regression shipment declarations')
    parser.add_argument('--send', action='store_true', help='Send emails after fixing')
    parser.add_argument('--folder', help='Fix only this folder (name, not full path)')
    parser.add_argument('--base', default='workspace/output/downloads-regression-emails',
                        help='Base directory containing shipment folders')
    args = parser.parse_args()

    base = os.path.join(os.path.dirname(SCRIPT_DIR), args.base)
    if args.folder:
        folders = [args.folder]
    else:
        folders = sorted([d for d in os.listdir(base) if os.path.isdir(os.path.join(base, d))])

    stats = {'no_manifest': 0, 'no_decl_detected': 0, 'no_meta_extracted': 0,
             'single_decl': 0, 'multi_decl': 0, 'errors': 0}

    for i, f in enumerate(folders):
        fpath = os.path.join(base, f)
        print(f'[{i + 1}/{len(folders)}] {f}...', end=' ', flush=True)
        try:
            result = fix_shipment(fpath)
            status = result['status']
            stats[status] = stats.get(status, 0) + 1

            if status == 'single_decl':
                print(f"1 decl: {result.get('waybill')} / {result.get('consignee')} / freight={result.get('freight')}")
            elif status == 'multi_decl':
                print(f"{result['count']} decls: {result['waybills']}")
            else:
                print(status)

            if args.send and status in ('single_decl', 'multi_decl'):
                ep = os.path.join(fpath, '_email_params.json')
                os.system(f'python3 {SCRIPT_DIR}/send_shipment_email.py --params "{ep}" --json-output')

        except Exception as e:
            stats['errors'] += 1
            print(f'ERROR: {e}')

    print(f'\n=== SUMMARY ===')
    for k, v in stats.items():
        if v:
            print(f'  {k}: {v}')


if __name__ == '__main__':
    main()
