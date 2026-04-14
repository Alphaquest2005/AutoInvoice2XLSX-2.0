#!/usr/bin/env python3
"""
Standalone email sender — reads _email_params.json and sends via workflow/email.py.

This script is the ONLY way TypeScript sends shipment emails after pipeline processing.
It decouples email sending from the pipeline so the state machine can track it independently.

Usage:
    python pipeline/send_shipment_email.py --params /path/to/_email_params.json [--json-output]
"""

import argparse
import json
import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)


def main():
    parser = argparse.ArgumentParser(description='Send shipment email from saved params')
    parser.add_argument('--params', required=True, help='Path to _email_params.json')
    parser.add_argument('--json-output', action='store_true', help='Emit JSON report')
    args = parser.parse_args()

    # Collect all email params files to send
    # If --params points to _email_params.json, also check for _email_params_2.json, etc.
    params_files = []
    if not os.path.exists(args.params):
        report = {'status': 'error', 'email_sent': False, 'error': f'Params file not found: {args.params}'}
        if args.json_output:
            print(f"REPORT:JSON:{json.dumps(report)}")
        else:
            print(f"ERROR: {report['error']}")
        sys.exit(1)

    params_files.append(args.params)

    # Auto-discover additional email params files for multi-declaration shipments
    params_dir = os.path.dirname(args.params)
    params_base = os.path.basename(args.params)
    if params_base == '_email_params.json':
        idx = 2
        while True:
            extra = os.path.join(params_dir, f'_email_params_{idx}.json')
            if os.path.exists(extra):
                params_files.append(extra)
                idx += 1
            else:
                break

    if len(params_files) > 1:
        print(f"Found {len(params_files)} email params files (multi-declaration shipment)")

    from workflow.email import (
        compose_email,
        compose_proposed_fixes_email,
        send_email as do_send_email,
    )
    from xlsx_to_pdf import generate_worksheet_pdf

    all_sent = True
    all_reports = []
    for pf in params_files:
        with open(pf) as f:
            params = json.load(f)

        # Generate worksheet PDF for each XLSX attachment
        waybill = params.get('waybill', 'UNKNOWN')
        attachment_paths = list(params.get('attachment_paths', []))
        for ap in list(attachment_paths):
            if ap.endswith('.xlsx') and os.path.exists(ap):
                try:
                    ws_pdf = generate_worksheet_pdf(ap, waybill)
                    if ws_pdf not in attachment_paths:
                        attachment_paths.append(ws_pdf)
                        print(f"  Generated worksheet PDF: {os.path.basename(ws_pdf)}")
                except Exception as e:
                    print(f"  Warning: worksheet PDF generation failed: {e}")

        email_draft = compose_email(
            waybill=params.get('waybill', 'UNKNOWN'),
            consignee_name=params.get('consignee_name', ''),
            consignee_code=params.get('consignee_code', ''),
            consignee_address=params.get('consignee_address', ''),
            total_invoices=params.get('total_invoices', 1),
            packages=params.get('packages', '1'),
            weight=params.get('weight', '0'),
            country_origin=params.get('country_origin', 'US'),
            freight=params.get('freight', '0'),
            man_reg=params.get('man_reg', ''),
            attachment_paths=attachment_paths,
            location=params.get('location', ''),
            office=params.get('office', ''),
            expected_entries=params.get('expected_entries', 0),
            notes=params.get('notes', ''),
        )

        email_sent = do_send_email(
            subject=email_draft['subject'],
            body=email_draft['body'],
            attachments=email_draft['attachments'],
        )

        if email_sent:
            print(f"Email sent: {email_draft['subject']} ({len(email_draft['attachments'])} attachments)")
        else:
            print(f"Email FAILED: {email_draft['subject']}")
            all_sent = False

        all_reports.append({
            'status': 'success' if email_sent else 'error',
            'email_sent': email_sent,
            'subject': email_draft['subject'],
            'attachments': len(email_draft['attachments']),
            'params_file': os.path.basename(pf),
        })

    # ── Proposed Fixes sidecar ───────────────────────────────────
    # When the pipeline found uncertain invoices it wrote a companion
    # ``_proposed_fixes_params.json`` next to the shipment params file.
    # Send it to the fixes recipient (separate reviewer mailbox) using
    # the subject/body/attachments stored in the JSON verbatim.
    fixes_path = os.path.join(params_dir, '_proposed_fixes_params.json')
    if os.path.exists(fixes_path):
        try:
            with open(fixes_path) as f:
                fparams = json.load(f)
            fixes_draft = compose_proposed_fixes_email(
                waybill=fparams.get('waybill', 'UNKNOWN'),
                subject=fparams.get('subject', ''),
                body=fparams.get('body', ''),
                attachment_paths=fparams.get('attachment_paths', []),
            )
            # Route to the fixes recipient (reviewer mailbox).
            from core.config import get_config
            cfg = get_config()
            fixes_recipient = getattr(cfg, 'email_fixes_recipient', None) \
                or cfg.email_sender
            fixes_sent = do_send_email(
                subject=fixes_draft['subject'],
                body=fixes_draft['body'],
                attachments=fixes_draft['attachments'],
                recipient=fixes_recipient,
            )
            if fixes_sent:
                print(
                    f"Proposed Fixes email sent to {fixes_recipient}: "
                    f"{fixes_draft['subject']} "
                    f"({len(fixes_draft['attachments'])} attachments)"
                )
            else:
                print(f"Proposed Fixes email FAILED: {fixes_draft['subject']}")
                all_sent = False
            all_reports.append({
                'status': 'success' if fixes_sent else 'error',
                'email_sent': fixes_sent,
                'subject': fixes_draft['subject'],
                'attachments': len(fixes_draft['attachments']),
                'params_file': os.path.basename(fixes_path),
                'kind': 'proposed_fixes',
            })
        except Exception as e:
            print(f"Proposed Fixes email error: {e}")
            all_sent = False

    # Report: use first report for backward compatibility, include all in multi
    report = dict(all_reports[0]) if all_reports else {'status': 'error', 'email_sent': False}
    if len(all_reports) > 1:
        report['all_emails'] = all_reports
        report['email_sent'] = all_sent
        report['emails_sent'] = sum(1 for r in all_reports if r['email_sent'])

    if args.json_output:
        print(f"REPORT:JSON:{json.dumps(report)}")

    sys.exit(0 if all_sent else 1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"FATAL ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
