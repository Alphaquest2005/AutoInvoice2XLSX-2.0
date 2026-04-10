#!/usr/bin/env python3
"""
Integration tests: email pipeline end-to-end.

Auto-discovers email directories under workspace/documents/ and generates
one test per directory. Each test:
  1. Marks the source email as unread on IMAP (so the app would reprocess it)
  2. Copies source PDFs to a temporary working directory
  3. Runs pipeline/run.py --input-dir --output-dir --json-output (same as app)
  4. Verifies pipeline success, invoice processing, email params, checklist
  5. Sends email via send_shipment_email.py (same path as app ipc-handlers)
"""

import glob
import imaplib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PIPELINE_DIR = os.path.join(BASE_DIR, 'pipeline')
DOCUMENTS_DIR = os.path.join(BASE_DIR, 'workspace', 'documents')
CLIENTS_DB = os.path.join(BASE_DIR, 'data', 'clients.db')

REQUIRED_EMAIL_FIELDS = ('waybill', 'consignee_name', 'packages', 'weight', 'freight')

TIMEOUT_SECONDS = 600


# ─── IMAP Helpers ──────────────────────────────────────────────

def _get_imap_config():
    """Read IMAP credentials from the clients database."""
    if not os.path.exists(CLIENTS_DB):
        return None
    try:
        # Copy DB to avoid locking issues with the running app
        tmp_db = os.path.join(tempfile.gettempdir(), 'test_clients_copy.db')
        shutil.copy2(CLIENTS_DB, tmp_db)
        conn = sqlite3.connect(tmp_db)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute('SELECT * FROM clients WHERE enabled = 1 LIMIT 1')
        row = cur.fetchone()
        conn.close()
        os.remove(tmp_db)
        if row:
            return {
                'server': row['incoming_server'],
                'port': row['incoming_port'],
                'user': row['incoming_address'],
                'password': row['incoming_password'],
                'ssl': bool(row['incoming_ssl']),
            }
    except Exception as e:
        print(f"    WARNING: Could not read IMAP config: {e}")
    return None


def _get_email_subject(dir_path):
    """Extract email subject from email.txt in the directory."""
    email_txt = os.path.join(dir_path, 'email.txt')
    if not os.path.exists(email_txt):
        return None
    try:
        with open(email_txt, 'r', encoding='utf-8') as f:
            for line in f:
                if line.startswith('Subject: '):
                    return line[len('Subject: '):].strip()
    except Exception:
        pass
    return None


def _mark_email_unread(imap_config, subject):
    """Connect to IMAP and mark the email with matching subject as unread."""
    if not imap_config or not subject:
        return False

    try:
        if imap_config['ssl']:
            imap = imaplib.IMAP4_SSL(imap_config['server'], imap_config['port'])
        else:
            imap = imaplib.IMAP4(imap_config['server'], imap_config['port'])

        imap.login(imap_config['user'], imap_config['password'])
        imap.select('INBOX')

        # Search by subject
        # IMAP SEARCH requires encoded subject for non-ASCII
        status, data = imap.search(None, 'SUBJECT', f'"{subject}"')
        if status == 'OK' and data[0]:
            uids = data[0].split()
            for uid in uids:
                # Remove \Seen flag to mark as unread
                imap.store(uid, '-FLAGS', '\\Seen')
            print(f"    Marked {len(uids)} message(s) as unread: {subject[:50]}")
            imap.close()
            imap.logout()
            return True
        else:
            print(f"    WARNING: No IMAP messages found for subject: {subject[:50]}")
            imap.close()
            imap.logout()
    except Exception as e:
        print(f"    WARNING: IMAP mark-unread failed: {e}")
    return False


# ─── Test Discovery ────────────────────────────────────────────

def discover_email_dirs():
    """Return list of (dir_name, dir_path) for email directories that contain PDFs."""
    if not os.path.isdir(DOCUMENTS_DIR):
        return []

    results = []
    for entry in sorted(os.listdir(DOCUMENTS_DIR)):
        dir_path = os.path.join(DOCUMENTS_DIR, entry)
        if not os.path.isdir(dir_path):
            continue
        # Check for at least one PDF
        pdfs = glob.glob(os.path.join(dir_path, '*.pdf')) + \
               glob.glob(os.path.join(dir_path, '*.PDF'))
        if not pdfs:
            continue
        results.append((entry, dir_path))
    return results


def _sanitize_method_name(dir_name):
    """Convert a directory name into a valid Python method name."""
    name = re.sub(r'[^a-zA-Z0-9]+', '_', dir_name)
    name = name.strip('_').lower()
    return name


# ─── Test Factory ──────────────────────────────────────────────

def _make_test(dir_name, dir_path):
    """Create a test method for a given email directory."""

    def test_method(self):
        # Step 0: Mark email as unread on IMAP
        imap_config = _get_imap_config()
        subject = _get_email_subject(dir_path)
        if imap_config and subject:
            _mark_email_unread(imap_config, subject)
        else:
            print(f"    Skipping IMAP mark-unread (no config or no subject)")

        # Step 1: Create temp working directory with source PDFs
        temp_dir = tempfile.mkdtemp(prefix=f'test_email_{_sanitize_method_name(dir_name)}_')
        try:
            for fname in os.listdir(dir_path):
                src = os.path.join(dir_path, fname)
                if os.path.isfile(src):
                    shutil.copy2(src, os.path.join(temp_dir, fname))

            # Step 2: Run pipeline (same as app's runBLPipeline in ipc-handlers.ts)
            result = subprocess.run(
                [sys.executable, '-u', 'run.py',
                 '--input-dir', temp_dir,
                 '--output-dir', temp_dir,
                 '--json-output'],
                cwd=PIPELINE_DIR,
                capture_output=True, text=True, timeout=TIMEOUT_SECONDS,
            )

            # Pipeline must exit cleanly
            self.assertEqual(
                result.returncode, 0,
                f"Pipeline exited with code {result.returncode}.\n"
                f"stderr (last 1000 chars): {result.stderr[-1000:]}\n"
                f"stdout (last 1000 chars): {result.stdout[-1000:]}"
            )

            # Extract JSON report from stdout
            report = None
            for line in result.stdout.split('\n'):
                stripped = line.strip()
                if stripped.startswith('REPORT:JSON:'):
                    report = json.loads(stripped[len('REPORT:JSON:'):])
                    break

            self.assertIsNotNone(
                report,
                f"Pipeline did not output JSON report.\n"
                f"stdout (last 1000 chars): {result.stdout[-1000:]}"
            )

            # Report status should be success
            status = report.get('status', '')
            self.assertEqual(
                status, 'success',
                f"Report status should be 'success', got '{status}'. Report: {json.dumps(report, indent=2)[:1000]}"
            )

            # At least 1 invoice processed
            # BL-only or manifest-only emails have 0 invoices — cross-email
            # matching happens in the app's TypeScript layer (classifyRoute),
            # not in run.py. Skip these gracefully.
            invoice_count = report.get('invoice_count', 0)
            if invoice_count == 0:
                self.skipTest(
                    f"BL/manifest-only email — no invoices to process. "
                    f"Cross-email matching is handled by the app layer."
                )
                return

            # Email params file must exist with required fields
            email_params_path = report.get('email_params_path', '')
            self.assertTrue(
                email_params_path and os.path.exists(email_params_path),
                f"Email params file should exist at: {email_params_path}"
            )

            with open(email_params_path, 'r') as f:
                email_params = json.load(f)

            for field in REQUIRED_EMAIL_FIELDS:
                self.assertIn(
                    field, email_params,
                    f"Email params missing required field '{field}'. "
                    f"Available fields: {list(email_params.keys())}"
                )

            # All attachment files must exist
            attachment_paths = email_params.get('attachment_paths', [])
            self.assertGreater(
                len(attachment_paths), 0,
                "Email params should list at least one attachment"
            )
            for att_path in attachment_paths:
                # Handle Windows-style paths in WSL
                check_path = att_path.replace('C:\\', '/mnt/c/').replace('\\', '/')
                self.assertTrue(
                    os.path.exists(check_path),
                    f"Attachment file should exist: {att_path} (checked: {check_path})"
                )

            # Checklist must pass (no blockers)
            checklist = report.get('checklist')
            if checklist is not None:
                self.assertTrue(
                    checklist.get('passed', False),
                    f"Checklist should pass. Blockers: {checklist.get('blocker_count', '?')}. "
                    f"Items: {json.dumps(checklist.get('failures', []), indent=2)[:500]}"
                )

            # Step 3: Send email via send_shipment_email.py
            # (same path as app's sendShipmentEmailFromParams in email-processor.ts)
            send_result = subprocess.run(
                [sys.executable, '-u',
                 os.path.join(PIPELINE_DIR, 'send_shipment_email.py'),
                 '--params', email_params_path,
                 '--json-output'],
                cwd=PIPELINE_DIR,
                capture_output=True, text=True, timeout=TIMEOUT_SECONDS,
            )

            self.assertEqual(
                send_result.returncode, 0,
                f"send_shipment_email.py exited with code {send_result.returncode}.\n"
                f"stderr: {send_result.stderr[-500:]}\n"
                f"stdout: {send_result.stdout[-500:]}"
            )

            # Verify email was sent
            email_sent = False
            for line in send_result.stdout.split('\n'):
                stripped = line.strip()
                if stripped.startswith('REPORT:JSON:'):
                    send_report = json.loads(stripped[len('REPORT:JSON:'):])
                    email_sent = send_report.get('email_sent', False)
                    break
                if 'Email sent:' in stripped:
                    email_sent = True
                    break

            self.assertTrue(
                email_sent,
                f"Email should have been sent.\n"
                f"stdout: {send_result.stdout[-500:]}"
            )

        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    test_method.__doc__ = f"Email pipeline: {dir_name}"
    return test_method


class TestEmailPipeline(unittest.TestCase):
    """Dynamically generated integration tests for each email in workspace/documents/."""
    pass


# Dynamically add test methods for each discovered email directory
for _dir_name, _dir_path in discover_email_dirs():
    _method_name = f'test_{_sanitize_method_name(_dir_name)}'
    # Ensure unique method names
    if hasattr(TestEmailPipeline, _method_name):
        _method_name += '_2'
    setattr(TestEmailPipeline, _method_name, _make_test(_dir_name, _dir_path))


if __name__ == '__main__':
    loader = unittest.TestLoader()
    loader.sortTestMethodsUsing = lambda x, y: (x > y) - (x < y)
    suite = loader.loadTestsFromTestCase(TestEmailPipeline)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
