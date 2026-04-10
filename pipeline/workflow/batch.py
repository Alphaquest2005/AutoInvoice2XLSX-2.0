"""
Batch processing workflow - replaces the monolithic test_email_workflow.py.

Key architectural changes:
- Targeted retry: only re-run failed stages, not the whole pipeline
- State checkpoints: intermediate results are persisted and reused
- Deterministic email: subject/body composed from frozen metadata
- Format spec lifecycle: auto-gen specs promoted on success, discarded on failure
"""

import json
import logging
import os
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def process_single_invoice(
    invoice_path: str,
    declaration_path: str,
    declaration_metadata: dict,
    file_output_dir: str,
    base_dir: str,
    invoice_index: int = 1,
    total_invoices: int = 1,
    send_email: bool = True,
) -> dict:
    """
    Process one invoice through the pipeline with checkpoint/resume.

    Stages:
    1. Extract text (OCR) - cached, never re-run on retry
    2. Parse items (format spec) - re-run only if format spec changes
    3. Classify items - re-run only if parsed items change
    4. Generate XLSX
    5. Variance check + LLM fix if needed
    6. Send email (deterministic from frozen metadata)
    """
    import sys
    pipeline_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if pipeline_dir not in sys.path:
        sys.path.insert(0, pipeline_dir)
    pipe_dir = os.path.join(pipeline_dir, 'pipeline')
    if pipe_dir not in sys.path:
        sys.path.insert(0, pipe_dir)

    from core.config import get_config
    from core.state import PipelineState
    from workflow.email import compose_email, send_email as do_send_email
    from workflow.variance_fixer import fix_variance
    from workflow.format_spec_generator import generate_format_spec, promote_spec, discard_spec

    cfg = get_config(base_dir)

    invoice_name = os.path.basename(invoice_path)
    print(f"    Processing invoice {invoice_index}/{total_invoices}: {invoice_name}")

    # Load or create pipeline state (checkpoint/resume)
    state = PipelineState.load_or_create(file_output_dir, invoice_path)

    result = {
        'invoice_path': invoice_path,
        'invoice_index': invoice_index,
        'status': 'pending',
        'xlsx_path': None,
        'invoice_number': None,
        'email_sent': False,
        'errors': [],
    }

    try:
        # ─── Stage 1: Extract text (cached) ─────────────────
        if not state.is_stage_complete('extract') or not state.extracted_text:
            print(f"      [1/5] Extracting text...")
            state.extracted_text = _extract_text(invoice_path)
            state.ocr_method = 'pdfplumber+ocr'
            if state.extracted_text:
                state.mark_stage_complete('extract')
                state.save()
            else:
                state.mark_stage_failed('extract', 'No text extracted')
                result['errors'].append('No text could be extracted from invoice')
                result['status'] = 'extract_failed'
                notify_failed_import(
                    invoice_path=invoice_path,
                    reason="OCR extraction failed — no text could be extracted",
                    base_dir=base_dir,
                )
                return result
        else:
            print(f"      [1/5] Using cached extracted text")

        # ─── Stage 2: Parse items ───────────────────────────
        if not state.is_stage_complete('parse') or not state.parsed_data:
            print(f"      [2/5] Parsing items...")
            parse_result = _run_pipeline(invoice_path, file_output_dir, base_dir)

            if parse_result.get('status') in ('success', 'completed'):
                state.parsed_data = parse_result
                state.format_name = _get_format_name(parse_result)

                # Freeze invoice number on first successful extraction
                if not state.invoice_number:
                    state.invoice_number = _extract_invoice_number(parse_result, invoice_path, state.extracted_text)
                if not state.invoice_total:
                    state.invoice_total = _extract_invoice_total(parse_result)

                state.mark_stage_complete('parse')

                # Find generated XLSX
                xlsx_path = _find_xlsx(parse_result, file_output_dir, invoice_name)
                if xlsx_path:
                    state.xlsx_path = xlsx_path
                    state.variance = _extract_variance(parse_result)
                    state.mark_stage_complete('generate_xlsx')

                state.save()
            else:
                # Parse failed - try generating format spec
                print(f"      [2/5] Parse failed - generating format spec...")
                spec_result = generate_format_spec(
                    invoice_text=state.extracted_text,
                    detected_supplier=state.supplier_name,
                )

                if spec_result.get('success'):
                    state.auto_format_spec_path = spec_result['spec_path']
                    print(f"      Generated: {spec_result['format_name']}")

                    # Reload format registry and retry parse ONLY
                    from format_registry import FormatRegistry
                    FormatRegistry._instance = None

                    retry_result = _run_pipeline(invoice_path, file_output_dir, base_dir)

                    if retry_result.get('status') in ('success', 'completed'):
                        state.parsed_data = retry_result
                        state.format_name = _get_format_name(retry_result)

                        if not state.invoice_number:
                            state.invoice_number = _extract_invoice_number(retry_result, invoice_path, state.extracted_text)

                        state.mark_stage_complete('parse')

                        xlsx_path = _find_xlsx(retry_result, file_output_dir, invoice_name)
                        if xlsx_path:
                            state.xlsx_path = xlsx_path
                            state.variance = _extract_variance(retry_result)
                            state.mark_stage_complete('generate_xlsx')

                        state.save()
                    else:
                        # Retry failed too - discard the auto-generated spec
                        discard_spec(state.auto_format_spec_path)
                        state.auto_format_spec_path = None
                        state.mark_stage_failed('parse', 'Pipeline failed even with generated format spec')
                        result['status'] = 'pipeline_failed'
                        notify_failed_import(
                            invoice_path=invoice_path,
                            reason="Item extraction failed — no items could be parsed from invoice",
                            extracted_text=state.extracted_text,
                            format_name=state.format_name,
                            base_dir=base_dir,
                        )
                        state.save()
                        return result
                else:
                    state.mark_stage_failed('parse', f"Format spec generation failed: {spec_result.get('error')}")
                    result['status'] = 'pipeline_failed'
                    notify_failed_import(
                        invoice_path=invoice_path,
                        reason=f"Format spec generation failed: {spec_result.get('error')}",
                        extracted_text=state.extracted_text,
                        base_dir=base_dir,
                    )
                    state.save()
                    return result

        # ─── Stage 3: Rename files with frozen invoice number ──
        if state.invoice_number and state.xlsx_path:
            clean_num = _safe_filename(state.invoice_number)

            # Rename invoice PDF
            new_inv_path = _rename_file(invoice_path, f"{clean_num}.pdf")
            if new_inv_path != invoice_path:
                result['invoice_path'] = new_inv_path
                invoice_path = new_inv_path
                print(f"      Renamed invoice: {clean_num}.pdf")

            # Rename XLSX
            new_xlsx_path = _rename_file(state.xlsx_path, f"{clean_num}.xlsx")
            if new_xlsx_path != state.xlsx_path:
                state.xlsx_path = new_xlsx_path
                print(f"      Renamed XLSX: {clean_num}.xlsx")

        result['invoice_number'] = state.invoice_number
        result['xlsx_path'] = state.xlsx_path

        # ─── Stage 4: Variance check + fix ──────────────────
        if state.xlsx_path:
            variance = state.variance or 0
            variance_ok = abs(float(variance)) < cfg.variance_threshold

            if not variance_ok:
                print(f"      [4/5] Fixing variance ${variance:.2f}...")
                fix_result = fix_variance(
                    xlsx_path=state.xlsx_path,
                    invoice_text=state.extracted_text,
                    current_variance=float(variance),
                )

                if fix_result.get('success'):
                    state.variance = fix_result.get('new_variance', variance)
                    variance_ok = abs(float(state.variance)) < cfg.variance_threshold
                    result['llm_fix'] = fix_result
                    print(f"      Variance fixed: ${state.variance:.2f}")
                else:
                    print(f"      Variance fix failed: {fix_result.get('error')}")
                    result['errors'].append(f"Variance fix failed: {fix_result.get('error')}")
            else:
                print(f"      [4/5] Variance OK: ${variance:.2f}")

            result['variance_check'] = state.variance

            # ─── Promote or discard auto-generated format spec ──
            if state.auto_format_spec_path:
                if variance_ok:
                    promoted = promote_spec(state.auto_format_spec_path)
                    if promoted:
                        print(f"      Promoted format spec to production")
                else:
                    discard_spec(state.auto_format_spec_path)
                    state.auto_format_spec_path = None

        # ─── Stage 5: Send email (deterministic) ────────────
        variance = state.variance or 0
        variance_ok = abs(float(variance)) < cfg.variance_threshold

        if send_email and state.xlsx_path and variance_ok:
            print(f"      [5/5] Sending email...")

            # Compose from FROZEN metadata - no LLM involvement
            email_draft = compose_email(
                waybill=declaration_metadata.get('waybill'),
                consignee_name=declaration_metadata.get('consignee', 'Consignee Name Not Found'),
                invoice_number=state.invoice_number,  # Frozen from first extraction
                invoice_index=invoice_index,
                total_invoices=total_invoices,
                packages=declaration_metadata.get('packages', '1'),
                weight=declaration_metadata.get('weight', '0'),
                country_origin=declaration_metadata.get('country_origin', 'US'),
                freight=declaration_metadata.get('freight', '0'),
                man_reg=declaration_metadata.get('man_reg'),
                attachment_paths=[
                    declaration_path,
                    invoice_path,
                    state.xlsx_path,
                ],
            )

            email_sent = do_send_email(
                subject=email_draft['subject'],
                body=email_draft['body'],
                attachments=email_draft['attachments'],
            )

            result['email_sent'] = email_sent
            result['email_draft'] = email_draft

            if email_sent:
                print(f"      Email sent: {email_draft['subject']}")
            else:
                result['errors'].append("Email sending failed")
        elif send_email and state.xlsx_path and not variance_ok:
            result['email_skipped'] = True
            result['skip_reason'] = f"Variance ${variance:.2f} - needs manual correction"
            print(f"      [5/5] Email skipped: variance ${variance:.2f}")
            notify_failed_import(
                invoice_path=invoice_path,
                reason=f"Unresolved variance after LLM fix attempt",
                extracted_text=state.extracted_text,
                variance=float(variance),
                format_name=state.format_name,
                base_dir=base_dir,
            )
        else:
            print(f"      [5/5] No XLSX to email")

        result['status'] = 'success'
        state.status = 'success'
        state.save()

    except Exception as e:
        logger.error(f"Invoice processing error: {e}", exc_info=True)
        result['errors'].append(str(e))
        result['status'] = 'error'
        state.mark_stage_failed('unknown', str(e))
        state.save()

    return result


def process_pdf(pdf_path: str, output_dir: str, base_dir: str, index: int, send_email: bool = True) -> dict:
    """
    Process a single PDF through the full workflow.
    Handles splitting, multi-invoice support, and email sending.
    """
    import sys
    pipeline_dir = os.path.join(base_dir, 'pipeline')
    if pipeline_dir not in sys.path:
        sys.path.insert(0, pipeline_dir)

    from pdf_splitter import run as split_pdf

    filename = os.path.basename(pdf_path)
    print(f"\n{'='*60}")
    print(f"[{index}] Processing: {filename}")
    print(f"{'='*60}")

    result = {
        'input_file': pdf_path,
        'filename': filename,
        'status': 'pending',
        'output_files': [],
        'emails_sent': 0,
        'errors': [],
    }

    # Output directory for this file
    safe_name = _safe_filename(filename).replace('.pdf', '')
    file_output_dir = os.path.join(output_dir, safe_name)
    os.makedirs(file_output_dir, exist_ok=True)

    # Copy original
    original_copy = os.path.join(file_output_dir, filename)
    if not os.path.exists(original_copy):
        shutil.copy2(pdf_path, original_copy)
    result['output_files'].append(original_copy)

    # Step 1: Split PDF
    print(f"  [1/3] Splitting PDF...")
    try:
        split_result = split_pdf(pdf_path, output_dir=file_output_dir, split_invoices=True)

        if split_result.get('status') != 'success':
            result['errors'].append(f"Split failed: {split_result.get('error')}")
            result['status'] = 'split_failed'
            return result

        pages_info = split_result.get('pages', [])
        decl_pages = sum(1 for p in pages_info if p['doc_type'] == 'declaration')
        inv_pages = sum(1 for p in pages_info if p['doc_type'] == 'invoice')
        invoice_count = split_result.get('invoice_count', 1)
        print(f"       Split: {decl_pages} declaration, {inv_pages} invoice pages")
        print(f"       Detected {invoice_count} separate invoice(s)")

        # Rename declaration
        declaration_metadata = split_result.get('declaration_metadata', {}) or {}
        bl_number = declaration_metadata.get('waybill')
        output_files = split_result.get('output_files', {})

        if bl_number and 'declaration' in output_files:
            old_path = output_files['declaration']
            new_path = _rename_file(old_path, f"{bl_number}-Manifest.pdf")
            output_files['declaration'] = new_path
            print(f"       Renamed declaration: {bl_number}-Manifest.pdf")

        if 'declaration' in output_files and os.path.exists(output_files['declaration']):
            result['output_files'].append(output_files['declaration'])

        for inv_path in split_result.get('invoices', []):
            if os.path.exists(inv_path):
                result['output_files'].append(inv_path)

    except Exception as e:
        result['errors'].append(f"Split exception: {str(e)}")
        result['status'] = 'split_failed'
        return result

    # Step 2 & 3: Process each invoice
    invoices = split_result.get('invoices', [])
    declaration_path = output_files.get('declaration')

    if not invoices:
        legacy = output_files.get('invoice')
        if legacy:
            invoices = [legacy]

    total_invoices = len(invoices)

    if invoices:
        print(f"  [2/3] Processing {total_invoices} invoice(s)...")

        for i, inv_path in enumerate(invoices, 1):
            if not os.path.exists(inv_path):
                continue

            inv_result = process_single_invoice(
                invoice_path=inv_path,
                declaration_path=declaration_path,
                declaration_metadata=declaration_metadata,
                file_output_dir=file_output_dir,
                base_dir=base_dir,
                invoice_index=i,
                total_invoices=total_invoices,
                send_email=send_email,
            )

            if inv_result.get('xlsx_path'):
                result['output_files'].append(inv_result['xlsx_path'])
            if inv_result.get('email_sent'):
                result['emails_sent'] += 1
            if inv_result.get('errors'):
                result['errors'].extend(inv_result['errors'])

        print(f"  [3/3] Completed: {result['emails_sent']}/{total_invoices} emails sent")

    result['status'] = 'completed_with_errors' if result['errors'] else 'success'
    print(f"\n  Status: {result['status']}")
    print(f"  Output files: {len(result['output_files'])}")
    print(f"  Emails sent: {result['emails_sent']}/{total_invoices}")

    return result


# ─── Helper functions ─────────────────────────────────────


def _extract_text(invoice_path: str) -> str:
    """Extract text from invoice PDF (OCR if needed)."""
    text = ""
    try:
        import pdfplumber
        with pdfplumber.open(invoice_path) as pdf:
            for page in pdf.pages[:5]:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"
    except Exception:
        pass

    if not text.strip():
        try:
            from pdf_splitter import ocr_page
            import fitz
            doc = fitz.open(invoice_path)
            for page_num in range(min(doc.page_count, 5)):
                page_text = ocr_page(invoice_path, page_num, dpi=200)
                if page_text:
                    text += page_text + "\n"
            doc.close()
        except Exception:
            pass

    return text


def _run_pipeline(invoice_path: str, output_dir: str, base_dir: str) -> dict:
    """Run the core pipeline stages."""
    from pipeline_runner import PipelineRunner

    invoice_path = os.path.abspath(invoice_path)
    output_dir = os.path.abspath(output_dir)
    base_dir = os.path.abspath(base_dir)

    safe_name = _safe_filename(os.path.basename(invoice_path))
    xlsx_path = os.path.join(output_dir, safe_name.replace('.pdf', '.xlsx'))

    config_path = os.path.join(base_dir, 'pipeline.yaml')
    runner = PipelineRunner(config_path=config_path)
    runner.base_dir = Path(base_dir)

    return runner.run(input_file=invoice_path, output_file=xlsx_path)


def _extract_invoice_number(pipeline_result: dict, invoice_path: str, extracted_text: str) -> Optional[str]:
    """Extract invoice number from pipeline result, with fallbacks. Result is frozen after first call."""
    # From pipeline stages
    for stage in pipeline_result.get('stages', []):
        if stage.get('name') == 'extract':
            invoices = stage.get('invoices', [])
            if invoices:
                inv_num = invoices[0].get('invoice_number')
                if inv_num:
                    return inv_num

    # From extracted data
    inv_num = pipeline_result.get('extracted', {}).get('invoice_number')
    if inv_num:
        return inv_num

    # From work directory
    work_dir = pipeline_result.get('work_dir', '')
    if work_dir:
        for fname in ['extracted.json']:
            fpath = os.path.join(work_dir, fname)
            if os.path.exists(fpath):
                try:
                    with open(fpath, 'r') as f:
                        data = json.load(f)
                    inv_num = data.get('invoice_number')
                    if inv_num:
                        return inv_num
                except Exception:
                    pass

    # From text with regex
    if extracted_text:
        patterns = [
            r'ORDER\s*#?\s*(\d{3}-\d{7}-\d{7})',
            r'(\d{3}-\d{7}-\d{7})',
            r'ORDER\s*ID[:\s]*(PO-\d{3}-\d+)',
            r'(?:INVOICE|ORDER)\s*(?:#|NO\.?|NUMBER)[:\s]*([A-Z0-9][A-Z0-9-]{3,})',
        ]
        for pattern in patterns:
            match = re.search(pattern, extracted_text, re.IGNORECASE)
            if match:
                candidate = match.group(1).strip()
                if any(c.isdigit() for c in candidate):
                    return candidate

    return None


def _extract_invoice_total(pipeline_result: dict) -> Optional[float]:
    """Extract invoice total from pipeline result."""
    for stage in pipeline_result.get('stages', []):
        if stage.get('name') == 'extract':
            invoices = stage.get('invoices', [])
            if invoices:
                total = invoices[0].get('total')
                if total:
                    try:
                        return float(total)
                    except (ValueError, TypeError):
                        pass
    return None


def _extract_variance(pipeline_result: dict) -> Optional[float]:
    """Extract variance from pipeline result."""
    for stage in pipeline_result.get('stages', []):
        if stage.get('name') == 'generate_xlsx':
            return stage.get('variance_check', 0)
    return None


def _get_format_name(pipeline_result: dict) -> str:
    """Get format name used by pipeline."""
    for stage in pipeline_result.get('stages', []):
        if stage.get('name') == 'extract':
            return stage.get('format_name', 'unknown')
    return 'unknown'


def _find_xlsx(pipeline_result: dict, output_dir: str, invoice_name: str) -> Optional[str]:
    """Find generated XLSX from pipeline result."""
    actual = pipeline_result.get('output')
    if actual and os.path.exists(actual):
        return actual

    safe_name = _safe_filename(invoice_name)
    xlsx_path = os.path.join(output_dir, safe_name.replace('.pdf', '.xlsx'))
    if os.path.exists(xlsx_path):
        return xlsx_path

    for f in os.listdir(output_dir):
        if f.endswith('.xlsx'):
            return os.path.join(output_dir, f)

    return None


def _rename_file(old_path: str, new_name: str) -> str:
    """Rename file in same directory. Returns new path."""
    if not os.path.exists(old_path):
        return old_path
    directory = os.path.dirname(old_path)
    new_path = os.path.join(directory, new_name)
    if old_path != new_path:
        shutil.move(old_path, new_path)
    return new_path


def _safe_filename(name: str) -> str:
    """Make filename filesystem-safe."""
    return ''.join(c if c.isalnum() or c in '._-' else '_' for c in name)


def notify_failed_import(
    invoice_path: str,
    reason: str,
    extracted_text: str = "",
    variance: float = None,
    format_name: str = None,
    base_dir: str = None,
) -> bool:
    """
    Email a failed import to the shipments inbox for manual review.

    Triggered when:
    - OCR quality is too poor to extract items
    - LLM variance fixer cannot resolve the variance to zero

    Attaches the source PDF and extracted OCR text so staff can
    process the invoice manually.
    """
    import smtplib
    import ssl
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders

    try:
        from core.config import load_config
        cfg = load_config(base_dir)
    except Exception:
        logger.warning("Could not load config for failed import notification")
        return False

    filename = os.path.basename(invoice_path)

    # Build body
    lines = [
        f"Failed Import Notification",
        f"",
        f"File: {filename}",
        f"Reason: {reason}",
    ]
    if format_name:
        lines.append(f"Detected Format: {format_name}")
    if variance is not None:
        lines.append(f"Unresolved Variance: ${variance:.2f}")
    lines.append(f"")
    lines.append(f"--- Extracted OCR Text ---")
    lines.append(extracted_text[:10000] if extracted_text else "(no text extracted)")

    body = "\n".join(lines)

    try:
        msg = MIMEMultipart()
        msg['From'] = f"{cfg.email_sender_name} <{cfg.email_sender}>"
        msg['To'] = cfg.email_recipient
        msg['Subject'] = f"Failed Import: {filename}"
        msg.attach(MIMEText(body, 'plain'))

        # Attach source PDF
        if os.path.exists(invoice_path):
            with open(invoice_path, 'rb') as f:
                part = MIMEBase('application', 'pdf')
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
                msg.attach(part)

        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(cfg.smtp_host, cfg.smtp_port, context=context) as server:
            server.login(cfg.email_sender, cfg.email_password)
            server.send_message(msg)

        logger.info(f"Sent failed import notification for {filename}")
        return True

    except Exception as e:
        logger.warning(f"Failed to send import notification for {filename}: {e}")
        return False
