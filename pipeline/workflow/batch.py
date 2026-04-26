"""
Batch processing workflow - replaces the monolithic test_email_workflow.py.

Key architectural changes:
- Targeted retry: only re-run failed stages, not the whole pipeline
- State checkpoints: intermediate results are persisted and reused
- Deterministic email: subject/body composed from frozen metadata
- Format spec lifecycle: auto-gen specs promoted on success, discarded on failure
- Per-invoice consignee + doc_type resolution: each invoice's resolved
  doc_type is plumbed through the pipeline_runner via a thread-local
  shim on xlsx_generator.run so the rebuild path no longer silently
  defaults to 4000-000 when the resolver picked Budget Marine -> 7400-000.
"""

import json
import logging
import os
import re
import shutil
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# Bootstrap pipeline/ onto sys.path so config_loader resolves before
# any other pipeline import below relies on it.
import sys as _sys
_PIPELINE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PIPELINE_DIR not in _sys.path:
    _sys.path.insert(0, _PIPELINE_DIR)

from config_loader import (
    load_file_paths,
    load_issue_types,
    load_library_enums,
    load_patterns,
    load_pipeline,
)

logger = logging.getLogger(__name__)

# ── Config-loaded constants (every literal lives in config/*.yaml|json) ──
_FILE_PATHS = load_file_paths()
_ISSUE = load_issue_types()
_LIBRARY = load_library_enums()
_PATTERNS = load_patterns()
_PIPE = load_pipeline()

# Stage names (PipelineState contract).
_STAGES = _PIPE["batch_stages"]
_STAGE_EXTRACT       = _STAGES["EXTRACT"]
_STAGE_PARSE         = _STAGES["PARSE"]
_STAGE_GENERATE_XLSX = _STAGES["GENERATE_XLSX"]
_STAGE_UNKNOWN       = _STAGES["UNKNOWN"]

# Status enum values (granular per-invoice statuses).
_BATCH_STATUS = _ISSUE["batch_run_status"]
_STATUS_PENDING               = _BATCH_STATUS["PENDING"]
_STATUS_COMPLETED             = _BATCH_STATUS["COMPLETED"]
_STATUS_EXTRACT_FAILED        = _BATCH_STATUS["EXTRACT_FAILED"]
_STATUS_PIPELINE_FAILED       = _BATCH_STATUS["PIPELINE_FAILED"]
_STATUS_SPLIT_FAILED          = _BATCH_STATUS["SPLIT_FAILED"]
_STATUS_COMPLETED_WITH_ERRORS = _BATCH_STATUS["COMPLETED_WITH_ERRORS"]
_STATUS_SUCCESS = _ISSUE["status"]["SUCCESS"]
_STATUS_ERROR   = _ISSUE["status"]["ERROR"]

# Default values injected into compose_email() when upstream missing.
_EMAIL_DEFAULTS = _PIPE["batch_compose_email_defaults"]
_DEFAULT_CONSIGNEE_NAME = _EMAIL_DEFAULTS["consignee_name"]
_DEFAULT_PACKAGES       = _EMAIL_DEFAULTS["packages"]
_DEFAULT_WEIGHT         = _EMAIL_DEFAULTS["weight"]
_DEFAULT_FREIGHT        = _EMAIL_DEFAULTS["freight"]
_DEFAULT_COUNTRY_ORIGIN = _EMAIL_DEFAULTS["country_origin"]

# OCR method tag recorded on PipelineState for traceability.
_OCR_METHOD_PDFPLUMBER_OCR = _PIPE["batch_ocr_method_tags"]["PDFPLUMBER_OCR"]

# pdf_splitter doc_type values (per-page kind).
_SPLIT_DOC_DECLARATION = _PIPE["pdf_split_doc_types"]["DECLARATION"]
_SPLIT_DOC_INVOICE     = _PIPE["pdf_split_doc_types"]["INVOICE"]

# UI / formatting constants.
_BANNER_WIDTH                = _PIPE["batch_banner_width"]
_EXTRACT_TEXT_TRUNCATE_CHARS = _PIPE["batch_extract_text_truncation_chars"]

# File extensions / paths.
_EXT_PDF  = _FILE_PATHS["extensions"]["pdf"]
_EXT_XLSX = _FILE_PATHS["extensions"]["xlsx"]
_PIPELINE_DIR_NAME    = _FILE_PATHS["source_dirs"]["pipeline"]
_PIPELINE_CONFIG_NAME = _FILE_PATHS["config_basenames"]["pipeline"]
_WORK_EXTRACTED_JSON  = _FILE_PATHS["pipeline_work_files"]["extracted_json"]

# Email/MIME library values.
_MIME_TEXT_SUBTYPE       = _LIBRARY["email_mime"]["text_subtype"]
_MIME_BASE_MAIN_TYPE     = _LIBRARY["email_mime"]["base_main_type"]
_MIME_BASE_PDF_TYPE      = _LIBRARY["email_mime"]["base_pdf_subtype"]
_MIME_HEADER_DISPOSITION = _LIBRARY["email_mime"]["header_disposition"]

# Invoice-number recovery patterns from OCR text (first match wins).
_INVOICE_NUMBER_PATTERNS = _PATTERNS["batch_invoice_number_patterns"]

# User-visible failure messages.
_BATCH_MSG = _PIPE["batch_messages"]
_MSG_NO_TEXT_EXTRACTED         = _BATCH_MSG["no_text_extracted_short"]
_MSG_NO_TEXT_USER              = _BATCH_MSG["no_text_extracted_user"]
_MSG_OCR_EXTRACT_FAILED        = _BATCH_MSG["ocr_extract_failed_reason"]
_MSG_PIPELINE_FAILED_WITH_SPEC = _BATCH_MSG["pipeline_failed_with_spec"]
_MSG_ITEM_EXTRACTION_FAILED    = _BATCH_MSG["item_extraction_failed"]
_MSG_EMAIL_SEND_FAILED         = _BATCH_MSG["email_send_failed"]
_NO_TEXT_PLACEHOLDER           = _BATCH_MSG["no_text_placeholder"]

# ── Document-type plumbing shim ──────────────────────────────────────
# pipeline_runner.PipelineRunner.run() rebuilds self.context from the
# argv it receives, clobbering any document_type a caller might pre-set.
# Until that file is refactored to accept an extra_context kwarg, we
# inject document_type by monkey-patching xlsx_generator.run (the only
# downstream consumer) to read from a thread-local set by _run_pipeline.
# Per-thread so concurrent worker threads don't bleed values into each
# other. Idempotent — applied at most once per process.
_doc_type_thread_local = threading.local()
_xlsx_patch_applied = False


def _apply_xlsx_doc_type_shim() -> None:
    """Wrap xlsx_generator.run so it reads document_type from our
    thread-local when the caller's context dict didn't supply one.
    Safe to call multiple times — only the first call patches."""
    global _xlsx_patch_applied
    if _xlsx_patch_applied:
        return
    import xlsx_generator
    _original_run = xlsx_generator.run

    def _patched_run(input_path, output_path, config=None, context=None):
        ctx = dict(context or {})
        if not ctx.get("document_type"):
            injected = getattr(_doc_type_thread_local, "value", None)
            if injected:
                ctx["document_type"] = injected
        return _original_run(input_path, output_path, config=config, context=ctx)

    xlsx_generator.run = _patched_run
    _xlsx_patch_applied = True


def _resolve_doc_type_for_invoice(extracted_text: str,
                                  declaration_metadata: dict) -> dict:
    """Per-invoice consignee + doc_type via the SSOT resolver.

    Returns the full resolver dict (consignee_name / doc_type / source /
    matched_rule) so callers can record provenance for downstream
    grouping and the consignee_unrecognised checklist finding.
    """
    from consignee_resolver import resolve_invoice_consignee
    decl = declaration_metadata or {}
    return resolve_invoice_consignee(
        invoice_text=extracted_text or "",
        bl_consignee=(decl.get("consignee") or ""),
        manifest_consignee=(decl.get("manifest_consignee") or ""),
    )


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
    4. Generate XLSX (with per-invoice doc_type from consignee resolver)
    5. Variance check + LLM fix if needed
    6. Send email (deterministic from frozen metadata)
    """
    import sys
    pipeline_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if pipeline_dir not in sys.path:
        sys.path.insert(0, pipeline_dir)
    pipe_dir = os.path.join(pipeline_dir, _PIPELINE_DIR_NAME)
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

    state = PipelineState.load_or_create(file_output_dir, invoice_path)

    result = {
        "invoice_path": invoice_path,
        "invoice_index": invoice_index,
        "status": _STATUS_PENDING,
        "xlsx_path": None,
        "invoice_number": None,
        "email_sent": False,
        "errors": [],
    }

    try:
        # ─── Stage 1: Extract text (cached) ─────────────────
        if not state.is_stage_complete(_STAGE_EXTRACT) or not state.extracted_text:
            print(f"      [1/5] Extracting text...")
            state.extracted_text = _extract_text(invoice_path)
            state.ocr_method = _OCR_METHOD_PDFPLUMBER_OCR
            if state.extracted_text:
                state.mark_stage_complete(_STAGE_EXTRACT)
                state.save()
            else:
                state.mark_stage_failed(_STAGE_EXTRACT, _MSG_NO_TEXT_EXTRACTED)
                result["errors"].append(_MSG_NO_TEXT_USER)
                result["status"] = _STATUS_EXTRACT_FAILED
                notify_failed_import(
                    invoice_path=invoice_path,
                    reason=_MSG_OCR_EXTRACT_FAILED,
                    base_dir=base_dir,
                )
                return result
        else:
            print(f"      [1/5] Using cached extracted text")

        # ── Resolve consignee + doc_type from invoice text + BL fallback ──
        # This is the per-invoice resolution that closes the rebuild-path
        # leak documented in project_consignee_resolver_ssot.md. The
        # resolved doc_type flows into _run_pipeline -> xlsx_generator
        # via the thread-local shim above.
        consignee_resolution = _resolve_doc_type_for_invoice(
            state.extracted_text, declaration_metadata
        )
        result["consignee_resolution"] = consignee_resolution

        # ─── Stage 2: Parse items ───────────────────────────
        if not state.is_stage_complete(_STAGE_PARSE) or not state.parsed_data:
            print(f"      [2/5] Parsing items...")
            parse_result = _run_pipeline(
                invoice_path, file_output_dir, base_dir,
                document_type=consignee_resolution.get("doc_type"),
            )

            if parse_result.get("status") in (_STATUS_SUCCESS, _STATUS_COMPLETED):
                state.parsed_data = parse_result
                state.format_name = _get_format_name(parse_result)

                if not state.invoice_number:
                    state.invoice_number = _extract_invoice_number(parse_result, invoice_path, state.extracted_text)
                if not state.invoice_total:
                    state.invoice_total = _extract_invoice_total(parse_result)

                state.mark_stage_complete(_STAGE_PARSE)

                xlsx_path = _find_xlsx(parse_result, file_output_dir, invoice_name)
                if xlsx_path:
                    state.xlsx_path = xlsx_path
                    state.variance = _extract_variance(parse_result)
                    state.mark_stage_complete(_STAGE_GENERATE_XLSX)

                state.save()
            else:
                print(f"      [2/5] Parse failed - generating format spec...")
                spec_result = generate_format_spec(
                    invoice_text=state.extracted_text,
                    detected_supplier=state.supplier_name,
                )

                if spec_result.get("success"):
                    state.auto_format_spec_path = spec_result["spec_path"]
                    print(f"      Generated: {spec_result['format_name']}")

                    from format_registry import FormatRegistry
                    FormatRegistry._instance = None

                    retry_result = _run_pipeline(
                        invoice_path, file_output_dir, base_dir,
                        document_type=consignee_resolution.get("doc_type"),
                    )

                    if retry_result.get("status") in (_STATUS_SUCCESS, _STATUS_COMPLETED):
                        state.parsed_data = retry_result
                        state.format_name = _get_format_name(retry_result)

                        if not state.invoice_number:
                            state.invoice_number = _extract_invoice_number(retry_result, invoice_path, state.extracted_text)

                        state.mark_stage_complete(_STAGE_PARSE)

                        xlsx_path = _find_xlsx(retry_result, file_output_dir, invoice_name)
                        if xlsx_path:
                            state.xlsx_path = xlsx_path
                            state.variance = _extract_variance(retry_result)
                            state.mark_stage_complete(_STAGE_GENERATE_XLSX)

                        state.save()
                    else:
                        discard_spec(state.auto_format_spec_path)
                        state.auto_format_spec_path = None
                        state.mark_stage_failed(_STAGE_PARSE, _MSG_PIPELINE_FAILED_WITH_SPEC)
                        result["status"] = _STATUS_PIPELINE_FAILED
                        notify_failed_import(
                            invoice_path=invoice_path,
                            reason=_MSG_ITEM_EXTRACTION_FAILED,
                            extracted_text=state.extracted_text,
                            format_name=state.format_name,
                            base_dir=base_dir,
                        )
                        state.save()
                        return result
                else:
                    state.mark_stage_failed(_STAGE_PARSE, f"Format spec generation failed: {spec_result.get('error')}")
                    result["status"] = _STATUS_PIPELINE_FAILED
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

            new_inv_path = _rename_file(invoice_path, f"{clean_num}{_EXT_PDF}")
            if new_inv_path != invoice_path:
                result["invoice_path"] = new_inv_path
                invoice_path = new_inv_path
                print(f"      Renamed invoice: {clean_num}{_EXT_PDF}")

            new_xlsx_path = _rename_file(state.xlsx_path, f"{clean_num}{_EXT_XLSX}")
            if new_xlsx_path != state.xlsx_path:
                state.xlsx_path = new_xlsx_path
                print(f"      Renamed XLSX: {clean_num}{_EXT_XLSX}")

        result["invoice_number"] = state.invoice_number
        result["xlsx_path"] = state.xlsx_path

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

                if fix_result.get("success"):
                    state.variance = fix_result.get("new_variance", variance)
                    variance_ok = abs(float(state.variance)) < cfg.variance_threshold
                    result["llm_fix"] = fix_result
                    print(f"      Variance fixed: ${state.variance:.2f}")
                else:
                    print(f"      Variance fix failed: {fix_result.get('error')}")
                    result["errors"].append(f"Variance fix failed: {fix_result.get('error')}")
            else:
                print(f"      [4/5] Variance OK: ${variance:.2f}")

            result["variance_check"] = state.variance

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

            email_draft = compose_email(
                waybill=declaration_metadata.get("waybill"),
                consignee_name=declaration_metadata.get("consignee", _DEFAULT_CONSIGNEE_NAME),
                invoice_number=state.invoice_number,
                invoice_index=invoice_index,
                total_invoices=total_invoices,
                packages=declaration_metadata.get("packages", _DEFAULT_PACKAGES),
                weight=declaration_metadata.get("weight", _DEFAULT_WEIGHT),
                country_origin=declaration_metadata.get("country_origin", _DEFAULT_COUNTRY_ORIGIN),
                freight=declaration_metadata.get("freight", _DEFAULT_FREIGHT),
                man_reg=declaration_metadata.get("man_reg"),
                attachment_paths=[
                    declaration_path,
                    invoice_path,
                    state.xlsx_path,
                ],
            )

            email_sent = do_send_email(
                subject=email_draft["subject"],
                body=email_draft["body"],
                attachments=email_draft["attachments"],
            )

            result["email_sent"] = email_sent
            result["email_draft"] = email_draft

            if email_sent:
                print(f"      Email sent: {email_draft['subject']}")
            else:
                result["errors"].append(_MSG_EMAIL_SEND_FAILED)
        elif send_email and state.xlsx_path and not variance_ok:
            result["email_skipped"] = True
            result["skip_reason"] = f"Variance ${variance:.2f} - needs manual correction"
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

        result["status"] = _STATUS_SUCCESS
        state.status = _STATUS_SUCCESS
        state.save()

    except Exception as e:
        logger.error(f"Invoice processing error: {e}", exc_info=True)
        result["errors"].append(str(e))
        result["status"] = _STATUS_ERROR
        state.mark_stage_failed(_STAGE_UNKNOWN, str(e))
        state.save()

    return result


def process_pdf(pdf_path: str, output_dir: str, base_dir: str, index: int, send_email: bool = True) -> dict:
    """
    Process a single PDF through the full workflow.
    Handles splitting, multi-invoice support, and email sending.
    """
    import sys
    pipeline_dir = os.path.join(base_dir, _PIPELINE_DIR_NAME)
    if pipeline_dir not in sys.path:
        sys.path.insert(0, pipeline_dir)

    from pdf_splitter import run as split_pdf

    filename = os.path.basename(pdf_path)
    print(f"\n{'='*_BANNER_WIDTH}")
    print(f"[{index}] Processing: {filename}")
    print(f"{'='*_BANNER_WIDTH}")

    result = {
        "input_file": pdf_path,
        "filename": filename,
        "status": _STATUS_PENDING,
        "output_files": [],
        "emails_sent": 0,
        "errors": [],
    }

    safe_name = _safe_filename(filename).replace(_EXT_PDF, "")
    file_output_dir = os.path.join(output_dir, safe_name)
    os.makedirs(file_output_dir, exist_ok=True)

    original_copy = os.path.join(file_output_dir, filename)
    if not os.path.exists(original_copy):
        shutil.copy2(pdf_path, original_copy)
    result["output_files"].append(original_copy)

    print(f"  [1/3] Splitting PDF...")
    try:
        split_result = split_pdf(pdf_path, output_dir=file_output_dir, split_invoices=True)

        if split_result.get("status") != _STATUS_SUCCESS:
            result["errors"].append(f"Split failed: {split_result.get('error')}")
            result["status"] = _STATUS_SPLIT_FAILED
            return result

        pages_info = split_result.get("pages", [])
        decl_pages = sum(1 for p in pages_info if p["doc_type"] == _SPLIT_DOC_DECLARATION)
        inv_pages  = sum(1 for p in pages_info if p["doc_type"] == _SPLIT_DOC_INVOICE)
        invoice_count = split_result.get("invoice_count", 1)
        print(f"       Split: {decl_pages} declaration, {inv_pages} invoice pages")
        print(f"       Detected {invoice_count} separate invoice(s)")

        declaration_metadata = split_result.get("declaration_metadata", {}) or {}
        bl_number = declaration_metadata.get("waybill")
        output_files = split_result.get("output_files", {})

        if bl_number and _SPLIT_DOC_DECLARATION in output_files:
            old_path = output_files[_SPLIT_DOC_DECLARATION]
            new_path = _rename_file(old_path, f"{bl_number}-Manifest{_EXT_PDF}")
            output_files[_SPLIT_DOC_DECLARATION] = new_path
            print(f"       Renamed declaration: {bl_number}-Manifest{_EXT_PDF}")

        if _SPLIT_DOC_DECLARATION in output_files and os.path.exists(output_files[_SPLIT_DOC_DECLARATION]):
            result["output_files"].append(output_files[_SPLIT_DOC_DECLARATION])

        for inv_path in split_result.get("invoices", []):
            if os.path.exists(inv_path):
                result["output_files"].append(inv_path)

    except Exception as e:
        result["errors"].append(f"Split exception: {str(e)}")
        result["status"] = _STATUS_SPLIT_FAILED
        return result

    invoices = split_result.get("invoices", [])
    declaration_path = output_files.get(_SPLIT_DOC_DECLARATION)

    if not invoices:
        legacy = output_files.get(_SPLIT_DOC_INVOICE)
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

            if inv_result.get("xlsx_path"):
                result["output_files"].append(inv_result["xlsx_path"])
            if inv_result.get("email_sent"):
                result["emails_sent"] += 1
            if inv_result.get("errors"):
                result["errors"].extend(inv_result["errors"])

        print(f"  [3/3] Completed: {result['emails_sent']}/{total_invoices} emails sent")

    result["status"] = _STATUS_COMPLETED_WITH_ERRORS if result["errors"] else _STATUS_SUCCESS
    print(f"\n  Status: {result['status']}")
    print(f"  Output files: {len(result['output_files'])}")
    print(f"  Emails sent: {result['emails_sent']}/{total_invoices}")

    return result


# ─── Helper functions ─────────────────────────────────────


def _extract_text(invoice_path: str) -> str:
    """Extract invoice text via the unified hybrid OCR pipeline.

    The old implementation ran pdfplumber on ``pages[:5]`` and then
    fell back to ``pdf_splitter.ocr_page`` page-by-page with a custom
    DPI. That split-brain has been replaced by a single call into
    ``multi_ocr.extract_text`` which runs the full hybrid matrix and
    consensus. The 5-page cap has been dropped — multi_ocr handles
    every page and the per-PDF-sha1 cache makes re-runs cheap.
    """
    try:
        import multi_ocr
    except ImportError:
        return ""

    try:
        result = multi_ocr.extract_text(invoice_path)
        return result.text or ""
    except Exception:
        return ""


def _run_pipeline(invoice_path: str, output_dir: str, base_dir: str,
                  document_type: Optional[str] = None) -> dict:
    """Run the core pipeline stages.

    When ``document_type`` is provided, it is injected into
    xlsx_generator.run via the thread-local shim so the rebuild path
    no longer silently defaults to 4000-000. The shim is applied on
    first call (idempotent).
    """
    from pipeline_runner import PipelineRunner

    invoice_path = os.path.abspath(invoice_path)
    output_dir = os.path.abspath(output_dir)
    base_dir = os.path.abspath(base_dir)

    safe_name = _safe_filename(os.path.basename(invoice_path))
    xlsx_path = os.path.join(output_dir, safe_name.replace(_EXT_PDF, _EXT_XLSX))

    config_path = os.path.join(base_dir, _PIPELINE_CONFIG_NAME)
    runner = PipelineRunner(config_path=config_path)
    runner.base_dir = Path(base_dir)

    _apply_xlsx_doc_type_shim()
    _doc_type_thread_local.value = document_type
    try:
        return runner.run(input_file=invoice_path, output_file=xlsx_path)
    finally:
        _doc_type_thread_local.value = None


def _extract_invoice_number(pipeline_result: dict, invoice_path: str, extracted_text: str) -> Optional[str]:
    """Extract invoice number from pipeline result, with fallbacks.
    Result is frozen after first call."""
    for stage in pipeline_result.get("stages", []):
        if stage.get("name") == _STAGE_EXTRACT:
            invoices = stage.get("invoices", [])
            if invoices:
                inv_num = invoices[0].get("invoice_number")
                if inv_num:
                    return inv_num

    inv_num = pipeline_result.get("extracted", {}).get("invoice_number")
    if inv_num:
        return inv_num

    work_dir = pipeline_result.get("work_dir", "")
    if work_dir:
        for fname in [_WORK_EXTRACTED_JSON]:
            fpath = os.path.join(work_dir, fname)
            if os.path.exists(fpath):
                try:
                    with open(fpath, "r") as f:
                        data = json.load(f)
                    inv_num = data.get("invoice_number")
                    if inv_num:
                        return inv_num
                except Exception:
                    pass

    if extracted_text:
        for pattern in _INVOICE_NUMBER_PATTERNS:
            match = re.search(pattern, extracted_text, re.IGNORECASE)
            if match:
                candidate = match.group(1).strip()
                if any(c.isdigit() for c in candidate):
                    return candidate

    return None


def _extract_invoice_total(pipeline_result: dict) -> Optional[float]:
    """Extract invoice total from pipeline result."""
    for stage in pipeline_result.get("stages", []):
        if stage.get("name") == _STAGE_EXTRACT:
            invoices = stage.get("invoices", [])
            if invoices:
                total = invoices[0].get("total")
                if total:
                    try:
                        return float(total)
                    except (ValueError, TypeError):
                        pass
    return None


def _extract_variance(pipeline_result: dict) -> Optional[float]:
    """Extract variance from pipeline result."""
    for stage in pipeline_result.get("stages", []):
        if stage.get("name") == _STAGE_GENERATE_XLSX:
            return stage.get("variance_check", 0)
    return None


def _get_format_name(pipeline_result: dict) -> str:
    """Get format name used by pipeline."""
    for stage in pipeline_result.get("stages", []):
        if stage.get("name") == _STAGE_EXTRACT:
            return stage.get("format_name", _STAGE_UNKNOWN)
    return _STAGE_UNKNOWN


def _find_xlsx(pipeline_result: dict, output_dir: str, invoice_name: str) -> Optional[str]:
    """Find generated XLSX from pipeline result."""
    actual = pipeline_result.get("output")
    if actual and os.path.exists(actual):
        return actual

    safe_name = _safe_filename(invoice_name)
    xlsx_path = os.path.join(output_dir, safe_name.replace(_EXT_PDF, _EXT_XLSX))
    if os.path.exists(xlsx_path):
        return xlsx_path

    for f in os.listdir(output_dir):
        if f.endswith(_EXT_XLSX):
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
    return "".join(c if c.isalnum() or c in "._-" else "_" for c in name)


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
    lines.append(extracted_text[:_EXTRACT_TEXT_TRUNCATE_CHARS] if extracted_text else _NO_TEXT_PLACEHOLDER)

    body = "\n".join(lines)

    try:
        msg = MIMEMultipart()
        msg["From"] = f"{cfg.email_sender_name} <{cfg.email_sender}>"
        msg["To"] = cfg.email_recipient
        msg["Subject"] = f"Failed Import: {filename}"
        msg.attach(MIMEText(body, _MIME_TEXT_SUBTYPE))

        if os.path.exists(invoice_path):
            with open(invoice_path, "rb") as f:
                part = MIMEBase(_MIME_BASE_MAIN_TYPE, _MIME_BASE_PDF_TYPE)
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header(_MIME_HEADER_DISPOSITION, f'attachment; filename="{filename}"')
                msg.attach(part)

        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(cfg.smtp_host, cfg.smtp_port, context=context) as server:
            server.login(cfg.email_sender, cfg.email_password)
            server.send_message(msg)

        logger.info(f"Sent failed import notification for {filename}")
        return True

    except Exception as e:
        logger.warning(f"Failed to send import notification for {filename}: {e}")
