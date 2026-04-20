#!/usr/bin/env python3
"""
PDF Splitter for Combined Invoice + Simplified Declaration Documents

Splits a combined PDF into separate documents based on content detection:
- Simplified Declaration (Grenada Customs Form)
- Commercial Invoice

Supports both text-based and scanned/image-based PDFs via OCR.

Usage:
  python pdf_splitter.py <input_pdf> [--output-dir <dir>]

Returns JSON with paths to split documents and extracted metadata.
"""

import json
import os
import re
import sys
import logging
import argparse
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
import tempfile

logger = logging.getLogger(__name__)

try:
    import pdfplumber
except ImportError:
    pdfplumber = None
    logger.warning("pdfplumber not installed - PDF splitting requires pdfplumber")

try:
    from PyPDF2 import PdfReader, PdfWriter
except ImportError:
    try:
        from pypdf import PdfReader, PdfWriter  # PyPDF2 v3+ renamed
    except ImportError:
        PdfReader = None
        PdfWriter = None
        logger.warning("PyPDF2/pypdf not installed - PDF splitting requires PyPDF2")

# OCR support - use PyMuPDF for rendering (same as pdf_extractor.py)
try:
    import fitz  # PyMuPDF
    PYMUPDF_AVAILABLE = True
except ImportError:
    fitz = None
    PYMUPDF_AVAILABLE = False
    logger.debug("PyMuPDF not installed - OCR for scanned PDFs unavailable")

try:
    import pytesseract
    from PIL import Image
    import io
    TESSERACT_AVAILABLE = True

    # Configure tesseract path for Windows
    import platform
    if platform.system() == 'Windows':
        tesseract_paths = [
            r'C:\Program Files\Tesseract-OCR\tesseract.exe',
            r'C:\Program Files (x86)\Tesseract-OCR\tesseract.exe',
        ]
        for tpath in tesseract_paths:
            if os.path.exists(tpath):
                pytesseract.pytesseract.tesseract_cmd = tpath
                break
    else:
        # WSL - try Windows path
        wsl_tesseract = '/mnt/c/Program Files/Tesseract-OCR/tesseract.exe'
        if os.path.exists(wsl_tesseract):
            pytesseract.pytesseract.tesseract_cmd = wsl_tesseract

    # Check if tesseract binary is available
    try:
        pytesseract.get_tesseract_version()
        TESSERACT_BINARY_AVAILABLE = True
    except:
        TESSERACT_BINARY_AVAILABLE = False
        logger.debug("Tesseract binary not found in PATH")
except ImportError:
    TESSERACT_AVAILABLE = False
    TESSERACT_BINARY_AVAILABLE = False
    logger.debug("pytesseract not installed - OCR for scanned PDFs unavailable")

# `OCR_AVAILABLE` now gates whether the hybrid `multi_ocr` pipeline can do
# any work at all. The only hard dependency is PyMuPDF (for rendering pages
# to images); individual engines inside multi_ocr (tesseract / paddleocr /
# vision_api) are optional and degrade gracefully when their backing
# package is missing. The legacy `TESSERACT_*_AVAILABLE` flags above are
# kept because multi_ocr's tesseract engine benefits from the
# `pytesseract.tesseract_cmd` path configuration performed during import.
OCR_AVAILABLE = PYMUPDF_AVAILABLE


@dataclass
class DocumentPage:
    """Represents a page with its detected document type."""
    page_num: int  # 0-indexed
    doc_type: str  # 'declaration', 'invoice', 'unknown'
    confidence: float
    keywords_found: List[str]


class DocumentType:
    DECLARATION = 'declaration'
    INVOICE = 'invoice'
    CURRENCY_CONVERSION = 'currency_conversion'
    UNKNOWN = 'unknown'


# Keywords for detecting xe.com / Google currency conversion screenshots.
# These pages are used as proof of conversion rate/date for customs filings
# and must be kept as attachments, NOT treated as invoices.
CURRENCY_CONVERSION_KEYWORDS = [
    'XE CURRENCY CONVERTER',
    'XE.COM',
    'CURRENCY CONVERTER',
    'MID-MARKET RATE',
    'CONVERT CHINESE YUAN',
    'CHINESE YUAN RENMINBI',
    'CURRENCYCONVERTER',
    'EXCHANGE RATE',
]


# Keywords for detecting Simplified Declaration
DECLARATION_KEYWORDS = [
    'SIMPLIFIED DECLARATION',
    'GRENADA SIMPLIFIED',
    'CUSTOMS FILE NO',
    'WAYBILL',
    'PACKAGES',
    'GROSS WEIGHT',
    'CONSIGNEE',
    'COUNTRY OF ORIGIN',
    'COMMERCIAL DESCRIPTION',
    'TOTAL FOB VALUE',
    'CUSTOMS AGENT',
    'C.I.F. VALUE',
]

# Keywords for detecting Commercial Invoice
INVOICE_KEYWORDS = [
    'INVOICE',
    'COMMERCIAL INVOICE',
    'PROFORMA',
    'BILL TO',
    'SHIP TO',
    'SOLD TO',
    'ORDER DATE',
    'INVOICE DATE',
    'ITEM NO',
    'QUANTITY',
    'UNIT PRICE',
    'TOTAL',
    'SUBTOTAL',
    'GRAND TOTAL',
    'PURCHASE ORDER',
    'P.O.',
]


def detect_document_type(page_text: str) -> Tuple[str, float, List[str]]:
    """
    Detect document type from page text.

    Returns:
        Tuple of (doc_type, confidence, keywords_found)
    """
    text_upper = page_text.upper()

    # Currency conversion pages (xe.com screenshots) take precedence — they often
    # contain invoice-like text mixed in (headers from the browser tab, etc.) and
    # would be misclassified as invoices otherwise.
    currency_matches = [kw for kw in CURRENCY_CONVERSION_KEYWORDS if kw in text_upper]
    if len(currency_matches) >= 2:
        # Two or more currency-specific markers → high-confidence currency page
        return DocumentType.CURRENCY_CONVERSION, min(len(currency_matches) / 4.0, 1.0), currency_matches

    # Count keyword matches
    declaration_matches = []
    invoice_matches = []

    for kw in DECLARATION_KEYWORDS:
        if kw in text_upper:
            declaration_matches.append(kw)

    for kw in INVOICE_KEYWORDS:
        if kw in text_upper:
            invoice_matches.append(kw)

    # Strong indicators
    has_simplified_declaration = 'SIMPLIFIED DECLARATION' in text_upper
    has_customs_file = 'CUSTOMS FILE' in text_upper
    has_waybill = 'WAYBILL' in text_upper

    has_invoice_header = 'INVOICE' in text_upper and 'SIMPLIFIED' not in text_upper
    has_item_list = any(kw in text_upper for kw in ['QUANTITY', 'UNIT PRICE', 'QTY', 'PRICE'])

    # Calculate scores
    declaration_score = len(declaration_matches) / len(DECLARATION_KEYWORDS)
    invoice_score = len(invoice_matches) / len(INVOICE_KEYWORDS)

    # Boost for strong indicators
    if has_simplified_declaration:
        declaration_score += 0.3
    if has_customs_file:
        declaration_score += 0.2
    if has_waybill:
        declaration_score += 0.1

    if has_invoice_header:
        invoice_score += 0.2
    if has_item_list:
        invoice_score += 0.2

    # Determine type
    if declaration_score > invoice_score and declaration_score > 0.15:
        return DocumentType.DECLARATION, min(declaration_score, 1.0), declaration_matches
    elif invoice_score > 0.15:
        return DocumentType.INVOICE, min(invoice_score, 1.0), invoice_matches
    else:
        return DocumentType.UNKNOWN, 0.0, []


def to_windows_path(linux_path: str) -> str:
    """Convert WSL Linux path to Windows path for tesseract."""
    if linux_path.startswith('/mnt/c/'):
        return 'C:' + linux_path[6:].replace('/', '\\')
    elif linux_path.startswith('/mnt/d/'):
        return 'D:' + linux_path[6:].replace('/', '\\')
    return linux_path


def ocr_page(pdf_path: str, page_num: int, dpi: int = 300,
             preprocess: bool = True, psm: int = 6,
             preprocess_mode: str = 'default') -> str:
    """Extract text from a single PDF page via the unified hybrid OCR pipeline.

    Uses a composite cache key ``sha1(parent_pdf) + ':page:N'`` so that
    per-page OCR results persist across runs even though the temp single-
    page PDF is deleted after each call.  This avoids re-running the full
    12-variant OCR matrix (~50 s/page) on every pipeline invocation.

    The ``dpi``, ``preprocess``, ``psm``, and ``preprocess_mode`` arguments
    are accepted for backward compatibility but are now ignored — the full
    quality matrix inside ``multi_ocr`` supersedes the old single-path
    Tesseract-with-one-preprocess approach.

    Args:
        pdf_path: Path to PDF file
        page_num: 0-indexed page number

    Returns:
        Extracted text string (empty on failure / missing deps)
    """
    if not PYMUPDF_AVAILABLE:
        return ""

    try:
        import multi_ocr  # noqa: WPS433 (lazy to avoid startup cost)
    except ImportError:
        logger.warning("multi_ocr module unavailable; ocr_page returning empty")
        return ""

    # Composite cache key: parent PDF SHA-1 + page number.
    # This survives temp-file deletion and avoids re-OCR on re-runs.
    import hashlib
    parent_sha1 = multi_ocr._pdf_sha1(pdf_path)
    page_cache_key = hashlib.sha1(
        f"{parent_sha1}:page:{page_num}".encode()
    ).hexdigest()

    cached = multi_ocr.cache_load(page_cache_key)
    if cached is not None:
        logger.info(f"Page {page_num + 1} OCR cache hit (parent {parent_sha1[:8]})")
        return cached.text or ""

    tmp_path: Optional[str] = None
    try:
        # Extract the single page into a tempfile so the hybrid pipeline can
        # OCR it in isolation.
        doc = fitz.open(pdf_path)
        try:
            if page_num < 0 or page_num >= doc.page_count:
                return ""
            single = fitz.open()
            single.insert_pdf(doc, from_page=page_num, to_page=page_num)
        finally:
            doc.close()

        fd, tmp_path = tempfile.mkstemp(
            prefix=f"ocr_page_{page_num}_", suffix=".pdf"
        )
        os.close(fd)
        single.save(tmp_path)
        single.close()

        # Run OCR without its own cache (we manage the composite key above)
        result = multi_ocr.extract_text(tmp_path, use_cache=False)

        # Save under the composite parent+page key
        multi_ocr.cache_save(page_cache_key, result)

        return result.text or ""
    except Exception as e:
        logger.warning(f"OCR failed for page {page_num + 1}: {e}")
        return ""
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass


def apply_position_heuristic(pages: List[DocumentPage], total_pages: int) -> List[DocumentPage]:
    """
    Apply position-based heuristic for scanned PDFs when OCR isn't available.

    Heuristic based on typical WebSource document structure:
    - First pages are usually Invoice
    - Last page is usually Declaration (Simplified Declaration form)

    For 4-page documents (common): pages 1-3 = invoice, page 4 = declaration
    For 3-page documents: pages 1-2 = invoice, page 3 = declaration
    For 2-page documents: page 1 = invoice, page 2 = declaration
    For 1-page documents: page 1 = invoice (no declaration)
    """
    if total_pages == 0:
        return pages

    result = []
    last_page_idx = total_pages - 1

    for i, page in enumerate(pages):
        if page.doc_type != DocumentType.UNKNOWN:
            # Already classified, keep it
            result.append(page)
        else:
            # Apply heuristic: last page is declaration, rest are invoice
            if i == last_page_idx and total_pages > 1:
                new_page = DocumentPage(
                    page_num=page.page_num,
                    doc_type=DocumentType.DECLARATION,
                    confidence=0.5,  # Medium confidence (heuristic)
                    keywords_found=['(position heuristic)']
                )
            else:
                new_page = DocumentPage(
                    page_num=page.page_num,
                    doc_type=DocumentType.INVOICE,
                    confidence=0.5,  # Medium confidence (heuristic)
                    keywords_found=['(position heuristic)']
                )
            result.append(new_page)

    return result


def analyze_pdf(pdf_path: str, use_ocr: bool = True, use_heuristic: bool = True) -> Tuple[List[DocumentPage], bool, bool, List[str]]:
    """
    Analyze a PDF and classify each page by document type.

    Args:
        pdf_path: Path to input PDF
        use_ocr: Whether to use OCR for scanned/image PDFs
        use_heuristic: Whether to fall back to position heuristic if OCR unavailable

    Returns:
        Tuple of (List of DocumentPage objects, whether OCR was used, whether heuristic was used, page texts)
    """
    if not pdfplumber:
        raise RuntimeError("pdfplumber is required for PDF analysis")

    pages = []
    used_ocr = False
    used_heuristic = False

    # OCR cache: store page_texts in a JSON sidecar next to the source PDF,
    # keyed by file size to detect changes. Avoids re-OCRing on every run.
    import json as _json
    cache_path = pdf_path + '.pages.json'
    cached_texts = None
    try:
        if os.path.exists(cache_path):
            with open(cache_path, 'r', encoding='utf-8') as _cf:
                _cache = _json.load(_cf)
            if _cache.get('size') == os.path.getsize(pdf_path):
                cached_texts = _cache.get('page_texts')
    except Exception:
        cached_texts = None

    with pdfplumber.open(pdf_path) as pdf:
        # First pass: try normal text extraction
        all_pages_empty = True
        page_texts = []

        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            page_texts.append(text)
            if len(text.strip()) > 50:  # Consider page has text if > 50 chars
                all_pages_empty = False

        # If all pages are empty, try OCR (use cached OCR if available)
        if all_pages_empty and use_ocr and OCR_AVAILABLE:
            if cached_texts and len(cached_texts) == len(pdf.pages):
                logger.info(f"Using cached OCR texts from {os.path.basename(cache_path)}")
                page_texts = list(cached_texts)
                used_ocr = True
            else:
                logger.info("No text found in PDF, attempting OCR...")
                used_ocr = True
                ocr_success = False
                for i in range(len(pdf.pages)):
                    ocr_text = ocr_page(pdf_path, i)
                    if ocr_text and len(ocr_text.strip()) > 50:
                        page_texts[i] = ocr_text
                        ocr_success = True
                        logger.debug(f"Page {i+1} OCR: {len(ocr_text)} chars extracted")

                # If OCR also failed, note it
                if not ocr_success:
                    logger.warning("OCR ran but extracted no useful text")
                    all_pages_empty = True  # Still empty after OCR
                else:
                    # Save OCR results to cache for next run
                    try:
                        with open(cache_path, 'w', encoding='utf-8') as _cf:
                            _json.dump({
                                'size': os.path.getsize(pdf_path),
                                'page_texts': page_texts,
                            }, _cf)
                    except Exception as _e:
                        logger.debug(f"Failed to save OCR cache: {_e}")

        elif all_pages_empty and use_ocr and not OCR_AVAILABLE:
            logger.warning(
                "PDF appears to be scanned/image-based but OCR is not available. "
                "Install Tesseract-OCR and Poppler, then: pip install pytesseract pdf2image"
            )

        # Analyze each page
        for i, text in enumerate(page_texts):
            doc_type, confidence, keywords = detect_document_type(text)

            pages.append(DocumentPage(
                page_num=i,
                doc_type=doc_type,
                confidence=confidence,
                keywords_found=keywords
            ))

            logger.debug(f"Page {i+1}: {doc_type} (confidence: {confidence:.2f})")

        # If all pages are still unknown and heuristic is enabled, apply position heuristic
        all_unknown = all(p.doc_type == DocumentType.UNKNOWN for p in pages)
        if all_unknown and use_heuristic and len(pages) > 0:
            logger.info("All pages unknown - applying position-based heuristic")
            pages = apply_position_heuristic(pages, len(pdf.pages))
            used_heuristic = True
        elif use_heuristic and len(pages) > 1:
            # If some pages are classified but some remain unknown,
            # apply contextual heuristic: if declaration found, unknown pages are likely invoice
            has_declaration = any(p.doc_type == DocumentType.DECLARATION for p in pages)
            has_unknown = any(p.doc_type == DocumentType.UNKNOWN for p in pages)

            if has_declaration and has_unknown:
                logger.info("Declaration found, classifying unknown pages as invoice")
                for i, page in enumerate(pages):
                    if page.doc_type == DocumentType.UNKNOWN:
                        pages[i] = DocumentPage(
                            page_num=page.page_num,
                            doc_type=DocumentType.INVOICE,
                            confidence=0.4,  # Lower confidence (contextual heuristic)
                            keywords_found=['(contextual: non-declaration page)']
                        )
                used_heuristic = True

    return pages, used_ocr, used_heuristic, page_texts


def split_pdf(
    pdf_path: str,
    pages: List[DocumentPage],
    output_dir: str
) -> Dict[str, str]:
    """
    Split PDF into separate documents by type.

    Args:
        pdf_path: Path to input PDF
        pages: List of DocumentPage objects from analyze_pdf
        output_dir: Directory to write output PDFs

    Returns:
        Dict mapping document type to output path
    """
    if not PdfReader or not PdfWriter:
        raise RuntimeError("PyPDF2 is required for PDF splitting")

    os.makedirs(output_dir, exist_ok=True)

    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    reader = PdfReader(pdf_path)

    # Group pages by document type
    type_pages: Dict[str, List[int]] = {
        DocumentType.DECLARATION: [],
        DocumentType.INVOICE: [],
        DocumentType.CURRENCY_CONVERSION: [],
        DocumentType.UNKNOWN: [],
    }

    for page in pages:
        type_pages[page.doc_type].append(page.page_num)

    output_paths = {}

    # Write each document type to a separate PDF
    for doc_type, page_nums in type_pages.items():
        if not page_nums:
            continue

        writer = PdfWriter()
        for page_num in sorted(page_nums):
            writer.add_page(reader.pages[page_num])

        # Generate output filename
        suffix = {
            DocumentType.DECLARATION: '_Declaration',
            DocumentType.INVOICE: '_Invoice',
            DocumentType.CURRENCY_CONVERSION: '_CurrencyConversion',
            DocumentType.UNKNOWN: '_Unknown',
        }[doc_type]

        output_path = os.path.join(output_dir, f"{base_name}{suffix}.pdf")

        with open(output_path, 'wb') as f:
            writer.write(f)

        output_paths[doc_type] = output_path
        logger.info(f"Wrote {doc_type}: {output_path} ({len(page_nums)} pages)")

    return output_paths


def detect_invoice_boundaries(pdf_path: str, invoice_page_nums: List[int], page_texts: List[str] = None):
    """
    Detect boundaries between different invoices in the invoice pages.

    Looks for invoice number patterns to determine where one invoice ends
    and another begins.

    Args:
        pdf_path: Path to the original PDF
        invoice_page_nums: List of page numbers (0-indexed) that are invoices
        page_texts: Pre-extracted page texts (from analyze_pdf) to avoid re-OCR

    Returns:
        Tuple of (invoice_groups, invoice_ids) where:
        - invoice_groups: List of page number lists, each representing a separate invoice
        - invoice_ids: Parallel list of detected invoice/order IDs (str or None)
    """
    import re

    if not invoice_page_nums:
        return [], []

    if len(invoice_page_nums) == 1:
        return [invoice_page_nums], [None]

    # Extract text from each invoice page and detect invoice numbers
    page_invoice_ids = []

    for page_num in sorted(invoice_page_nums):
        text = ""
        # Use pre-extracted text if available
        if page_texts and page_num < len(page_texts):
            text = page_texts[page_num] or ""

        if len(text.strip()) < 50:
            try:
                if pdfplumber:
                    with pdfplumber.open(pdf_path) as pdf:
                        if page_num < len(pdf.pages):
                            text = pdf.pages[page_num].extract_text() or ""

                # If no text, try OCR
                if len(text.strip()) < 50 and OCR_AVAILABLE:
                    text = ocr_page(pdf_path, page_num) or ""
            except Exception as e:
                logger.debug(f"Failed to extract text from page {page_num}: {e}")

        # Look for invoice/order number patterns
        invoice_id = None

        # Skip pattern matching on pages with very little text — OCR noise
        # on scanned pages produces false positives (e.g. "POWERFUL" → PO + "WERFUL")
        if len(text.strip()) < 50:
            page_invoice_ids.append((page_num, None))
            continue

        text_upper = text.upper()

        patterns = [
            # SHEIN invoice: INVUS20240728002530332 (OCR may insert spaces in digits)
            (r'(INVUS[\s\d]{10,})', 1),
            # SHEIN/multi-column order: GSUNJG55T00QV70 (alphanumeric, 10+ chars)
            (r'ORDER\s*NUMBER[:\s].*\n\s*([A-Z0-9]{10,})', 1),
            # Amazon order format: 111-5908955-5240243 (OCR may add spaces around hyphens)
            (r'ORDER\s*#?\s*(\d{3}\s*-\s*\d{7}\s*-\s*\d{7})', 1),
            (r'(\d{3}\s*-\s*\d{7}\s*-\s*\d{7})', 1),
            # Temu/online: "Order ID: PO-211-12345..."
            (r'ORDER\s*ID[:\s]*(PO-\d{3}-\d+)', 1),
            # FashionNova / generic order number (same line, no newline crossing)
            (r'ORDER\s*(?:#|NUMBER)[: \t]*([A-Z0-9][A-Z0-9-]{5,20})', 1),
            # Generic invoice number (after "Invoice No:" label, OCR may add spaces)
            (r'INVOICE\s*(?:#|NO\.?|NUMBER)[:\s]*([A-Z0-9][\sA-Z0-9-]{5,25})', 1),
            # PO number (word boundary prevents matching words like "PORTABLE", "POWERFUL")
            (r'(?<![A-Z])(?:P\.?O\.?|PURCHASE\s*ORDER)(?![A-Za-z])\s*(?:#|NO\.?)?[:\s]*([A-Z0-9-]{5,20})', 1),
        ]

        for pattern, group in patterns:
            match = re.search(pattern, text_upper)
            if match:
                raw = match.group(group).strip()
                # Normalize: strip OCR-inserted spaces from IDs
                candidate = re.sub(r'\s+', '', raw)
                # Reject pure-alpha IDs — real invoice IDs contain digits
                # (avoids false positives like "SELLER", "RTABLE" from OCR noise)
                if not re.search(r'\d', candidate):
                    continue
                invoice_id = candidate
                # Fix OCR-duplicated trailing digits: INVUS20240728002522189189
                # → INVUS20240728002522189 (last N digits repeated by OCR noise)
                for suffix_len in range(3, 8):
                    if len(invoice_id) > suffix_len * 2:
                        tail = invoice_id[-suffix_len:]
                        if invoice_id[-suffix_len * 2:-suffix_len] == tail:
                            invoice_id = invoice_id[:-suffix_len]
                            break
                break

        page_invoice_ids.append((page_num, invoice_id))

    # Group pages by invoice ID
    invoice_groups = []
    group_ids = []
    current_group = []
    current_id = None

    for page_num, invoice_id in page_invoice_ids:
        if invoice_id and invoice_id != current_id:
            # New invoice detected
            if current_group:
                invoice_groups.append(current_group)
                group_ids.append(current_id)
            current_group = [page_num]
            current_id = invoice_id
        else:
            # Same invoice or no ID detected - add to current group
            current_group.append(page_num)

    # Don't forget the last group
    if current_group:
        invoice_groups.append(current_group)
        group_ids.append(current_id)

    logger.info(f"Detected {len(invoice_groups)} separate invoice(s) from {len(invoice_page_nums)} pages")

    return invoice_groups, group_ids


def extract_conversion_rate(text: str) -> Optional[Dict]:
    """
    Extract currency conversion rate from xe.com / currency-converter page text.

    Tolerant of OCR noise (e.g. "CNY" → "ONY", "USD" → "USO", "=" → "-").

    Returns dict with keys:
        source_currency, target_currency, rate, date (optional), raw_line
    or None if no rate could be parsed.
    """
    import re
    from datetime import datetime

    if not text:
        return None

    text_upper = text.upper()

    # Currency tokens (including common OCR garbles)
    CNY_TOKENS = r'(?:CNY|ONY|CINY|CTNY|CN¥|¥)'
    USD_TOKENS = r'(?:USD|USO|USB|US\$|\$)'
    EUR_TOKENS = r'(?:EUR|EURO)'
    GBP_TOKENS = r'(?:GBP|£)'

    currency_pairs = [
        ('CNY', 'USD', CNY_TOKENS, USD_TOKENS),
        ('EUR', 'USD', EUR_TOKENS, USD_TOKENS),
        ('GBP', 'USD', GBP_TOKENS, USD_TOKENS),
        ('USD', 'CNY', USD_TOKENS, CNY_TOKENS),
    ]

    result = None
    for src, tgt, src_tok, tgt_tok in currency_pairs:
        # Pattern: "1 CNY = 0.140875 USD" (OCR may replace = with -, 1 with T/I)
        pattern = rf'[1TIl]\s*{src_tok}\s*[-=:~]\s*([0-9]+\.[0-9]{{3,}})\s*{tgt_tok}'
        m = re.search(pattern, text_upper)
        if m:
            try:
                rate = float(m.group(1))
                if 0 < rate < 100000:  # sanity check
                    result = {
                        'source_currency': src,
                        'target_currency': tgt,
                        'rate': rate,
                        'raw_line': m.group(0),
                    }
                    break
            except ValueError:
                continue

    # Fallback: amount-to-amount form. xe.com shows two labelled amounts on
    # adjacent lines: "X.XX Chinese Yuan Renminbi\nY.YY US Dollars". OCR is
    # noisy so we collect all matching pairs and take the median rate.
    if not result:
        # Normalize common OCR garbles on the currency name words
        t = text_upper
        t = re.sub(r'CHINOSE|CHINESE|SHINEWS|CHIRGES|CHINEWS', 'CHINESE', t)
        t = re.sub(r'USDOLLARS?', 'US DOLLARS', t)

        # Find "<amount> CHINESE YUAN" → "<amount> US DOLLARS" pairs within 3 lines
        rates = []
        for m in re.finditer(r'([\d.,]+)\s+CHINESE\s*YUAN[^\n]*\n(?:[^\n]*\n){0,3}?[^\n]*?([\d.,]+)[^\n]*?US\s*DOLLAR', t):
            src_raw = m.group(1).replace(',', '')
            tgt_raw = m.group(2).replace(',', '')
            try:
                src_amt = float(src_raw)
                tgt_amt = float(tgt_raw)
                if src_amt > 0 and tgt_amt > 0:
                    candidate = tgt_amt / src_amt
                    # Sanity: CNY→USD should be roughly 0.10-0.20
                    if 0.05 < candidate < 0.50:
                        rates.append((candidate, m.group(0)[:120]))
            except ValueError:
                continue

        if rates:
            # Pick the median rate
            rates.sort(key=lambda r: r[0])
            median_rate, raw = rates[len(rates) // 2]
            result = {
                'source_currency': 'CNY',
                'target_currency': 'USD',
                'rate': round(median_rate, 8),
                'raw_line': raw,
                'samples': len(rates),
            }

    if not result:
        return None

    # Try to extract date (xe.com shows "Mar 15, 2025, 13:52 UTC" style timestamps)
    date_patterns = [
        r'([A-Z][a-z]{2}\s+\d{1,2},?\s+\d{4}[^\n]*UTC?)',
        r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2})',
        r'([A-Z][a-z]{2}\s+\d{1,2},?\s+\d{4})',
    ]
    for pat in date_patterns:
        m = re.search(pat, text)
        if m:
            result['date'] = m.group(1).strip()
            break

    # Always stamp when we extracted the rate (fallback provenance)
    result['extracted_at'] = datetime.now().isoformat(timespec='seconds')
    return result


def split_pdf_multi_invoice(
    pdf_path: str,
    pages: List[DocumentPage],
    output_dir: str,
    page_texts: List[str] = None
) -> Dict[str, any]:
    """
    Split PDF into separate documents, with support for multiple invoices
    AND multiple declarations.

    Args:
        pdf_path: Path to input PDF
        pages: List of DocumentPage objects from analyze_pdf
        output_dir: Directory to write output PDFs
        page_texts: Pre-extracted page texts (from analyze_pdf) to avoid re-OCR

    Returns:
        Dict with:
            - declarations: list of paths to declaration PDFs (may be multiple)
            - invoices: list of paths to invoice PDFs (may be multiple)
            - declaration: path to first declaration (for backwards compatibility)
    """
    if not PdfReader or not PdfWriter:
        raise RuntimeError("PyPDF2 is required for PDF splitting")

    os.makedirs(output_dir, exist_ok=True)

    base_name = os.path.splitext(os.path.basename(pdf_path))[0]
    reader = PdfReader(pdf_path)

    # Collect page numbers by type
    declaration_pages = [p.page_num for p in pages if p.doc_type == DocumentType.DECLARATION]
    invoice_pages = [p.page_num for p in pages if p.doc_type == DocumentType.INVOICE]
    currency_pages = [p.page_num for p in pages if p.doc_type == DocumentType.CURRENCY_CONVERSION]

    result = {
        'declaration': None,
        'declarations': [],
        'invoices': [],
        'currency_conversion': None,
        'currency_rate': None,
    }

    # Currency conversion pages — kept as proof of FX rate for customs filings
    if currency_pages:
        writer = PdfWriter()
        for page_num in sorted(currency_pages):
            writer.add_page(reader.pages[page_num])
        cc_path = os.path.join(output_dir, f"{base_name}_CurrencyConversion.pdf")
        with open(cc_path, 'wb') as f:
            writer.write(f)
        result['currency_conversion'] = cc_path
        logger.info(f"Wrote currency conversion: {cc_path} ({len(currency_pages)} pages)")

        # Extract conversion rate and save JSON sidecar
        if page_texts:
            combined_text = "\n".join(
                page_texts[pn] for pn in sorted(currency_pages)
                if pn < len(page_texts) and page_texts[pn]
            )
            rate_info = extract_conversion_rate(combined_text)
            if rate_info:
                import json as _json
                json_path = os.path.join(output_dir, f"{base_name}_currency_conversion.json")
                try:
                    with open(json_path, 'w', encoding='utf-8') as jf:
                        _json.dump(rate_info, jf, indent=2)
                    result['currency_rate'] = rate_info
                    logger.info(
                        f"Extracted conversion rate: 1 {rate_info['source_currency']} = "
                        f"{rate_info['rate']} {rate_info['target_currency']}"
                    )
                except Exception as e:
                    logger.warning(f"Failed to write currency conversion JSON: {e}")

    # Each declaration page is a SEPARATE declaration (one per page)
    if declaration_pages:
        for i, page_num in enumerate(sorted(declaration_pages)):
            writer = PdfWriter()
            writer.add_page(reader.pages[page_num])

            # Name: _Declaration.pdf for single, _Declaration_1.pdf etc for multiple
            if len(declaration_pages) == 1:
                output_path = os.path.join(output_dir, f"{base_name}_Declaration.pdf")
            else:
                output_path = os.path.join(output_dir, f"{base_name}_Declaration_{i+1}.pdf")

            with open(output_path, 'wb') as f:
                writer.write(f)

            result['declarations'].append(output_path)
            logger.info(f"Wrote declaration {i+1}: {output_path}")

        # Backwards compatibility - first declaration
        if result['declarations']:
            result['declaration'] = result['declarations'][0]

    # Split invoice pages into separate PDFs if they contain different invoices
    if invoice_pages:
        invoice_groups = []
        invoice_ids = []
        if len(invoice_pages) > 1:
            invoice_groups, invoice_ids = detect_invoice_boundaries(pdf_path, sorted(invoice_pages), page_texts=page_texts)

            # Fallback: if boundary detection found only one group, check if each
            # page has its own "Grand Total" — if so, each page is a separate
            # sub-invoice (common in TEMU/SHEIN-style combined invoices).
            if len(invoice_groups) <= 1 and page_texts:
                import re as _re
                _grand_total_pages = []
                for pn in sorted(invoice_pages):
                    if pn < len(page_texts) and page_texts[pn]:
                        if _re.search(r'Grand\s+Total', page_texts[pn], _re.IGNORECASE):
                            _grand_total_pages.append(pn)
                if len(_grand_total_pages) > 1:
                    # Each page with Grand Total becomes its own invoice group
                    invoice_groups = [[pn] for pn in _grand_total_pages]
                    invoice_ids = [None] * len(invoice_groups)
                    logger.info(f"Per-page split: {len(invoice_groups)} sub-invoices "
                                f"detected (each page has Grand Total)")

        if len(invoice_groups) > 1:
            # For per-page splits of image-based PDFs, the page_texts from
            # analyze_pdf may be garbled (single-page OCR is much worse than
            # multi-page OCR for image-based PDFs).  Try full-document OCR on
            # a temporary combined invoice PDF to get better text, then split
            # by structural markers (e.g. "Grand Total").
            _enhanced_page_texts = None
            if page_texts:
                # Always try enhanced OCR for per-page splits of image-based
                # PDFs.  Single-page OCR is fundamentally worse than multi-page
                # OCR because OCR engines have less context.  Use the ORIGINAL
                # full PDF's multi_ocr cache (which benefits from multi-variant
                # consensus + low-confidence fallback) and split its text by
                # "Grand Total" boundaries to get per-invoice text.
                if True:  # Always try enhanced OCR for per-page splits
                    try:
                        import multi_ocr as _multi_ocr
                        _combo_result = _multi_ocr.extract_text(pdf_path)
                        _combo_text = _combo_result.text or ""
                        if _combo_text and len(re.findall(r'\d+\.\d{2}', _combo_text)) >= len(invoice_groups):
                            # Split by "Grand Total" boundaries — each sub-invoice
                            # ends with "Grand Total: <amount>".
                            _gt_pattern = re.compile(r'Grand\s+Total[:\s]*[\d,.]*', re.IGNORECASE)
                            _splits = list(_gt_pattern.finditer(_combo_text))
                            if len(_splits) >= len(invoice_groups):
                                _enhanced_page_texts = {}
                                for gi in range(len(invoice_groups)):
                                    _start = 0 if gi == 0 else _splits[gi - 1].end()
                                    _end = _splits[gi].end() if gi < len(_splits) else len(_combo_text)
                                    _chunk = _combo_text[_start:_end].strip()
                                    # Map to the original page numbers in this group
                                    for pn in invoice_groups[gi]:
                                        _enhanced_page_texts[pn] = _chunk
                                logger.info(
                                    f"Enhanced OCR: split {len(_combo_text)} chars "
                                    f"into {len(invoice_groups)} sub-invoices "
                                    f"from full-doc OCR [{_combo_result.engine_used}]"
                                )
                    except Exception as _e:
                        logger.debug(f"Enhanced OCR failed: {_e}")

            # Multiple distinct invoices detected — write each as separate PDF
            # Name files after their invoice/order ID when available
            for idx, group_pages in enumerate(invoice_groups):
                writer = PdfWriter()
                for page_num in group_pages:
                    writer.add_page(reader.pages[page_num])

                inv_id = invoice_ids[idx] if idx < len(invoice_ids) else None
                if inv_id:
                    # Sanitize ID for filesystem (remove chars invalid in filenames)
                    safe_id = re.sub(r'[<>:"/\\|?*]', '_', inv_id)
                    output_path = os.path.join(output_dir, f"{safe_id}.pdf")
                else:
                    output_path = os.path.join(output_dir, f"{base_name}_Invoice_{idx+1}.pdf")
                with open(output_path, 'wb') as f:
                    writer.write(f)

                result['invoices'].append(output_path)
                logger.info(f"Wrote invoice {idx+1}/{len(invoice_groups)}: {output_path} ({len(group_pages)} pages)")

                # Save OCR text sidecar to avoid re-OCR downstream
                # Prefer enhanced (full-doc OCR) text over garbled page_texts
                # Don't overwrite existing sidecar (may have been manually corrected)
                _src_texts = _enhanced_page_texts or ({pn: page_texts[pn] for pn in range(len(page_texts)) if page_texts[pn]} if page_texts else {})
                if _src_texts:
                    sidecar_text = "\n\n".join(_src_texts.get(pn, '') for pn in group_pages if _src_texts.get(pn))
                    if sidecar_text.strip():
                        txt_path = output_path.rsplit('.', 1)[0] + '.txt'
                        if not os.path.exists(txt_path):
                            with open(txt_path, 'w', encoding='utf-8') as tf:
                                tf.write(sidecar_text)
        else:
            # Single invoice (or boundaries not detected) — combine all pages
            writer = PdfWriter()
            for page_num in sorted(invoice_pages):
                writer.add_page(reader.pages[page_num])

            output_path = os.path.join(output_dir, f"{base_name}_Invoice.pdf")
            with open(output_path, 'wb') as f:
                writer.write(f)

            result['invoices'].append(output_path)
            logger.info(f"Wrote invoice: {output_path} ({len(invoice_pages)} pages)")

            # Save OCR text sidecar to avoid re-OCR downstream
            # Don't overwrite existing sidecar (may have been manually corrected)
            if page_texts:
                sidecar_text = "\n\n".join(page_texts[pn] for pn in sorted(invoice_pages) if pn < len(page_texts) and page_texts[pn])
                if sidecar_text.strip():
                    txt_path = output_path.rsplit('.', 1)[0] + '.txt'
                    if not os.path.exists(txt_path):
                        with open(txt_path, 'w', encoding='utf-8') as tf:
                            tf.write(sidecar_text)

    return result


class _OcrEmptyError(Exception):
    """Raised when OCR returns no text — signals that we should still try LLM vision."""


def extract_declaration_metadata(pdf_path: str) -> Dict[str, Optional[str]]:
    """
    Extract key metadata from a Simplified Declaration PDF.

    Uses OCR for scanned documents. Extracts:
        - waybill: Waybill/HAWB number
        - customs_file: Customs file number
        - consignee: Consignee name (without freight info)
        - freight: Freight value (written by client in consignee line)
        - packages: Number of packages
        - weight: Gross weight
        - country_origin: Country of origin
        - fob_value: FOB value
    """
    import re

    metadata = {
        'waybill': None,
        'customs_file': None,
        'man_reg': None,  # Man Reg Number (manifest registry)
        'consignee': None,
        'office': None,  # Customs Office code (e.g. GDWBS, GDSGO)
        'freight': None,
        'packages': None,
        'weight': None,
        'country_origin': None,
        'fob_value': None,
    }

    try:
        # Extract text from ALL pages (not just page 0)
        # CARICOM docs can be multi-page: BL + Shipper's Letter + CARICOM Invoice
        #
        # Optimisation: try the whole-document OCR cache first.  The per-page
        # fallback (12 variants × N pages) takes ~50 s/page on scanned PDFs.
        # If the whole-document result is already cached it contains text from
        # every page and is returned in <1 s.
        text = ""
        if OCR_AVAILABLE:
            try:
                import multi_ocr
                whole_doc = multi_ocr.extract_text(pdf_path)
                if whole_doc.text and len(whole_doc.text.strip()) >= 50:
                    text = whole_doc.text
                    logger.info(
                        f"Declaration: using whole-document OCR "
                        f"({len(text)} chars, engine={whole_doc.engine_used})"
                    )
            except Exception as e:
                logger.debug(f"Whole-document OCR failed, falling back to per-page: {e}")

        # Fallback: per-page extraction when whole-document OCR is unavailable
        if not text:
            all_texts = []
            num_pages = 0
            if pdfplumber:
                with pdfplumber.open(pdf_path) as pdf:
                    num_pages = len(pdf.pages)
                    for page in pdf.pages:
                        all_texts.append(page.extract_text() or "")

            # For pages with little/no text, try OCR
            if OCR_AVAILABLE:
                for i in range(num_pages):
                    if len(all_texts[i].strip()) < 50:
                        logger.info(f"Using OCR for declaration page {i+1}")
                        all_texts[i] = ocr_page(pdf_path, i)

            text = '\n'.join(all_texts)

        if not text:
            # OCR found nothing — skip regex extraction but still try LLM vision
            # below (it can read scanned forms that OCR cannot).
            raise _OcrEmptyError("No text extracted from declaration")

        lines = text.split('\n')
        text_upper = text.upper()

        # HAWB extraction: Search entire text first (handles two-column OCR layouts)
        # OCR may insert spaces and misread letters (e.g. S→9):
        #   "HAWBS665535" → OCR → "HAWB 9665535"
        # Capture HAWB + following alphanumeric on the SAME line ([ \t] not \s)
        hawb_match = re.search(r'(HAWB[ \tA-Z0-9-]+)', text_upper)
        if hawb_match:
            raw_hawb = hawb_match.group(1)
            # If match absorbed the integer part of a following decimal (e.g.
            # "HAWB 9590375 6.37" → captured "HAWB 9590375 6"), trim it.
            end_pos = hawb_match.end()
            if end_pos < len(text_upper) and text_upper[end_pos] == '.':
                raw_hawb = re.sub(r'[ \t]+\d+$', '', raw_hawb)
            raw_hawb = re.sub(r'[ \t]+', '', raw_hawb)  # strip OCR spaces
            metadata['waybill'] = raw_hawb

        for i, line in enumerate(lines):
            line_upper = line.upper().strip()
            line_clean = line.strip()

            # Waybill fallback: If no HAWB found, try line-by-line extraction
            if 'WAYBILL' in line_upper and not metadata['waybill']:
                # Generic alphanumeric after waybill (but not "NUMBER" itself)
                # Allow OCR-inserted spaces in the value
                match = re.search(r'WAYBILL[:\s]*(?:NUMBER[:\s]*)?([A-Z0-9][ \tA-Z0-9-]+)', line_upper)
                if match and match.group(1).strip() != 'NUMBER':
                    metadata['waybill'] = re.sub(r'[ \t]+', '', match.group(1))

            # Customs File Number
            if 'CUSTOMS' in line_upper and 'FILE' in line_upper and not metadata['customs_file']:
                match = re.search(r'CUSTOMS[^\d]*(\d+/\d+|\d+)', line_upper)
                if match:
                    metadata['customs_file'] = match.group(1)

            # Man Reg Number: "Man Reg Number: 2024/28" or "2024 / 30" -> "2024 28"
            # OCR may insert extra spaces around the separator: "2024 / 30"
            if ('MAN' in line_upper and 'REG' in line_upper) and not metadata['man_reg']:
                match = re.search(r'(\d{4})[/\s-]*(\d+)', line_upper)
                if match:
                    metadata['man_reg'] = f"{match.group(1)} {match.group(2)}"

            # Customs Office code: "Customs Office GDWBS" or "Customs Office: GDSGO"
            # Must come after the "CUSTOMS FILE" check because both lines start with
            # "CUSTOMS", but this one has "OFFICE" and captures the 4-6 letter code.
            if 'CUSTOMS' in line_upper and 'OFFICE' in line_upper and not metadata['office']:
                match = re.search(r'CUSTOMS\s+OFFICE[:\s]+([A-Z]{4,6})\b', line_upper)
                if match:
                    raw_office = match.group(1)
                    # OCR correction: common misreads for known CARICOM offices
                    # D↔O confusion is the most frequent OCR error in these codes
                    _OFFICE_OCR_CORRECTIONS = {
                        'GOWBS': 'GDWBS',  # Grenada: D misread as O
                        'GOSGO': 'GDSGO',  # Grenada: D misread as O
                    }
                    metadata['office'] = _OFFICE_OCR_CORRECTIONS.get(raw_office, raw_office)

            # Consignee - format: "Consignee: NAME ( FREIGHT X.XX US )"
            # The client writes freight in parentheses after name
            if 'CONSIGNEE' in line_upper and ':' in line_clean and not metadata['consignee']:
                consignee_part = line_clean.split(':', 1)[1].strip()

                # Skip junk values that aren't real consignee names
                junk_lower = consignee_part.lower()
                if any(junk in junk_lower for junk in (
                    'charges will be', 'billed to', 'to order',
                    'to the order of', 'same as above', 'not negotiable',
                )):
                    pass  # skip — not a real consignee name
                else:
                    # Extract freight from parentheses (client-written).
                    # OCR sometimes misreads the closing ")" as "}" or "]",
                    # so accept any bracket-like closer.
                    freight_match = re.search(
                        r'\(\s*FREIGHT\s+([\d.]+)\s*(?:US)?\s*[\)\}\]]',
                        consignee_part, re.IGNORECASE,
                    )
                    if freight_match:
                        metadata['freight'] = freight_match.group(1)
                        # Remove the whole "( FREIGHT ... )" section from the
                        # consignee name (tolerating OCR closer variants).
                        consignee_name = re.sub(
                            r'\s*\(.*?FREIGHT.*?[\)\}\]]',
                            '', consignee_part,
                            flags=re.IGNORECASE,
                        ).strip()
                        metadata['consignee'] = consignee_name
                    else:
                        # No freight info, just use the whole thing as consignee
                        metadata['consignee'] = consignee_part

            # Packages: "No and Type of package: 1 Package" or "1 PACKAGES"
            if ('PACKAGE' in line_upper or 'PKG' in line_upper) and not metadata['packages']:
                match = re.search(r'(\d+)\s*(?:PACKAGES?|PKGS?)', line_upper)
                if match:
                    metadata['packages'] = match.group(1)

            # Gross Weight/Mass: "Gross Mass: 2.0" or "2.5 KG"
            if ('GROSS' in line_upper and ('MASS' in line_upper or 'WEIGHT' in line_upper)) and not metadata['weight']:
                match = re.search(r'GROSS\s*(?:MASS|WEIGHT)[:\s]*([\d.]+)', line_upper)
                if match:
                    metadata['weight'] = match.group(1)
                else:
                    # Try pattern with units
                    match = re.search(r'([\d.]+)\s*(?:KG|KGS|LBS?)', line_upper)
                    if match:
                        metadata['weight'] = match.group(1)

            # Country of Origin: "COUNTRY OF ORIGIN OF GOODS\nUSA" or inline "COUNTRY OF ORIGIN: USA"
            if 'COUNTRY' in line_upper and 'ORIGIN' in line_upper and not metadata['country_origin']:
                match = re.search(r'COUNTRY\s+(?:OF\s+)?ORIGIN\s+(?:OF\s+GOODS\s*)?[:\s]*([A-Z]{2,})', line_upper)
                if match and match.group(1) not in ('OF', 'THE', 'AND', 'GOODS'):
                    metadata['country_origin'] = match.group(1).strip()
                else:
                    # Country might be on the next line or embedded in next line
                    for j in range(i + 1, min(i + 3, len(lines))):
                        next_line = lines[j].strip().upper()
                        if not next_line:
                            continue
                        # Direct country code on its own line
                        if len(next_line) <= 30 and next_line.isalpha():
                            metadata['country_origin'] = next_line
                            break
                        # Country code at end of line: "Tel: 1 473 439 1983 USA"
                        end_match = re.search(r'\b([A-Z]{2,3})\s*$', next_line)
                        if end_match and end_match.group(1) not in ('OF', 'THE', 'AND', 'TEL'):
                            metadata['country_origin'] = end_match.group(1)
                            break

            # FOB Value
            if ('FOB' in line_upper or 'F.O.B' in line_upper) and not metadata['fob_value']:
                match = re.search(r'(?:FOB|F\.O\.B)[:\s]*(?:USD|US\$|\$)?\s*([\d,]+(?:\.\d{2})?)', line_upper)
                if match:
                    metadata['fob_value'] = match.group(1).replace(',', '')

        # If packages still not found, try simpler pattern
        if not metadata['packages']:
            match = re.search(r'(\d+)\s*PACKAGE', text_upper)
            if match:
                metadata['packages'] = match.group(1)

        # ── BL-specific fallback patterns ──
        # CARICOM docs may contain BL pages with weight/packages in different format

        # Pieces: "165 PCS" or "165 PIECE(S)" — use as packages if not found
        if not metadata['packages']:
            match = re.search(r'(\d+)\s*(?:PCS|PIECE\(?S?\)?)', text_upper)
            if match:
                metadata['packages'] = match.group(1)

        # Total Pieces: "Total Pieces : 165 PCS"
        if not metadata['packages']:
            match = re.search(r'TOTAL\s*PIECES?\s*:?\s*(\d+)', text_upper)
            if match:
                metadata['packages'] = match.group(1)

        # Weight from BL TOTAL line: "TOTAL  900  408  56.634" (lbs kg cf cm)
        if not metadata['weight']:
            match = re.search(r'TOTAL\s+(\d+)\s+(\d+)\s+[\d.]+', text_upper)
            if match:
                # Second number is usually KG
                metadata['weight'] = match.group(2)

        # Weight from BL gross weight columns (lbs kg pattern)
        if not metadata['weight']:
            match = re.search(r'(\d{3,6})\s+(\d{2,6})\s+[\d.]+\s+[\d.]+', text_upper)
            if match:
                # Pattern: LBS KG CF CM — take kg (second number)
                lbs = int(match.group(1))
                kg = int(match.group(2))
                # Sanity: kg should be roughly lbs * 0.45
                if 0.3 < kg / max(lbs, 1) < 0.7:
                    metadata['weight'] = str(kg)

        # Consignee from BL or CARICOM: line after "CONSIGNEE" header
        if not metadata['consignee']:
            for i, line in enumerate(lines):
                lu = line.upper().strip()
                if 'CONSIGNEE' in lu and i + 1 < len(lines):
                    # Take the next non-empty line as consignee candidate
                    for j in range(i + 1, min(i + 4, len(lines))):
                        candidate = lines[j].strip()
                        cand_upper = candidate.upper()
                        # Skip junk, addresses, tel numbers, labels
                        if not candidate or len(candidate) < 3:
                            continue
                        if any(junk in cand_upper for junk in (
                            'CHARGES', 'BILLED', 'TO ORDER', 'NEGOTIABLE',
                            'NAME', 'ADDRESS', 'COUNTRY', 'TEL:', 'PHONE',
                            'C/O', 'TRUE BLUE', 'SPICE ISLAND', 'AVENUE',
                        )):
                            continue
                        if re.match(r'^[\d\s()+\-]+$', candidate):
                            continue  # phone number
                        metadata['consignee'] = candidate
                        break
                    if metadata['consignee']:
                        break

        # CARICOM Invoice total: "TOTAL INVOICE Amount $67,826.57"
        # OCR may garble "Amount" to "Ai t" etc, so use flexible pattern
        if not metadata['fob_value']:
            match = re.search(
                r'TOTAL\s+INVOICE\s+\S+\s*\$?([\d,]+(?:\.\d{2})?)',
                text_upper)
            if match:
                metadata['fob_value'] = match.group(1).replace(',', '')
        # Also try: "$67,826.57" near end of CARICOM page
        if not metadata['fob_value']:
            match = re.search(
                r'\$([\d,]+\.\d{2})\s*$',
                text, re.MULTILINE)
            if match:
                val = match.group(1).replace(',', '')
                try:
                    if float(val) > 100:  # sanity: must be > $100
                        metadata['fob_value'] = val
                except ValueError:
                    pass

    except Exception as e:
        logger.error(f"Failed to extract metadata: {e}")

    # ── LLM vision pass: detect handwritten/pencil annotations ──
    # Scanned declaration forms often have customs values, tariff codes, and
    # weights written in pencil that OCR misses entirely.  When the LLM API
    # is available, render the page and ask the model to read pencil marks.
    try:
        hw = extract_declaration_handwriting(pdf_path)
        if hw:
            handwritten = hw.get('handwritten', {})
            printed = hw.get('printed', {})
            # Always supplement OCR metadata with LLM vision-extracted printed
            # fields — the vision model reads scanned forms more reliably than
            # Tesseract OCR.  Only fill gaps, don't overwrite OCR results.
            if printed:
                if not metadata['consignee'] and printed.get('consignee'):
                    metadata['consignee'] = printed['consignee']
                if not metadata['freight'] and printed.get('freight'):
                    metadata['freight'] = str(printed['freight'])
                if not metadata['waybill'] and printed.get('waybill'):
                    metadata['waybill'] = printed['waybill']
                if not metadata['office'] and printed.get('office'):
                    metadata['office'] = printed['office']
                if not metadata['man_reg'] and printed.get('man_reg'):
                    metadata['man_reg'] = printed['man_reg']
                if not metadata['weight'] and printed.get('weight'):
                    metadata['weight'] = str(printed['weight'])
                if not metadata['packages'] and printed.get('packages'):
                    metadata['packages'] = str(printed['packages'])
                logger.info(f"LLM vision supplemented metadata for {os.path.basename(pdf_path)}: "
                            f"consignee={printed.get('consignee')}, waybill={printed.get('waybill')}")
            # Store handwritten data when detected
            if hw.get('has_handwriting') and handwritten:
                metadata['_handwritten'] = handwritten
                if _is_truthy_value(handwritten.get('customs_value_ec')):
                    metadata['_customs_value_ec'] = handwritten['customs_value_ec']
                if _is_truthy_value(handwritten.get('customs_value_usd')):
                    metadata['_customs_value_usd'] = handwritten['customs_value_usd']
                logger.info(f"LLM vision found handwriting on {os.path.basename(pdf_path)}: {handwritten}")
    except Exception as e:
        logger.debug(f"LLM vision extraction skipped: {e}")

    return metadata


def _enhance_declaration_image(img_bytes: bytes) -> bytes:
    """Enhance a scanned declaration image to make faint pencil marks more visible.

    Applies contrast enhancement and adaptive processing to boost faint
    handwritten pencil annotations that are nearly invisible in raw scans.
    Returns enhanced PNG bytes ready for LLM vision.
    """
    try:
        import cv2  # type: ignore
        import numpy as np  # type: ignore
    except ImportError:
        # Fallback: use PIL-only enhancement if cv2 unavailable
        try:
            from PIL import Image, ImageEnhance, ImageFilter  # type: ignore
            import io
            pil_img = Image.open(io.BytesIO(img_bytes))
            # Boost contrast 2x to make pencil marks darker
            pil_img = ImageEnhance.Contrast(pil_img).enhance(2.0)
            # Sharpen to crisp up faint strokes
            pil_img = pil_img.filter(ImageFilter.UnsharpMask(radius=2, percent=200, threshold=1))
            buf = io.BytesIO()
            pil_img.save(buf, format='PNG')
            return buf.getvalue()
        except Exception:
            return img_bytes

    # Decode PNG bytes to OpenCV array
    arr = np.frombuffer(img_bytes, dtype=np.uint8)
    img = cv2.imdecode(arr, cv2.IMREAD_COLOR)
    if img is None:
        return img_bytes

    # Convert to grayscale for processing
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

    # 1. Denoise to reduce scanner noise without blurring pencil strokes
    denoised = cv2.fastNlMeansDenoising(gray, h=8)

    # 2. CLAHE — Contrast Limited Adaptive Histogram Equalization
    #    Boosts local contrast so faint pencil in bright regions becomes visible.
    #    clipLimit=3.0 is more aggressive than standard (2.0) to catch very faint marks.
    clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(8, 8))
    enhanced = clahe.apply(denoised)

    # 3. Adaptive threshold to create a clean binary image highlighting all marks.
    #    We blend this with the CLAHE result rather than replacing it, so the LLM
    #    still sees grayscale variation (helpful for distinguishing print vs pencil).
    binary = cv2.adaptiveThreshold(
        enhanced, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
        cv2.THRESH_BINARY, 31, 15,
    )

    # 4. Blend: 60% CLAHE + 40% binary — preserves grayscale detail while
    #    boosting faint strokes that only show in the binary channel.
    blended = cv2.addWeighted(enhanced, 0.6, binary, 0.4, 0)

    # 5. Mild sharpening to crisp up pencil edges
    sharpen_kernel = np.array([
        [0, -0.5, 0],
        [-0.5, 3, -0.5],
        [0, -0.5, 0],
    ], dtype=np.float32)
    sharpened = cv2.filter2D(blended, -1, sharpen_kernel)

    # Encode back to PNG
    _, out_bytes = cv2.imencode('.png', sharpened)
    return out_bytes.tobytes()


def _is_truthy_value(val) -> bool:
    """Check if a value from LLM JSON is a meaningful non-empty string/number."""
    if val is None:
        return False
    s = str(val).strip()
    return bool(s) and s.lower() not in ('null', 'none', 'n/a', '')


def _vision_cache_path(pdf_path: str, base_dir: str) -> Optional[str]:
    """Return disk cache path for LLM vision results keyed on PDF content hash.

    Caching avoids repeated (slow, flaky) vision API calls on re-runs.
    """
    try:
        import hashlib
        with open(pdf_path, 'rb') as f:
            h = hashlib.sha1(f.read()).hexdigest()
        cache_dir = os.path.join(base_dir, 'data', 'vision_cache', h[:2])
        os.makedirs(cache_dir, exist_ok=True)
        return os.path.join(cache_dir, f'{h}.json')
    except Exception:
        return None


def extract_declaration_handwriting(pdf_path: str, base_dir: str = '.') -> Dict[str, Optional[str]]:
    """Extract handwritten/pencil annotations from a Simplified Declaration PDF using LLM vision.

    Scanned declaration forms often have customs values, tariff codes, and weights
    written in pencil by the client. OCR cannot reliably detect faint pencil marks,
    so this function renders the page at high DPI, applies contrast enhancement
    (CLAHE + adaptive threshold + sharpening), and sends the enhanced image to an
    LLM with vision to extract the handwritten data.

    Uses Z.AI's vision model (glm-4.6v) via the OpenAI-compatible endpoint
    (/api/paas/v4/chat/completions), NOT the Anthropic proxy which only supports
    text models.

    Results are cached on disk keyed by PDF content hash to avoid repeat API
    calls on re-runs. Timeouts trigger up to 2 retries before giving up.

    Returns dict with keys matching the JSON schema below.
    Empty dict if no LLM available or no handwriting detected.
    """
    if not PYMUPDF_AVAILABLE:
        return {}

    import base64

    # ── Disk cache check ────────────────────────────────────────
    cache_path = _vision_cache_path(pdf_path, base_dir)
    if cache_path and os.path.exists(cache_path):
        try:
            with open(cache_path, 'r') as f:
                cached = json.load(f)
            logger.info(f"LLM vision cache hit for {os.path.basename(pdf_path)}")
            return cached
        except Exception as e:
            logger.debug(f"Vision cache read failed: {e}")

    # Load LLM settings (we only need the API key — vision uses its own endpoint/model)
    try:
        sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
        from classifier import _load_llm_settings
        llm_settings = _load_llm_settings(base_dir)
        api_key = llm_settings.get('api_key')
    except Exception as e:
        logger.warning(f"Cannot load LLM settings for vision extraction: {e}")
        return {}

    if not api_key:
        logger.debug("No API key for LLM vision extraction")
        return {}

    # Vision model config — separate from text model
    vision_model = 'glm-4.6v'
    vision_endpoint = 'https://api.z.ai/api/coding/paas/v4/chat/completions'

    try:
        # Render declaration page as image at 300 DPI for better pencil visibility.
        # Previous 150 DPI was too low — faint pencil strokes need higher resolution
        # to be distinguishable from paper texture/noise.
        doc = fitz.open(pdf_path)
        page = doc[0]
        mat = fitz.Matrix(300 / 72, 300 / 72)
        pix = page.get_pixmap(matrix=mat)
        raw_img_bytes = pix.tobytes("png")
        doc.close()

        # Enhance image to boost faint pencil marks
        enhanced_bytes = _enhance_declaration_image(raw_img_bytes)
        logger.debug(
            f"Declaration image: raw={len(raw_img_bytes)//1024}KB, "
            f"enhanced={len(enhanced_bytes)//1024}KB"
        )

        img_b64 = base64.b64encode(enhanced_bytes).decode('utf-8')

        # Build vision prompt — more specific about where handwriting appears
        # on Grenada Simplified Declaration forms
        prompt = """Examine this scanned Grenada Simplified Declaration Form carefully.
The image has been contrast-enhanced to make faint pencil marks more visible.

Look for ANY handwritten or pencil annotations on the form. On these forms,
customs officers and clients commonly write values in these locations:

1. DUTY/TAX VALUES: Look in the "For Official Use Only" section at the bottom,
   AND near the consignee name area. Values may be written VERTICALLY along the
   left margin or near the consignee field. Look for numbers like "$125.36" or
   "58.83" or "EC$340.00" written in pencil/pen anywhere on the form.

2. TARIFF CODES: 8-digit codes (e.g. "63079090", "56081990") often handwritten
   in the "Particulars of declaration by Importer" table or in margins.

3. The "Particulars of declaration by Importer" table rows — check each column:
   Marks, numbers, HS Code, Description, Qty, Weight, Value, Duty Rate, Duty.

4. ANY pencil/pen marks in margins, between fields, or rotated/sideways text.

Also extract the printed form data:
- Consignee name (without the FREIGHT part)
- Freight amount (from the consignee line, e.g. "FREIGHT 11.00 US")
- WayBill Number (starts with HAWB)
- Man Reg Number
- Customs Office code (e.g. GDWBS, GDSGO)
- Gross Mass / Weight
- Number of packages

Return a JSON object with these fields:
{
  "handwritten": {
    "customs_value_ec": "numeric value in EC$ if visible, e.g. 340.00",
    "customs_value_usd": "numeric value in USD if visible, e.g. 125.36",
    "tariff_code": "tariff/HS code if visible, e.g. 63079090",
    "duty_rate": "duty rate percentage if visible, e.g. 20",
    "weight": "weight if handwritten",
    "description": "goods description if handwritten",
    "other_notes": "any other handwritten annotations including their location on the form"
  },
  "printed": {
    "consignee": "name without freight",
    "freight": "freight amount as number",
    "waybill": "waybill/HAWB number",
    "man_reg": "manifest registry number",
    "office": "customs office code",
    "weight": "gross mass",
    "packages": "number of packages"
  },
  "has_handwriting": true/false
}

IMPORTANT:
- Faint pencil marks appear as light gray strokes in this enhanced image.
- Numbers may be written sideways/vertically — still extract them.
- If you see ANY numeric values that appear handwritten, set has_handwriting to true.
- For customs_value_ec and customs_value_usd, return ONLY the numeric value (no $ sign).
Return ONLY valid JSON, no other text."""

        # Z.AI vision API uses OpenAI-compatible format with image_url
        # Base64 images use data URI: data:image/png;base64,...
        from urllib.request import Request, urlopen

        request_data = json.dumps({
            "model": vision_model,
            "max_tokens": 2000,
            "messages": [{
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/png;base64,{img_b64}",
                        }
                    },
                    {
                        "type": "text",
                        "text": prompt,
                    }
                ]
            }]
        }).encode('utf-8')

        # Retry on transient timeouts / connection resets. Vision API is
        # occasionally slow; one timeout should not permanently skip a
        # declaration (we need the waybill to split per declaration).
        last_err: Optional[Exception] = None
        result = None
        for attempt in range(3):
            req = Request(
                vision_endpoint,
                data=request_data,
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {api_key}',
                }
            )
            try:
                with urlopen(req, timeout=120) as response:
                    result = json.loads(response.read().decode('utf-8'))
                break
            except Exception as e:
                last_err = e
                logger.warning(
                    f"LLM vision attempt {attempt+1}/3 failed for "
                    f"{os.path.basename(pdf_path)}: {e}"
                )
        if result is None:
            raise last_err if last_err else RuntimeError("vision API unreachable")

        # OpenAI-compatible response format
        response_text = result['choices'][0]['message']['content'].strip()
        logger.info(f"LLM vision response for {os.path.basename(pdf_path)}: {response_text[:200]}")

        # Parse JSON from response (may be wrapped in ```json ... ```)
        cleaned = re.sub(r'^```(?:json)?\s*', '', response_text)
        cleaned = re.sub(r'\s*```$', '', cleaned)
        json_match = re.search(r'\{[\s\S]+\}', cleaned)
        if json_match:
            parsed = json.loads(json_match.group())
            # Post-process: if the LLM found handwriting but put the value
            # in other_notes instead of customs_value_ec/usd (common when it
            # can't determine the currency), extract the numeric value and
            # treat it as EC$ (the default currency on Grenada declarations).
            hw = parsed.get('handwritten', {})
            if parsed.get('has_handwriting') and hw:
                has_customs = (
                    _is_truthy_value(hw.get('customs_value_ec'))
                    or _is_truthy_value(hw.get('customs_value_usd'))
                )
                if not has_customs and hw.get('other_notes'):
                    notes = str(hw['other_notes'])
                    # Extract numeric value from notes like "122.66 written vertically"
                    num_match = re.search(r'(\d+\.?\d*)', notes)
                    if num_match:
                        val = float(num_match.group(1))
                        if val > 1.0:  # sanity: must be a meaningful amount
                            hw['customs_value_ec'] = str(val)
                            logger.info(
                                f"Recovered handwritten customs value EC${val} "
                                f"from other_notes: {notes}"
                            )
            # Persist to disk cache so subsequent runs skip the API call.
            if cache_path:
                try:
                    with open(cache_path, 'w') as f:
                        json.dump(parsed, f, indent=2)
                except Exception as e:
                    logger.debug(f"Vision cache write failed: {e}")
            return parsed

    except Exception as e:
        logger.warning(f"LLM vision extraction failed for {pdf_path}: {e}")

    return {}


def run(
    input_path: str,
    output_dir: Optional[str] = None,
    extract_metadata: bool = True,
    split_invoices: bool = True
) -> Dict:
    """
    Main entry point for PDF splitting.

    Args:
        input_path: Path to combined PDF
        output_dir: Output directory (default: same as input)
        extract_metadata: Whether to extract declaration metadata
        split_invoices: Whether to split multiple invoices into separate PDFs

    Returns:
        Dict with:
            - status: 'success' or 'error'
            - pages: List of page analysis results
            - output_files: Dict mapping doc type to output path (legacy format)
            - invoices: List of invoice PDF paths (when split_invoices=True)
            - declaration_metadata: Declaration metadata (if extract_metadata=True)
            - error: Error message (if status='error')
    """
    if not os.path.exists(input_path):
        return {'status': 'error', 'error': f'Input file not found: {input_path}'}

    if not pdfplumber:
        return {'status': 'error', 'error': 'pdfplumber not installed'}

    if not PdfReader:
        return {'status': 'error', 'error': 'PyPDF2 not installed'}

    if output_dir is None:
        output_dir = os.path.dirname(input_path)

    try:
        # Analyze pages
        pages, used_ocr, used_heuristic, page_texts = analyze_pdf(input_path)

        # Auto-reorder scanned-out-of-order pages (e.g. invoice scanned with
        # page 2 face-up first).  Reuses the page_texts from analyze_pdf so
        # this adds no extra OCR work — no-op when "Page N of M" / "N / M"
        # markers are already in physical order.
        try:
            new_order = detect_logical_page_order(page_texts)
            if new_order is not None:
                reorder_info = auto_reorder_if_scanned(
                    input_path, page_texts=page_texts,
                )
                if reorder_info.get('reordered'):
                    logger.info(
                        f"auto_reorder: {os.path.basename(input_path)} pages "
                        f"rewritten to logical order {new_order}"
                    )
                    # Permute in-memory state to match the rewritten PDF so
                    # downstream splitters see the corrected order.
                    page_texts = [page_texts[i] for i in new_order]
                    pages = [
                        DocumentPage(
                            page_num=j,
                            doc_type=pages[i].doc_type,
                            confidence=pages[i].confidence,
                            keywords_found=pages[i].keywords_found,
                        )
                        for j, i in enumerate(new_order)
                    ]
        except Exception as e:
            logger.warning(f"auto_reorder pre-pass failed for {input_path}: {e}")

        # Summary
        declaration_pages = [p for p in pages if p.doc_type == DocumentType.DECLARATION]
        invoice_pages = [p for p in pages if p.doc_type == DocumentType.INVOICE]
        unknown_pages = [p for p in pages if p.doc_type == DocumentType.UNKNOWN]

        logger.info(f"Analysis: {len(declaration_pages)} declaration, {len(invoice_pages)} invoice, {len(unknown_pages)} unknown pages")
        if used_ocr:
            logger.info("OCR was used for text extraction")
        if used_heuristic:
            logger.info("Position heuristic was used (OCR unavailable or failed)")

        # Split PDF - use multi-invoice splitter if enabled
        if split_invoices:
            multi_result = split_pdf_multi_invoice(input_path, pages, output_dir, page_texts=page_texts)
            # Build legacy output_files dict for compatibility
            output_files = {}
            if multi_result['declaration']:
                output_files[DocumentType.DECLARATION] = multi_result['declaration']
            if multi_result['invoices']:
                # Legacy: first invoice goes to 'invoice' key
                output_files[DocumentType.INVOICE] = multi_result['invoices'][0]
            invoices_list = multi_result['invoices']
        else:
            output_files = split_pdf(input_path, pages, output_dir)
            invoices_list = [output_files.get(DocumentType.INVOICE)] if DocumentType.INVOICE in output_files else []

        # Get declarations list
        declarations_list = multi_result.get('declarations', []) if split_invoices else []

        result = {
            'status': 'success',
            'input_path': input_path,
            'total_pages': len(pages),
            'used_ocr': used_ocr,
            'used_heuristic': used_heuristic,
            'ocr_available': OCR_AVAILABLE,
            'pages': [
                {
                    'page_num': p.page_num + 1,  # 1-indexed for display
                    'doc_type': p.doc_type,
                    'confidence': round(p.confidence, 2),
                    'keywords': p.keywords_found[:5],  # Top 5 keywords
                }
                for p in pages
            ],
            'output_files': output_files,
            'invoices': invoices_list,
            'invoice_count': len(invoices_list),
            'declarations': declarations_list,  # List of all declaration PDFs
            'declaration_count': len(declarations_list),
        }

        # Extract declaration metadata if requested
        if extract_metadata and DocumentType.DECLARATION in output_files:
            metadata = extract_declaration_metadata(output_files[DocumentType.DECLARATION])
            result['declaration_metadata'] = metadata

        return result

    except Exception as e:
        logger.exception(f"PDF split failed: {e}")
        return {'status': 'error', 'error': str(e)}


# ── Auto page-order detection (scanned-document recovery) ──────
# When a multi-page invoice is fed into the scanner reverse-side-up, the
# resulting PDF has pages in inverse order (e.g. page 2 first, page 1
# second).  Format parsers run "items_start..items_end" section scans on
# the concatenated text, so out-of-order pages can silently drop items
# (a section_start marker on a later physical page leaves anything that
# would have been "after" it on an earlier physical page outside the
# scan window).
#
# This helper looks for "Page N of M" / "N of M" / "N / M" footer markers
# (Amazon prints "orderID = ... 1 / 2" at the bottom of each page) and,
# when adjacent pages claim the same total but appear in the wrong order,
# returns a permutation that puts them back in logical order.  Pages
# without markers stay in their physical position so declarations and
# other appended documents are not disturbed.

_PAGE_MARKER_PATTERNS = [
    # Amazon: "orderID = 112-9925042-2903468 1 / 2" — captures identity
    (re.compile(
        r'order\s*I[Dd]\s*=\s*([\w\-\s]+?)\s+(\d+)\s*/\s*(\d+)\s*$',
        re.I | re.M,
    ), 'orderid'),
    # Generic "Page 1 of 2" or "page 1 of 2"
    (re.compile(r'page\s+(\d+)\s+of\s+(\d+)\b', re.I), 'page_of'),
    # Tail "1 / 2" at end of line (footer page numbers)
    (re.compile(r'(?:^|\s)(\d{1,2})\s*/\s*(\d{1,2})\s*$', re.M), 'slash'),
    # "1 of 2" anywhere (lowest priority — body-text false positives)
    (re.compile(r'\b(\d{1,2})\s+of\s+(\d{1,2})\b', re.I), 'of'),
]


def _extract_page_marker(text: str, max_total: int = 20) -> Optional[Tuple[int, int, str]]:
    """Return (logical_n, total_m, identity_key) for a page, or None.

    Identity key is the document identifier (e.g. order number) when the
    pattern provides one, otherwise a synthetic ``_anon_total_<M>`` so
    pages claiming the same total can still be grouped.

    Only the last ~400 chars of the page are searched so body text like
    "Pack of 2" or "(2 / 2 sets)" doesn't trigger false matches.
    """
    if not text:
        return None
    tail = text[-400:]
    for pat, name in _PAGE_MARKER_PATTERNS:
        last = None
        for m in pat.finditer(tail):
            last = m
        if not last:
            continue
        if name == 'orderid':
            identity = re.sub(r'\s+', '', last.group(1))
            n, total = int(last.group(2)), int(last.group(3))
        else:
            groups = last.groups()
            n, total = int(groups[-2]), int(groups[-1])
            identity = f'_anon_total_{total}'
        if 1 <= n <= total <= max_total:
            return (n, total, identity)
    return None


def detect_logical_page_order(page_texts: List[str]) -> Optional[List[int]]:
    """Return a physical→logical permutation, or None when no reorder is needed.

    Args:
        page_texts: text of each PDF page in physical order.

    Returns:
        ``new_order`` such that ``new_order[i]`` is the original (physical)
        page index that should appear at output position ``i``.  Returns
        ``None`` if every group is already in order, or if too few pages
        carry markers to make a confident decision.
    """
    if not page_texts:
        return None

    markers = [_extract_page_marker(t or '') for t in page_texts]

    # Group contiguous pages that share a total_m (and identity, when known).
    # An anonymous marker may join an adjacent identified group with the
    # same total, and inherits its identity.
    groups: List[Dict] = []
    current = None
    for i, marker in enumerate(markers):
        if marker is None:
            current = None
            continue
        n, total, identity = marker
        if current is not None:
            cg_total = current['total']
            cg_ident = current['identity']
            same_total = (cg_total == total)
            id_compat = (
                cg_ident == identity
                or cg_ident.startswith('_anon_')
                or identity.startswith('_anon_')
            )
            if same_total and id_compat:
                if cg_ident.startswith('_anon_') and not identity.startswith('_anon_'):
                    current['identity'] = identity
                current['members'].append((i, n))
                continue
        current = {'total': total, 'identity': identity, 'members': [(i, n)]}
        groups.append(current)

    new_order = list(range(len(page_texts)))
    changed = False
    for g in groups:
        members = g['members']
        if len(members) < 2:
            continue  # single-page groups can't be reordered
        physical_slots = sorted(p for p, _ in members)
        sorted_by_n = sorted(members, key=lambda x: x[1])
        # Sanity: distinct N values (don't reorder if two pages claim same N)
        ns = [n for _, n in sorted_by_n]
        if len(set(ns)) != len(ns):
            logger.debug(
                f"detect_logical_page_order: skipping group {g['identity']} "
                f"(duplicate page numbers {ns})"
            )
            continue
        for slot, (orig_phys, _) in zip(physical_slots, sorted_by_n):
            if new_order[slot] != orig_phys:
                changed = True
            new_order[slot] = orig_phys

    return new_order if changed else None


def auto_reorder_if_scanned(
    pdf_path: str,
    output_path: Optional[str] = None,
    page_texts: Optional[List[str]] = None,
) -> Dict:
    """Detect out-of-order scanned pages and rewrite the PDF in logical order.

    Idempotent: if the PDF is already in logical order (or carries no
    markers) this is a no-op.

    Args:
        pdf_path: PDF to inspect/rewrite.
        output_path: where to write the reordered PDF (defaults to
            ``pdf_path`` — overwrites in place).
        page_texts: pre-extracted page texts.  If omitted, this function
            extracts them via the same path ``analyze_pdf`` uses (with
            its OCR cache).

    Returns:
        ``{'reordered': bool, 'new_order': list|None, 'reason': str,
           'output': str|None}``.
    """
    if not os.path.exists(pdf_path):
        return {'reordered': False, 'reason': 'file_not_found', 'new_order': None}

    if page_texts is None:
        try:
            _, _, _, page_texts = analyze_pdf(pdf_path, use_ocr=True, use_heuristic=False)
        except Exception as e:
            logger.debug(f"auto_reorder: text extraction failed for {pdf_path}: {e}")
            return {'reordered': False, 'reason': f'extract_failed:{e}', 'new_order': None}

    new_order = detect_logical_page_order(page_texts or [])
    if new_order is None:
        return {'reordered': False, 'reason': 'order_already_correct_or_no_markers', 'new_order': None}

    target = output_path or pdf_path
    rewrite = reorder_pages(pdf_path, new_order, output_path=target)
    if rewrite.get('status') != 'success':
        return {
            'reordered': False,
            'reason': f"reorder_failed:{rewrite.get('error', 'unknown')}",
            'new_order': new_order,
        }

    # Refresh the .pages.json sidecar (keyed on file size — the new file
    # likely has a different size, so the cache would already be stale,
    # but rewriting it here saves an OCR pass on the next run).
    cache_path = target + '.pages.json'
    try:
        reordered_texts = [page_texts[i] for i in new_order]
        with open(cache_path, 'w', encoding='utf-8') as cf:
            json.dump({
                'size': os.path.getsize(target),
                'page_texts': reordered_texts,
            }, cf)
    except Exception as e:
        # Non-fatal: just delete the sidecar so analyze_pdf re-extracts.
        logger.debug(f"auto_reorder: sidecar refresh failed ({e}); deleting")
        try:
            if os.path.exists(cache_path):
                os.remove(cache_path)
        except OSError:
            pass

    # Also refresh the .txt sidecar if the splitter produced one.
    txt_sidecar = target.rsplit('.', 1)[0] + '.txt'
    if os.path.exists(txt_sidecar):
        try:
            with open(txt_sidecar, 'r', encoding='utf-8') as f:
                head = f.read(20)
            if not head.startswith('# MANUAL'):
                # Stale auto-generated sidecar — drop it; re-extraction
                # downstream will rebuild it from the reordered PDF.
                os.remove(txt_sidecar)
        except OSError:
            pass

    logger.info(
        f"auto_reorder: rewrote {os.path.basename(pdf_path)} "
        f"physical→logical = {new_order}"
    )
    return {
        'reordered': True,
        'reason': 'pages_out_of_order',
        'new_order': new_order,
        'output': target,
    }


def reorder_pages(input_path: str, page_order: List[int], output_path: str = None) -> Dict:
    """
    Reorder pages in a PDF according to the specified order.

    Args:
        input_path: Path to source PDF
        page_order: List of 0-indexed page numbers in desired order.
                    e.g. [1, 0] swaps the first two pages.
        output_path: Path to write reordered PDF. If None, overwrites input.

    Returns:
        Dict with status, output path, and page count.
    """
    if not PdfReader or not PdfWriter:
        return {'status': 'error', 'error': 'pypdf/PyPDF2 not installed'}

    if not os.path.exists(input_path):
        return {'status': 'error', 'error': f'Input file not found: {input_path}'}

    try:
        reader = PdfReader(input_path)
        total_pages = len(reader.pages)

        # Validate page indices
        for idx in page_order:
            if idx < 0 or idx >= total_pages:
                return {
                    'status': 'error',
                    'error': f'Invalid page index {idx}. PDF has {total_pages} pages (0-{total_pages - 1}).'
                }

        if len(page_order) != total_pages:
            return {
                'status': 'error',
                'error': f'page_order has {len(page_order)} entries but PDF has {total_pages} pages.'
            }

        writer = PdfWriter()
        for idx in page_order:
            writer.add_page(reader.pages[idx])

        # Write to temp file first, then replace (safe even if output == input)
        if not output_path:
            output_path = input_path

        tmp_path = output_path + '.tmp'
        with open(tmp_path, 'wb') as f:
            writer.write(f)

        # Replace original
        if os.path.exists(output_path):
            os.remove(output_path)
        os.rename(tmp_path, output_path)

        logger.info(f"Reordered PDF: {input_path} -> {output_path} (order: {page_order})")

        return {
            'status': 'success',
            'output': output_path,
            'page_count': total_pages,
            'page_order': page_order,
        }

    except Exception as e:
        logger.exception(f"PDF reorder failed: {e}")
        return {'status': 'error', 'error': str(e)}


def get_page_count(input_path: str) -> Dict:
    """Get the number of pages in a PDF."""
    if not PdfReader:
        return {'status': 'error', 'error': 'pypdf/PyPDF2 not installed'}
    if not os.path.exists(input_path):
        return {'status': 'error', 'error': f'File not found: {input_path}'}
    try:
        reader = PdfReader(input_path)
        return {'status': 'success', 'page_count': len(reader.pages)}
    except Exception as e:
        return {'status': 'error', 'error': str(e)}


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(description='Split combined PDF into Invoice and Declaration')
    parser.add_argument('input_pdf', help='Path to combined PDF')
    parser.add_argument('--output-dir', '-o', help='Output directory (default: same as input)')
    parser.add_argument('--no-metadata', action='store_true', help='Skip metadata extraction')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')

    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    result = run(
        args.input_pdf,
        output_dir=args.output_dir,
        extract_metadata=not args.no_metadata
    )

    print(json.dumps(result, indent=2))
    return 0 if result['status'] == 'success' else 1


if __name__ == '__main__':
    sys.exit(main())
