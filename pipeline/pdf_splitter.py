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

OCR_AVAILABLE = PYMUPDF_AVAILABLE and TESSERACT_AVAILABLE and TESSERACT_BINARY_AVAILABLE


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


def _preprocess_image(img_path: str) -> None:
    """Preprocess an image in-place to improve OCR accuracy.

    Uses OpenCV when available (best quality), falls back to Pillow.
    Pipeline: grayscale → upscale → denoise → CLAHE contrast → Otsu threshold
              → morphological cleanup to remove small noise specks.
    """
    try:
        import cv2
        import numpy as np

        img = cv2.imread(img_path)
        if img is None:
            return

        # Grayscale
        if len(img.shape) == 3:
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        else:
            gray = img

        # Upscale if small (below ~2000px width suggests low effective DPI)
        h, w = gray.shape
        if w < 2000:
            gray = cv2.resize(gray, (w * 2, h * 2), interpolation=cv2.INTER_CUBIC)

        # Denoise (h=10 balances noise removal vs preserving thin strokes)
        gray = cv2.fastNlMeansDenoising(gray, h=10)

        # CLAHE contrast enhancement (handles uneven lighting in scans)
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        gray = clahe.apply(gray)

        # Otsu threshold — auto-determines optimal threshold level,
        # better than adaptive for scanned invoices with uniform backgrounds.
        # Adaptive threshold can turn border/decoration noise into false text.
        _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)

        # Morphological opening — remove small noise specks (1-2px dots/marks)
        # that Tesseract misreads as punctuation or letters.
        kernel = np.ones((2, 2), np.uint8)
        binary = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel, iterations=1)

        cv2.imwrite(img_path, binary)
        return

    except ImportError:
        pass  # OpenCV not installed, fall back to Pillow
    except Exception as e:
        logger.debug(f"OpenCV preprocessing failed, trying Pillow: {e}")

    # Pillow fallback
    try:
        from PIL import Image, ImageEnhance, ImageFilter
        img = Image.open(img_path)

        if img.mode != 'L':
            img = img.convert('L')

        enhancer = ImageEnhance.Contrast(img)
        img = enhancer.enhance(1.5)
        img = img.filter(ImageFilter.SHARPEN)
        img.save(img_path)
    except ImportError:
        pass
    except Exception as e:
        logger.debug(f"Image preprocessing failed: {e}")


def _preprocess_image_adaptive(img_path: str) -> None:
    """Adaptive threshold preprocessing — better for receipts and uneven lighting."""
    try:
        import cv2
        img = cv2.imread(img_path, cv2.IMREAD_GRAYSCALE)
        if img is None:
            return
        if img.shape[1] < 2000:
            img = cv2.resize(img, None, fx=2, fy=2, interpolation=cv2.INTER_CUBIC)
        img = cv2.adaptiveThreshold(
            img, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY, 31, 15
        )
        cv2.imwrite(img_path, img)
    except ImportError:
        _preprocess_image(img_path)
    except Exception as e:
        logger.debug(f"Adaptive preprocessing failed: {e}")


def ocr_page(pdf_path: str, page_num: int, dpi: int = 300,
             preprocess: bool = True, psm: int = 6,
             preprocess_mode: str = 'default') -> str:
    """
    Extract text from a PDF page using OCR.
    Uses PyMuPDF for rendering and subprocess for tesseract (WSL compatible).

    Args:
        pdf_path: Path to PDF file
        page_num: 0-indexed page number
        dpi: Resolution for image conversion (default 300 for better OCR)
        preprocess: Apply image preprocessing for scanned docs
        psm: Tesseract page segmentation mode (default 6 = uniform block)
        preprocess_mode: 'default' (Otsu), 'adaptive' (adaptive threshold),
                         or 'none' (skip preprocessing)

    Returns:
        Extracted text string
    """
    if not OCR_AVAILABLE:
        return ""

    import subprocess
    import uuid

    try:
        # Use PyMuPDF to render page as image
        doc = fitz.open(pdf_path)
        page = doc.load_page(page_num)

        # Render at specified DPI
        mat = fitz.Matrix(dpi/72, dpi/72)
        pix = page.get_pixmap(matrix=mat)

        # Determine temp directory (Windows-native or WSL-compatible)
        import platform
        if platform.system() == 'Windows':
            import tempfile
            temp_dir = tempfile.gettempdir()
        else:
            # WSL: use Windows-accessible temp for tesseract.exe
            temp_dir = '/mnt/c/Temp'
        os.makedirs(temp_dir, exist_ok=True)

        unique_id = uuid.uuid4().hex[:8]
        img_path = os.path.join(temp_dir, f'ocr_page_{unique_id}.png')
        out_base = os.path.join(temp_dir, f'ocr_out_{unique_id}')
        out_file = out_base + '.txt'

        pix.save(img_path)
        doc.close()

        # Preprocess image for better OCR (contrast, sharpen, grayscale)
        if preprocess_mode == 'adaptive':
            _preprocess_image_adaptive(img_path)
        elif preprocess_mode != 'none' and preprocess:
            _preprocess_image(img_path)

        # Run tesseract via subprocess
        # --oem 3: Use LSTM neural net engine (most accurate)
        psm_str = str(psm)
        tesseract_cmd = pytesseract.pytesseract.tesseract_cmd
        if platform.system() == 'Windows':
            cmd = [tesseract_cmd, img_path, out_base,
                   '-l', 'eng', '--psm', psm_str, '--oem', '3']
        else:
            # WSL: convert to Windows paths for tesseract.exe
            win_img = to_windows_path(img_path)
            win_out = to_windows_path(out_base)
            cmd = [tesseract_cmd, win_img, win_out,
                   '-l', 'eng', '--psm', psm_str, '--oem', '3']

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)

        text = ""
        if result.returncode == 0 and os.path.exists(out_file):
            with open(out_file, 'r', encoding='utf-8') as f:
                text = f.read()

        # Cleanup temp files
        if os.path.exists(img_path):
            os.remove(img_path)
        if os.path.exists(out_file):
            os.remove(out_file)

        return text

    except Exception as e:
        logger.warning(f"OCR failed for page {page_num + 1}: {e}")
        return ""


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


def detect_invoice_boundaries(pdf_path: str, invoice_page_nums: List[int], page_texts: List[str] = None) -> List[List[int]]:
    """
    Detect boundaries between different invoices in the invoice pages.

    Looks for invoice number patterns to determine where one invoice ends
    and another begins.

    Args:
        pdf_path: Path to the original PDF
        invoice_page_nums: List of page numbers (0-indexed) that are invoices
        page_texts: Pre-extracted page texts (from analyze_pdf) to avoid re-OCR

    Returns:
        List of page number lists, each representing a separate invoice
    """
    import re

    if not invoice_page_nums:
        return []

    if len(invoice_page_nums) == 1:
        return [invoice_page_nums]

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
        text_upper = text.upper()

        patterns = [
            # SHEIN invoice: INVUS20240728002530332 (anywhere on page)
            (r'(INVUS\d{10,})', 1),
            # Amazon order format: 111-5908955-5240243
            (r'ORDER\s*#?\s*(\d{3}-\d{7}-\d{7})', 1),
            (r'(\d{3}-\d{7}-\d{7})', 1),
            # Temu/online: "Order ID: PO-211-12345..."
            (r'ORDER\s*ID[:\s]*(PO-\d{3}-\d+)', 1),
            # SHEIN/multi-column: "Order Number: Order Date:\nGSUNJY57BOONGXE 2024-07-17"
            # The ID is on the next line after "Order Number:" when two columns merge
            (r'ORDER\s*NUMBER[:\s].*\n\s*([A-Z0-9]{10,})', 1),
            # FashionNova / generic order number (same line)
            (r'ORDER\s*(?:#|NUMBER)[:\s]*([A-Z0-9][A-Z0-9-]{5,20})', 1),
            # Generic invoice number (after "Invoice No:" label)
            (r'INVOICE\s*(?:#|NO\.?|NUMBER)[:\s]*([A-Z0-9][A-Z0-9-]{5,20})', 1),
            # PO number
            (r'(?:P\.?O\.?|PURCHASE\s*ORDER)\s*(?:#|NO\.?)?[:\s]*([A-Z0-9-]{5,20})', 1),
        ]

        for pattern, group in patterns:
            match = re.search(pattern, text_upper)
            if match:
                invoice_id = match.group(group).strip()
                break

        page_invoice_ids.append((page_num, invoice_id))

    # Group pages by invoice ID
    invoice_groups = []
    current_group = []
    current_id = None

    for page_num, invoice_id in page_invoice_ids:
        if invoice_id and invoice_id != current_id:
            # New invoice detected
            if current_group:
                invoice_groups.append(current_group)
            current_group = [page_num]
            current_id = invoice_id
        else:
            # Same invoice or no ID detected - add to current group
            current_group.append(page_num)

    # Don't forget the last group
    if current_group:
        invoice_groups.append(current_group)

    logger.info(f"Detected {len(invoice_groups)} separate invoice(s) from {len(invoice_page_nums)} pages")

    return invoice_groups


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
        if len(invoice_pages) > 1:
            invoice_groups = detect_invoice_boundaries(pdf_path, sorted(invoice_pages), page_texts=page_texts)

        if len(invoice_groups) > 1:
            # Multiple distinct invoices detected — write each as separate PDF
            for idx, group_pages in enumerate(invoice_groups):
                writer = PdfWriter()
                for page_num in group_pages:
                    writer.add_page(reader.pages[page_num])

                output_path = os.path.join(output_dir, f"{base_name}_Invoice_{idx+1}.pdf")
                with open(output_path, 'wb') as f:
                    writer.write(f)

                result['invoices'].append(output_path)
                logger.info(f"Wrote invoice {idx+1}/{len(invoice_groups)}: {output_path} ({len(group_pages)} pages)")

                # Save OCR text sidecar to avoid re-OCR downstream
                # Don't overwrite existing sidecar (may have been manually corrected)
                if page_texts:
                    sidecar_text = "\n\n".join(page_texts[pn] for pn in group_pages if pn < len(page_texts) and page_texts[pn])
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
        'freight': None,
        'packages': None,
        'weight': None,
        'country_origin': None,
        'fob_value': None,
    }

    try:
        # Extract text from ALL pages (not just page 0)
        # CARICOM docs can be multi-page: BL + Shipper's Letter + CARICOM Invoice
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

        # Combine all pages
        text = '\n'.join(all_texts)

        if not text:
            return metadata

        lines = text.split('\n')
        text_upper = text.upper()

        # HAWB extraction: Search entire text first (handles two-column OCR layouts)
        # OCR may put labels and values on separate lines
        hawb_match = re.search(r'(HAWB[A-Z0-9-]+)', text_upper)
        if hawb_match:
            metadata['waybill'] = hawb_match.group(1)

        for i, line in enumerate(lines):
            line_upper = line.upper().strip()
            line_clean = line.strip()

            # Waybill fallback: If no HAWB found, try line-by-line extraction
            if 'WAYBILL' in line_upper and not metadata['waybill']:
                # Generic alphanumeric after waybill (but not "NUMBER" itself)
                match = re.search(r'WAYBILL[:\s]*(?:NUMBER[:\s]*)?([A-Z0-9-]+)', line_upper)
                if match and match.group(1) != 'NUMBER':
                    metadata['waybill'] = match.group(1)

            # Customs File Number
            if 'CUSTOMS' in line_upper and 'FILE' in line_upper and not metadata['customs_file']:
                match = re.search(r'CUSTOMS[^\d]*(\d+/\d+|\d+)', line_upper)
                if match:
                    metadata['customs_file'] = match.group(1)

            # Man Reg Number: "Man Reg Number: 2024/28" -> "2024 28"
            if ('MAN' in line_upper and 'REG' in line_upper) and not metadata['man_reg']:
                match = re.search(r'(\d{4})[/\s-]?(\d+)', line_upper)
                if match:
                    metadata['man_reg'] = f"{match.group(1)} {match.group(2)}"

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
                    # Extract freight from parentheses (client-written)
                    freight_match = re.search(r'\(\s*FREIGHT\s+([\d.]+)\s*(?:US)?\s*\)', consignee_part, re.IGNORECASE)
                    if freight_match:
                        metadata['freight'] = freight_match.group(1)
                        # Remove freight part from consignee name
                        consignee_name = re.sub(r'\s*\([^)]*FREIGHT[^)]*\)', '', consignee_part, flags=re.IGNORECASE).strip()
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

    return metadata


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
