#!/usr/bin/env python3
"""
Stage 1: PDF Extraction
Extracts text and tables from invoice PDFs using multiple OCR methods.

Architecture:
  1. Extract raw text from PDF (pdfplumber, pymupdf, or tesseract)
  2. Try data-driven FormatRegistry parsing first (config/formats/*.yaml)
  3. Fall back to legacy hardcoded parsing only if no format matches

Supported OCR methods:
  - pdfplumber (default): Good for PDFs with embedded text and tables
  - pymupdf (fitz): Fast, good for text extraction from native PDFs
  - tesseract: OCR for scanned/image-based PDFs

Usage:
  run(input_path, output_path, config={'ocr_method': 'tesseract'})
"""

import json
import os
import re
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Try to import format registry (data-driven parsing)
try:
    from format_registry import get_registry, FormatRegistry
    FORMAT_REGISTRY_AVAILABLE = True
except ImportError:
    FORMAT_REGISTRY_AVAILABLE = False
    logger.debug("FormatRegistry not available, using legacy parsing")

# Try to import extraction libraries
try:
    import pdfplumber
except ImportError:
    pdfplumber = None

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None

try:
    import pytesseract
    from PIL import Image
    import io
    TESSERACT_AVAILABLE = True

    # Configure tesseract path for Windows/WSL
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

except ImportError:
    TESSERACT_AVAILABLE = False

# Try to import pdf2image as fallback for rendering PDFs to images
try:
    from pdf2image import convert_from_path
    PDF2IMAGE_AVAILABLE = True
except ImportError:
    PDF2IMAGE_AVAILABLE = False

# Available OCR methods
OCR_METHODS = ['pdfplumber', 'pymupdf', 'tesseract']


def try_format_registry_parse(text: str, tables: List[Dict] = None) -> Optional[Dict]:
    """
    Try to parse text using data-driven FormatRegistry.

    This is the preferred parsing method - all format-specific logic
    lives in config/formats/*.yaml files.

    Args:
        text: Extracted invoice text
        tables: Optional table data from PDF

    Returns:
        Parsed result dict if a format matched, None otherwise
    """
    if not FORMAT_REGISTRY_AVAILABLE:
        return None

    try:
        # Get the format registry (loads specs from config/formats/)
        registry = get_registry()
        parser = registry.get_parser(text)

        if parser:
            logger.info(f"Using data-driven parser: {parser.name}")
            result = parser.parse(text)

            # Add tables to result if available
            if result and result.get('status') == 'success' and tables:
                if result.get('invoices'):
                    result['invoices'][0]['tables'] = tables

            return result

        logger.debug("No format matched in registry, will use legacy parsing")
        return None

    except Exception as e:
        logger.warning(f"FormatRegistry parsing failed: {e}, falling back to legacy")
        return None


def to_windows_path(linux_path: str) -> str:
    """Convert WSL Linux path to Windows path for tesseract."""
    if linux_path.startswith('/mnt/c/'):
        return 'C:' + linux_path[6:].replace('/', '\\')
    elif linux_path.startswith('/mnt/d/'):
        return 'D:' + linux_path[6:].replace('/', '\\')
    return linux_path


def get_available_methods() -> List[str]:
    """Return list of available OCR methods based on installed libraries."""
    available = []
    if pdfplumber:
        available.append('pdfplumber')
    if fitz:
        available.append('pymupdf')
    if TESSERACT_AVAILABLE:
        available.append('tesseract')
    return available


def run(input_path: str, output_path: str, config: Dict = None, context: Dict = None) -> Dict:
    """
    Extract invoice data from PDF or pre-extracted text file.

    Args:
        input_path: Path to PDF or text file
        output_path: Path to write extracted JSON
        config: Optional config dict with:
            - ocr_method: 'pdfplumber' (default), 'pymupdf', or 'tesseract'
            - skip_txt_fallback: If True, don't use existing .txt file
        context: Pipeline context

    Returns structured JSON with invoice metadata and raw line items.
    """
    config = config or {}
    ocr_method = config.get('ocr_method', 'pdfplumber')
    skip_txt_fallback = config.get('skip_txt_fallback', False)

    if not input_path or not os.path.exists(input_path):
        return {'status': 'error', 'error': f'Input file not found: {input_path}'}

    input_lower = input_path.lower()

    # Handle pre-extracted text files
    if input_lower.endswith('.txt'):
        try:
            # Read the text file
            with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
                text_content = f.read()

            # TRY DATA-DRIVEN PARSING FIRST (FormatRegistry)
            registry_result = try_format_registry_parse(text_content)
            if registry_result:
                registry_result['ocr_method'] = 'text_file'
                if output_path:
                    os.makedirs(os.path.dirname(output_path), exist_ok=True)
                    with open(output_path, 'w') as f:
                        json.dump(registry_result, f, indent=2)
                return registry_result

            # LEGACY FALLBACK: Use format_parser legacy functions
            logger.debug("No format matched for text file, using legacy parser")
            from format_parser import parse_text_file
            result = parse_text_file(input_path, output_path)
            result['ocr_method'] = 'text_file'
            return result
        except Exception as e:
            logger.error(f"Text file parsing failed: {e}")
            import traceback
            traceback.print_exc()
            return {
                'status': 'error',
                'error': f'Text file parsing failed: {str(e)}'
            }

    # Reject XLSX files - they should not go through extract stage
    if input_lower.endswith('.xlsx') or input_lower.endswith('.xls'):
        return {
            'status': 'error',
            'error': f'Cannot extract from XLSX file: {input_path}. '
                     'XLSX files are already processed - use validate or reclassify instead.'
        }

    # If a .txt file exists alongside the PDF, prefer text parsing (unless skip_txt_fallback)
    # This is common for Amazon invoices where text was pre-extracted
    if not skip_txt_fallback:
        txt_path = os.path.splitext(input_path)[0] + '.txt'
        if os.path.exists(txt_path):
            try:
                # Read the text file
                with open(txt_path, 'r', encoding='utf-8', errors='replace') as f:
                    text_content = f.read()

                # TRY DATA-DRIVEN PARSING FIRST (FormatRegistry)
                registry_result = try_format_registry_parse(text_content)
                if registry_result:
                    registry_result['ocr_method'] = 'text_file'
                    if output_path:
                        os.makedirs(os.path.dirname(output_path), exist_ok=True)
                        with open(output_path, 'w') as f:
                            json.dump(registry_result, f, indent=2)
                    return registry_result

                # LEGACY FALLBACK: Use format_parser legacy functions
                logger.debug("No format matched for txt file, using legacy parser")
                from format_parser import parse_text_file
                result = parse_text_file(txt_path, output_path)
                result['ocr_method'] = 'text_file'
                return result
            except Exception as e:
                logger.warning(f"Text parsing failed for {txt_path}, falling back to {ocr_method}: {e}")

    # Validate OCR method
    available = get_available_methods()
    if ocr_method not in available:
        return {
            'status': 'error',
            'error': f"OCR method '{ocr_method}' not available. "
                     f"Installed methods: {available}. "
                     f"Install with: pip install pdfplumber PyMuPDF pytesseract pillow"
        }

    logger.info(f"Extracting with OCR method: {ocr_method}")

    # Dispatch to appropriate extraction method
    if ocr_method == 'pymupdf':
        return extract_with_pymupdf(input_path, output_path)
    elif ocr_method == 'tesseract':
        return extract_with_tesseract(input_path, output_path)
    else:
        return extract_with_pdfplumber(input_path, output_path)


def _extract_text_tables_unified(input_path: str) -> tuple:
    """Unified text+tables extraction via the hybrid OCR pipeline.

    Returns ``(full_text, tables_list, engine_label)``. Text always comes
    from ``multi_ocr.extract_text`` (which handles the digital-PDF short-
    circuit plus the full preprocessing × engine matrix and consensus).
    Tables are a supplementary best-effort pass via pdfplumber, since
    multi_ocr doesn't do table extraction.
    """
    try:
        import multi_ocr
    except ImportError:
        return "", [], "none"

    try:
        result = multi_ocr.extract_text(input_path)
        full_text = result.text or ""
        engine_label = result.engine_used or "multi_ocr"
    except Exception as e:
        logger.error(f"multi_ocr extract_text failed for {input_path}: {e}")
        return "", [], "none"

    all_tables: List[Dict] = []
    if pdfplumber:
        try:
            with pdfplumber.open(input_path) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    for table in (page.extract_tables() or []):
                        all_tables.append({
                            'page': page_num + 1,
                            'rows': table,
                        })
        except Exception as e:
            logger.debug(f"pdfplumber table pass failed for {input_path}: {e}")

    return full_text, all_tables, engine_label


def _build_extract_result(
    full_text: str,
    all_tables: List[Dict],
    engine_label: str,
    output_path: str,
) -> Dict:
    """Shared result-shaping path: FormatRegistry → legacy → JSON write."""
    # TRY DATA-DRIVEN PARSING FIRST (FormatRegistry)
    registry_result = try_format_registry_parse(full_text, all_tables)
    if registry_result:
        registry_result['ocr_method'] = engine_label
        if output_path:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, 'w') as f:
                json.dump(registry_result, f, indent=2)
        return registry_result

    # LEGACY FALLBACK: Use hardcoded parsing functions
    logger.debug("Using legacy hardcoded parsing")
    invoice_data = {
        'invoice_number': extract_invoice_number(full_text),
        'date': extract_date(full_text),
        'supplier': extract_supplier(full_text),
        'total': extract_total(full_text),
        'freight': extract_shipping(full_text),
        'tax': extract_tax(full_text),
        'discount': extract_discount(full_text),
        'items': [],
        'raw_text': full_text[:5000],
        'tables': all_tables,
    }

    # Prefer table-based items, fall back to text-based
    if all_tables:
        items = extract_items_from_tables(all_tables)
        invoice_data['items'] = items
    if not invoice_data['items']:
        text_items = extract_items_from_text(full_text)
        if text_items:
            invoice_data['items'] = text_items

    result = {
        'status': 'success',
        'ocr_method': engine_label,
        'invoices': [invoice_data],
    }

    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(result, f, indent=2)

    return result


def extract_with_pdfplumber(input_path: str, output_path: str) -> Dict:
    """Legacy entry point — now routes through the unified hybrid OCR pipeline.

    The ``pdfplumber`` / ``pymupdf`` / ``tesseract`` labels no longer
    correspond to three disjoint code paths. ``multi_ocr.extract_text``
    always runs, and the engine actually used is recorded in the result.
    """
    try:
        full_text, all_tables, engine_label = _extract_text_tables_unified(input_path)
    except Exception as e:
        return {'status': 'error', 'error': str(e)}

    if len(full_text.strip()) < 50:
        return {
            'status': 'error',
            'error': 'multi_ocr produced no usable text for this PDF. '
                     'Check that PyMuPDF and at least one OCR backend '
                     '(tesseract / paddleocr / vision_api) are installed.'
        }

    return _build_extract_result(full_text, all_tables, engine_label, output_path)


def extract_with_pymupdf(input_path: str, output_path: str) -> Dict:
    """Legacy entry point — delegates to the unified hybrid OCR pipeline."""
    return extract_with_pdfplumber(input_path, output_path)


def _run_tesseract_on_image(img_path: str, temp_dir: str) -> str:
    """Run Tesseract OCR on a single image file and return extracted text.

    Uses pytesseract.image_to_string() which handles tesseract binary
    resolution internally, avoiding path issues on Windows/WSL.
    Falls back to subprocess if image_to_string fails.
    """
    import platform

    # Primary: use pytesseract's built-in image_to_string (handles paths correctly)
    try:
        pil_img = Image.open(img_path)
        logger.info(f"OCR: Running pytesseract.image_to_string on {os.path.basename(img_path)} ({pil_img.size[0]}x{pil_img.size[1]} {pil_img.mode})")
        text = pytesseract.image_to_string(pil_img, lang='eng')
        logger.info(f"OCR result: {len(text)} chars, first 100: {repr(text[:100])}")
        if text.strip():
            return text
        else:
            logger.warning(f"OCR returned empty/whitespace text for {img_path}")
    except Exception as e:
        logger.warning(f"pytesseract.image_to_string failed: {e}", exc_info=True)

    # Fallback: subprocess approach (for WSL where tesseract.exe needs Windows paths)
    import subprocess
    import uuid

    unique_id = uuid.uuid4().hex[:8]
    out_base = os.path.join(temp_dir, f'ocr_inv_out_{unique_id}')
    out_file = out_base + '.txt'

    tesseract_cmd = pytesseract.pytesseract.tesseract_cmd
    win_img = to_windows_path(img_path)
    win_out = to_windows_path(out_base)

    try:
        cmd = [tesseract_cmd, win_img, win_out, '-l', 'eng']
        logger.info(f"OCR subprocess: {cmd}")
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        logger.info(f"OCR subprocess exit code: {proc.returncode}, stderr: {proc.stderr[:300]}")
        if proc.returncode != 0:
            logger.warning(f"Tesseract subprocess failed (code {proc.returncode}): {proc.stderr[:200]}")

        text = ""
        if os.path.exists(out_file):
            with open(out_file, 'r', encoding='utf-8', errors='replace') as f:
                text = f.read()
        else:
            logger.warning(f"Tesseract output file not found: {out_file}")
        logger.info(f"OCR subprocess result: {len(text)} chars, first 100: {repr(text[:100])}")
        return text
    except Exception as e:
        logger.warning(f"Tesseract subprocess error: {e}")
        return ""
    finally:
        if os.path.exists(out_file):
            os.remove(out_file)


def _extract_images_with_pypdf(input_path: str) -> List:
    """Extract embedded images from PDF pages using pypdf (pure Python, no system deps)."""
    try:
        from pypdf import PdfReader
    except ImportError:
        return []

    images = []
    try:
        reader = PdfReader(input_path)
        for page_num, page in enumerate(reader.pages):
            for img_obj in page.images:
                try:
                    pil_img = Image.open(io.BytesIO(img_obj.data))
                    images.append((page_num, pil_img))
                except Exception:
                    pass
    except Exception as e:
        logger.warning(f"pypdf image extraction failed: {e}")
    return images


def extract_with_tesseract(input_path: str, output_path: str) -> Dict:
    """Legacy entry point — delegates to the unified hybrid OCR pipeline.

    The old implementation rendered pages with PyMuPDF/pypdf/pdf2image
    and ran a single Tesseract pass per page. That single-engine path
    has been replaced by ``multi_ocr.extract_text`` which runs every
    configured preprocessing × engine combination and reconciles via
    consensus, so Tesseract is now just one of several engines whose
    outputs feed the vote.
    """
    return extract_with_pdfplumber(input_path, output_path)


def extract_items_from_text(text: str) -> List[Dict]:
    """Extract line items from raw text using pattern matching."""
    items = []

    # Common patterns for invoice line items
    # Pattern: SKU/Code + Description + Qty + Price + Total
    patterns = [
        # Pattern 1: number code description qty price total
        re.compile(
            r'^\s*(\d+)\s+'  # Line number
            r'(\S+)\s+'      # SKU/Code
            r'(.+?)\s+'      # Description
            r'(\d+)\s+'      # Quantity
            r'[\$]?([\d,]+\.?\d*)\s+'  # Unit price
            r'[\$]?([\d,]+\.?\d*)\s*$',  # Total
            re.MULTILINE
        ),
        # Pattern 2: code description qty price total
        re.compile(
            r'^\s*([A-Z0-9-]+)\s+'  # SKU/Code
            r'(.+?)\s{2,}'          # Description (followed by 2+ spaces)
            r'(\d+)\s+'             # Quantity
            r'[\$]?([\d,]+\.?\d*)\s+'  # Unit price
            r'[\$]?([\d,]+\.?\d*)\s*$',  # Total
            re.MULTILINE
        ),
    ]

    for pattern in patterns:
        for match in pattern.finditer(text):
            groups = match.groups()
            try:
                if len(groups) == 6:
                    # Pattern with line number
                    qty = int(groups[3])
                    unit_price = float(groups[4].replace(',', ''))
                    total = float(groups[5].replace(',', ''))
                    if qty > 0 and total > 0:
                        items.append({
                            'sku': groups[1],
                            'description': groups[2].strip(),
                            'quantity': qty,
                            'unit_cost': round(unit_price, 2),
                            'total_cost': total,
                        })
                elif len(groups) == 5:
                    # Pattern without line number
                    qty = int(groups[2])
                    unit_price = float(groups[3].replace(',', ''))
                    total = float(groups[4].replace(',', ''))
                    if qty > 0 and total > 0:
                        items.append({
                            'sku': groups[0],
                            'description': groups[1].strip(),
                            'quantity': qty,
                            'unit_cost': round(unit_price, 2),
                            'total_cost': total,
                        })
            except (ValueError, IndexError):
                continue

        if items:
            break  # Use first pattern that finds items

    # If no items found yet, try simpler receipt/order patterns
    if not items:
        items = extract_simple_receipt_items(text)

    return items


def normalize_ocr_price(price_str: str) -> float:
    """
    Normalize OCR price strings that may have space instead of decimal.
    Handles: "$39.99", "$39 99", "39 99", "39.99"
    """
    cleaned = price_str.replace('$', '').replace(',', '').strip()
    # Handle OCR artifact: "$39 99" -> "39.99" (space instead of decimal)
    space_cents = re.match(r'^(\d+)\s+(\d{2})$', cleaned)
    if space_cents:
        return float(f"{space_cents.group(1)}.{space_cents.group(2)}")
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def extract_simple_receipt_items(text: str) -> List[Dict]:
    """
    Extract items from simple receipt/order formats.
    Handles formats like:
      - "1pc Description $13.23"
      - "Description $13.23"
      - "1 of: Description $13.23" (Amazon)
      - "1 of: Description $39 99" (Amazon with OCR space artifact)
      - Lines in "Item details" section
    """
    items = []
    lines = text.split('\n')

    # Look for item section markers
    in_item_section = False
    item_section_markers = ['item detail', 'item(s)', 'ordered item', 'items ordered', 'product', 'description']

    for i, line in enumerate(lines):
        line_lower = line.lower().strip()
        line_clean = line.strip()

        # Check for item section start
        if any(marker in line_lower for marker in item_section_markers):
            in_item_section = True
            continue

        # Check for section end markers
        if in_item_section and any(marker in line_lower for marker in ['shipping address', 'payment info', 'subtotal', 'total:', 'billing']):
            in_item_section = False

        # Try to extract item from line
        if len(line_clean) > 10:
            # Amazon format: "1 of: Description ... $Price"
            # Price may have OCR noise before it, so look for $ followed by price at end
            # Also handle OCR artifact where price is "$39 99" instead of "$39.99"
            amazon_match = re.search(
                r'^(\d+)\s+of:\s+(.+?)\s+\$\s*([\d,]+(?:\.|\s)\d{0,2})\s*[\}\)\|\s]*$',
                line_clean,
                re.IGNORECASE
            )
            if amazon_match:
                qty = int(amazon_match.group(1))
                desc = amazon_match.group(2).strip()
                # Clean description - remove trailing OCR noise
                desc = re.sub(r'\s*[©\}\)\|\d\s]+$', '', desc).strip()
                price = normalize_ocr_price(amazon_match.group(3))
                if qty > 0 and price > 0 and len(desc) > 3:
                    items.append({
                        'sku': f'ITEM-{len(items)+1:03d}',
                        'description': desc,
                        'quantity': qty,
                        'unit_cost': round(price / qty, 2),
                        'total_cost': price,
                    })
                    continue

            # Amazon alternate: line starts with "1 of:" and has price somewhere
            # Handle both "$39.99" and "$39 99" OCR formats
            if re.match(r'^\d+\s+of:', line_clean, re.IGNORECASE):
                # Find prices - match both "$39.99" and "$39 99" formats
                prices = re.findall(r'\$([\d,]+(?:\.|\s)\d{2}|\d+)', line_clean)
                if prices:
                    qty_match = re.match(r'^(\d+)\s+of:\s*', line_clean, re.IGNORECASE)
                    if qty_match:
                        qty = int(qty_match.group(1))
                        # Extract description between "of:" and first $ or end
                        desc_match = re.search(r'of:\s*(.+?)(?:\s*[\$©\}\|\d]{2,}|\s*$)', line_clean, re.IGNORECASE)
                        if desc_match:
                            desc = desc_match.group(1).strip()
                            desc = re.sub(r'\s*[©\}\)\|\d\s]+$', '', desc).strip()
                            price = normalize_ocr_price(prices[-1])
                            if qty > 0 and price > 0 and len(desc) > 5:
                                items.append({
                                    'sku': f'ITEM-{len(items)+1:03d}',
                                    'description': desc,
                                    'quantity': qty,
                                    'unit_cost': round(price / qty, 2),
                                    'total_cost': price,
                                })
                                continue
            # Pattern: "Qty x Description $Price" or "Qtypc Description $Price"
            # Also handles OCR misreads like "Ipc" for "1pc" or "lpc" for "1pc"
            match = re.search(
                r'^([1-9Il])\s*(?:x|pc|pcs)?\s+(.+?)\s+\$?([\d,]+\.?\d{0,2})\s*$',
                line_clean,
                re.IGNORECASE
            )
            if match:
                # Handle OCR misreads: 'I' or 'l' often misread as '1'
                qty_str = match.group(1)
                if qty_str in ('I', 'l'):
                    qty = 1
                else:
                    qty = int(qty_str)
                desc = match.group(2).strip()
                price = float(match.group(3).replace(',', ''))
                if qty > 0 and price > 0 and len(desc) > 3:
                    items.append({
                        'sku': f'ITEM-{len(items)+1:03d}',
                        'description': desc,
                        'quantity': qty,
                        'unit_cost': round(price / qty, 2),
                        'total_cost': price,
                    })
                    continue

            # Pattern: "Description $Price" (assume qty=1)
            match = re.search(
                r'^([A-Za-z].+?)\s+\$?([\d,]+\.\d{2})\s*$',
                line_clean
            )
            if match and in_item_section:
                desc = match.group(1).strip()
                price_str = match.group(2).replace(',', '')
                if not price_str:
                    continue
                price = float(price_str)
                # Filter out non-item lines
                if price > 0.5 and len(desc) > 5:
                    skip_words = ['shipping', 'tax', 'subtotal', 'total', 'discount', 'fee', 'free']
                    if not any(word in desc.lower() for word in skip_words):
                        items.append({
                            'sku': f'ITEM-{len(items)+1:03d}',
                            'description': desc,
                            'quantity': 1,
                            'unit_cost': price,
                            'total_cost': price,
                        })

    return items


def extract_invoice_number(text: str) -> Optional[str]:
    """Extract invoice number from text."""
    # Patterns ordered from most specific to least specific
    patterns = [
        # Temu/online: "Order ID: PO-211-12345..."
        r'Order\s*ID[:\s]*(PO-\d{3}-\d+)',
        # Amazon order format: 111-5908955-5240243
        r'(\d{3}-\d{7}-\d{7})',
        # Standard invoice patterns
        r'Invoice\s*#?\s*:?\s*([A-Z0-9-]+)',
        r'INV[-\s]?(\d+)',
        r'Invoice\s+Number\s*:?\s*([A-Z0-9-]+)',
        # Order number patterns
        r'Order\s*(?:#|No\.?|Number)[:\s]*([A-Z0-9][A-Z0-9-]{3,})',
        # PO patterns
        r'(PO-[A-Z0-9-]+)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            result = match.group(1).strip()
            # Validate: must have at least one digit
            if any(c.isdigit() for c in result):
                return result
    return None


def extract_date(text: str) -> Optional[str]:
    """Extract invoice date from text."""
    patterns = [
        # Order time: Jun 3, 2024
        r'(?:Order\s*time|Date)[:\s]*((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{1,2},?\s+\d{4})',
        # Standard date formats
        r'Date\s*:?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})',
        r'(\d{4}-\d{2}-\d{2})',
        r'(\d{2}/\d{2}/\d{4})',
        r'(\d{1,2}/\d{1,2}/\d{4})',
        # YYYY/MM/DD format (like 2024/06/03 from OCR)
        r'(\d{4}/\d{2}/\d{2})',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def extract_supplier(text: str) -> Optional[str]:
    """Extract supplier name from text."""
    text_upper = text.upper()

    # Check for known vendor names/patterns in text
    known_vendors = [
        ('TEMU', 'TEMU'),
        ('AMAZON.COM', 'AMAZON'),
        ('AMAZON', 'AMAZON'),
        ('WALMART', 'WALMART'),
        ('TARGET', 'TARGET'),
        ('ABSOLUTE', 'ABSOLUTE'),
        ('ALIBABA', 'ALIBABA'),
        ('ALIEXPRESS', 'ALIEXPRESS'),
        ('SHEIN', 'SHEIN'),
        ('EBAY', 'EBAY'),
    ]

    for pattern, vendor_name in known_vendors:
        if pattern in text_upper:
            return vendor_name

    # Check for Temu by Order ID format (PO-211-XXXX)
    if re.search(r'ORDER\s*ID[:\s]*PO-\d{3}-\d+', text_upper):
        return 'TEMU'

    # Check for Amazon by Order ID format (XXX-XXXXXXX-XXXXXXX)
    if re.search(r'\d{3}-\d{7}-\d{7}', text):
        return 'AMAZON'

    # Fallback: look for explicit labels
    patterns = [
        r'(?:From|Supplier|Vendor|Bill From|Sold By)\s*:?\s*(.+?)(?:\n|$)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

    return None


def extract_total(text: str) -> Optional[float]:
    """Extract invoice total from text."""
    # Patterns ordered from most specific to least specific
    # Prefer "Order total" and "Grand Total" over generic "Total"
    patterns = [
        r'Order\s+total\s*:?\s*\$?([\d,]+\.?\d*)',
        r'Grand\s+Total\s*:?\s*\$?([\d,]+\.?\d*)',
        r'Amount\s+Due\s*:?\s*\$?([\d,]+\.?\d*)',
        r'Invoice\s+Total\s*:?\s*\$?([\d,]+\.?\d*)',
        # Generic "Total:" but not "Item total" or "Subtotal"
        r'(?<!Item\s)(?<!Sub)Total\s*:?\s*\$?([\d,]+\.?\d*)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                val = float(match.group(1).replace(',', ''))
                if val > 0:
                    return val
            except ValueError:
                continue
    return None


def extract_shipping(text: str) -> Optional[float]:
    """Extract shipping/freight cost from text."""
    patterns = [
        r'Shipping\s*:?\s*\$?([\d,]+\.?\d*)',
        r'Freight\s*:?\s*\$?([\d,]+\.?\d*)',
        r'Delivery\s*:?\s*\$?([\d,]+\.?\d*)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                val = float(match.group(1).replace(',', ''))
                if val > 0:
                    return val
            except ValueError:
                continue
    return None


def extract_tax(text: str) -> Optional[float]:
    """Extract tax from text."""
    patterns = [
        r'Sales\s+tax\s*:?\s*\$?([\d,]+\.?\d*)',
        r'Tax\s*:?\s*\$?([\d,]+\.?\d*)',
        r'VAT\s*:?\s*\$?([\d,]+\.?\d*)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                val = float(match.group(1).replace(',', ''))
                if val > 0:
                    return val
            except ValueError:
                continue
    return None


def extract_discount(text: str) -> Optional[float]:
    """Extract discount from text."""
    patterns = [
        r'(?:Item\s*)?discount\s*:?\s*-?\$?([\d,]+\.?\d*)',
        r'Savings\s*:?\s*-?\$?([\d,]+\.?\d*)',
        r'Promo\s*:?\s*-?\$?([\d,]+\.?\d*)',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            try:
                val = float(match.group(1).replace(',', ''))
                if val > 0:
                    return val
            except ValueError:
                continue
    return None


def extract_items_from_tables(tables: List[Dict]) -> List[Dict]:
    """Extract line items from table data."""
    items = []

    for table_data in tables:
        rows = table_data.get('rows', [])
        if not rows or len(rows) < 2:
            continue

        # Try to identify header row
        header = rows[0]
        if not header:
            continue

        # Map columns
        col_map = identify_columns(header)

        for row in rows[1:]:
            if not row or all(cell is None or str(cell).strip() == '' for cell in row):
                continue

            item = {}
            for field, col_idx in col_map.items():
                if col_idx < len(row):
                    val = row[col_idx]
                    if val is not None:
                        item[field] = str(val).strip()

            if item.get('description'):
                # Parse numeric fields
                for field in ['quantity', 'unit_cost', 'total_cost']:
                    if field in item:
                        try:
                            item[field] = float(str(item[field]).replace(',', '').replace('$', ''))
                        except ValueError:
                            pass

                items.append(item)

    return items


def identify_columns(header: List) -> Dict[str, int]:
    """Map header cells to field names."""
    col_map = {}
    keywords = {
        'description': ['description', 'item', 'product', 'name'],
        'quantity': ['qty', 'quantity', 'units'],
        'unit_cost': ['unit cost', 'unit price', 'price', 'rate'],
        'total_cost': ['total', 'amount', 'ext', 'extended'],
        'sku': ['sku', 'item #', 'item no', 'code', 'part'],
    }

    for i, cell in enumerate(header):
        if cell is None:
            continue
        cell_lower = str(cell).lower().strip()
        for field, kws in keywords.items():
            if field not in col_map:
                for kw in kws:
                    if kw in cell_lower:
                        col_map[field] = i
                        break

    return col_map


if __name__ == '__main__':
    import sys
    import argparse

    parser = argparse.ArgumentParser(description='Extract data from PDF using various OCR methods')
    parser.add_argument('input', help='Input PDF file path')
    parser.add_argument('output', nargs='?', help='Output JSON file path')
    parser.add_argument('--config', help='JSON config string with ocr_method and skip_txt_fallback')
    parser.add_argument('--ocr-method', choices=['pdfplumber', 'pymupdf', 'tesseract'],
                        default='pdfplumber', help='OCR method to use')
    parser.add_argument('--skip-txt-fallback', action='store_true',
                        help='Skip using existing .txt file')

    args = parser.parse_args()

    # Parse config if provided
    config = {}
    if args.config:
        try:
            config = json.loads(args.config)
        except json.JSONDecodeError:
            print(json.dumps({'status': 'error', 'error': f'Invalid config JSON: {args.config}'}))
            sys.exit(1)

    # CLI args override config
    if args.ocr_method != 'pdfplumber':
        config['ocr_method'] = args.ocr_method
    if args.skip_txt_fallback:
        config['skip_txt_fallback'] = True

    result = run(args.input, args.output, config=config)

    # Output result as JSON
    print('REPORT:' + json.dumps(result))

    if result.get('status') == 'error':
        sys.exit(1)
