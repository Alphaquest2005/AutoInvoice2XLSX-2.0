"""
Single-invoice processing stage.

Handles one invoice PDF through the complete pipeline:
  extract text → detect format → parse → match PO → classify → generate XLSX

Used by both the BL batch pipeline and the unified entry point.
"""

import json
import logging
import os
import re
import shutil
import sys
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Ensure pipeline directory is on path for imports
PIPELINE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PIPELINE_DIR not in sys.path:
    sys.path.insert(0, PIPELINE_DIR)

from bl_xlsx_generator import generate_bl_xlsx
from stages.supplier_resolver import (
    extract_pdf_text,
    normalize_parse_result,
    classify_matched_items,
    get_supplier_info,
    update_supplier_entry,
)


@dataclass
class InvoiceResult:
    """Result of processing a single invoice PDF."""
    pdf_file: str                    # original filename (e.g. "8440870.pdf")
    invoice_num: str                 # parsed invoice number
    invoice_data: Dict               # normalized invoice data
    matched_items: List[Dict]        # PO-matched items with tariff codes
    supplier_info: Dict              # {code, name, address, country}
    xlsx_path: str                   # path to generated XLSX
    pdf_output_path: str             # path to copied PDF in output dir
    classified_count: int            # items successfully classified
    matched_count: int               # items matched to PO
    format_name: str                 # detected format name
    freight: float = 0.0             # invoice freight amount
    packages: int = 1                # default, updated by BL allocation


def _extract_invoice_number(text: str) -> Optional[str]:
    """
    Try to extract an invoice number from raw PDF text using common patterns.

    Returns the first match found, or None if no invoice number pattern matches.
    """
    if not text:
        return None

    # Common invoice number patterns (ordered by specificity)
    patterns = [
        # "Invoice No: 12345" / "Invoice Number: 12345" / "Invoice #12345"
        r'(?:Invoice|Inv|Factura)\s*(?:No\.?|Number|Num|#)[:\s]*([A-Z0-9][\w\-/]{2,30})',
        # "Invoice INV/SX/2024/024077" — keyword followed directly by the number
        r'Invoice[: \t]+([A-Z0-9][\w\-/]{4,30})',
        # "Invoice # <header>\n<number>" — tabular: header row then data row
        r'Invoice\s*#[^\n]*\n\s*([A-Z0-9][\w\-/]{2,30})',
        # "INV-12345" or "INV_12345" or "INV/SX/2024/..." standalone patterns
        r'\b(INV[\-_/][A-Z0-9][\w\-/]{2,25})\b',
        # "Order No: 12345" / "Order Number: 12345" / "Order #12345" (same line)
        r'(?:Order|Sales\s*Order)\s*(?:No\.?|Number|#)[: \t]*([A-Z0-9][\w\-/]{2,30})',
        # "Reference No: 12345"
        r'(?:Ref(?:erence)?|Doc(?:ument)?)\s*(?:No\.?|Number|#)[: \t]+([A-Z0-9][\w\-/]{4,30})',
    ]

    # Words that should never be returned as an invoice number
    _reject = {'THE', 'FOR', 'AND', 'USD', 'ORDER', 'DESCRIPTION', 'DATE',
                'AMOUNT', 'TOTAL', 'PAGE', 'BILL', 'FROM', 'SHIP', 'ITEM',
                'PIECES', 'QUANTITY', 'NUMBER', 'ERENCES', 'DETAILS'}

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            candidate = match.group(1).strip().rstrip('.')
            # Must be 3+ chars, contain at least one digit, and not be a common word
            if (len(candidate) >= 3
                    and any(c.isdigit() for c in candidate)
                    and candidate.upper() not in _reject):
                return candidate

    return None


def _find_currency_conversion_json(pdf_path: str) -> Optional[str]:
    """Locate `<base>_currency_conversion.json` sidecar next to a split invoice PDF."""
    pdf_dir = os.path.dirname(pdf_path)
    pdf_base = os.path.splitext(os.path.basename(pdf_path))[0]
    # Strip _Invoice or _Invoice_N suffix to find the original base name
    m = re.match(r'^(.+?)_Invoice(?:_\d+)?$', pdf_base)
    if not m:
        return None
    orig_base = m.group(1)
    json_path = os.path.join(pdf_dir, f"{orig_base}_currency_conversion.json")
    return json_path if os.path.exists(json_path) else None


def _apply_currency_conversion(pdf_path: str, text: str, invoice_data: Dict) -> None:
    """
    If this invoice was extracted from a PDF that also shipped with a currency
    conversion page (e.g. xe.com screenshot), and the invoice is in the rate's
    source currency, convert item prices and invoice totals to the target
    currency (normally USD). Mutates invoice_data in place.
    """
    json_path = _find_currency_conversion_json(pdf_path)
    if not json_path:
        return
    try:
        with open(json_path, 'r', encoding='utf-8') as jf:
            rate_info = json.load(jf)
    except Exception as e:
        logger.debug(f"Failed to read currency conversion sidecar: {e}")
        return

    src_ccy = rate_info.get('source_currency', '').upper()
    tgt_ccy = rate_info.get('target_currency', '').upper()
    rate = rate_info.get('rate')
    if not src_ccy or not tgt_ccy or not rate:
        return

    # Determine whether this invoice is denominated in the source currency.
    # Use a strict marker like "(CNY)" to avoid false positives (e.g. the word
    # "CNY" appearing in a browser tab title of a USD-denominated invoice).
    marker = f"({src_ccy})"
    if text.count(marker) < 2:
        logger.debug(f"Invoice not in {src_ccy} — skipping currency conversion")
        return

    logger.info(
        f"Applying currency conversion: {src_ccy} -> {tgt_ccy} @ {rate} "
        f"(from {os.path.basename(json_path)})"
    )
    print(f"    Currency conversion: {src_ccy} -> {tgt_ccy} @ {rate}")

    # Convert per-item prices
    for item in invoice_data.get('items', []):
        for field in ('unit_price', 'unit_cost', 'total_cost', 'line_total'):
            if field in item and item[field] is not None:
                try:
                    item[field] = round(float(item[field]) * rate, 4)
                except (TypeError, ValueError):
                    pass

    # Convert invoice-level totals
    for field in ('invoice_total', 'subtotal', 'tax', 'discount', 'freight'):
        if field in invoice_data and invoice_data.get(field):
            try:
                invoice_data[field] = round(float(invoice_data[field]) * rate, 2)
            except (TypeError, ValueError):
                pass

    invoice_data['currency_converted'] = {
        'from': src_ccy,
        'to': tgt_ccy,
        'rate': rate,
        'source': os.path.basename(json_path),
    }


def process_single_invoice(
    pdf_path: str,
    registry,                        # FormatRegistry instance
    matcher,                         # POMatcher instance (or None for non-PO path)
    rules: List[Dict],
    noise_words: set,
    supplier_db: Dict,
    output_dir: str,
    document_type: str = '7400-000',
    verbose: bool = False,
    no_declaration: bool = False,
) -> Optional[InvoiceResult]:
    """
    Process one invoice PDF through the full pipeline.

    Args:
        pdf_path: Full path to the invoice PDF
        registry: FormatRegistry for format detection
        matcher: POMatcher for PO matching (None to skip PO matching)
        rules: Classification rules list
        noise_words: Noise words set for classification
        supplier_db: Mutable supplier database dict (updated in-place)
        output_dir: Directory for output XLSX and PDF copies
        document_type: CARICOM document type (7400-000 ungrouped, 4000-000 grouped)
        verbose: Enable verbose logging

    Returns:
        InvoiceResult on success, None if no text could be extracted
    """
    pdf_file = os.path.basename(pdf_path)
    print(f"  Processing: {pdf_file}")

    # 1. Extract text from PDF
    text = extract_pdf_text(pdf_path)
    if not text.strip():
        print(f"    WARNING: No text extracted from {pdf_file}")
        return None

    # 2. Detect format and parse using FormatRegistry
    format_spec = registry.detect_format(text)
    fmt_parser = None
    format_source = 'unmatched'  # tracks how format was determined: yaml_spec, llm_auto, unmatched
    if format_spec:
        format_source = 'yaml_spec'
        from format_parser import create_parser

        # Two-pass OCR: if format has ocr_config and initial parse yields few/no items,
        # re-extract text with format-specific OCR settings and re-parse
        ocr_cfg = format_spec.get('ocr_config')
        fmt_parser = create_parser(format_spec)
        raw_result = fmt_parser.parse(text)
        invoice_data = normalize_parse_result(raw_result)

        if ocr_cfg and not invoice_data.get('items'):
            logger.info(f"Re-OCR with format config: {ocr_cfg}")
            better_text = extract_pdf_text(pdf_path, ocr_config=ocr_cfg)
            if better_text and better_text != text:
                digit_new = sum(c.isdigit() for c in better_text)
                digit_old = sum(c.isdigit() for c in text)
                if digit_new > digit_old:
                    text = better_text
                    fmt_parser = create_parser(format_spec)
                    raw_result = fmt_parser.parse(text)
                    invoice_data = normalize_parse_result(raw_result)
                    logger.info(f"Re-OCR improved: {digit_old}→{digit_new} digits, {len(invoice_data.get('items',[]))} items")
    else:
        logger.warning(f"No format matched for {pdf_file}")

        # Attempt LLM-based auto-generation of format spec
        auto_result = None
        try:
            from stages.auto_format import try_auto_generate
            auto_result = try_auto_generate(
                text=text,
                registry=registry,
                supplier_name='',
                pdf_file=pdf_file,
            )
        except Exception as e:
            logger.warning(f"Auto format generation failed: {e}")

        if auto_result:
            invoice_data, _spec_path = auto_result
            format_source = 'llm_auto'
            if not invoice_data.get('format') or invoice_data['format'] == 'unknown':
                invoice_data['format'] = 'auto_generated'
        else:
            # Fall back to original unmatched behavior
            invoice_ref = _extract_invoice_number(text) or os.path.splitext(pdf_file)[0]
            invoice_ref = re.sub(r'\s*\(\d+\)$', '', invoice_ref)
            invoice_data = {
                'invoice_num': invoice_ref,
                'invoice_date': '',
                'invoice_total': 0,
                'freight': 0,
                'po_number': '',
                'format': 'unmatched',
                'items': [],
            }

    # 2b. Apply currency conversion if a rate sidecar exists and the invoice
    # is in the source currency. The splitter extracts the rate from
    # xe.com conversion pages that ship with some invoices and writes a
    # sibling `<base>_currency_conversion.json` next to the split invoice PDFs.
    _apply_currency_conversion(pdf_path, text, invoice_data)

    print(f"    Format: {invoice_data['format']}, "
          f"Items extracted: {len(invoice_data.get('items', []))}")
    print(f"    Invoice#: {invoice_data['invoice_num']}, "
          f"Date: {invoice_data['invoice_date']}, "
          f"Total: ${invoice_data['invoice_total']:.2f}")

    # 3. Determine supplier name: invoice PDF data first, PO data as fallback
    invoice_supplier = invoice_data.get('supplier_name', '')
    po_supplier = ''
    if matcher:
        po_supplier = matcher.determine_supplier(pdf_file) or ""
    supplier_name = invoice_supplier or po_supplier
    print(f"    Supplier: {supplier_name or 'UNKNOWN'}"
          f"{' (from invoice)' if invoice_supplier else ' (from PO)'}")

    # 4. Match items against PO (if matcher provided)
    if matcher:
        matched = matcher.match_invoice(invoice_data, pdf_file)
    else:
        # No PO matching — convert items to matched format
        matched = _items_without_po(invoice_data)

    # Filter out zero-value items (back orders with no price)
    # Skip filter when the entire invoice is $0.00 — these are legitimate
    # free/promotional items (common in SHEIN) that still need declaration.
    invoice_total = invoice_data.get('invoice_total', 0) or 0
    before_filter = len(matched)
    if invoice_total > 0:
        matched = [
            m for m in matched
            if (m.get('total_cost') or 0) != 0 or (m.get('unit_price') or 0) != 0
        ]
    if len(matched) < before_filter:
        print(f"    Filtered {before_filter - len(matched)} zero-value back-order items")

    matched_count = sum(1 for m in matched
                        if m.get('match_score', 0) > 0 or m.get('po_item_ref'))
    unmatched_count = len(matched) - matched_count
    print(f"    Matched items: {len(matched)}"
          f"{f' (unmatched: {unmatched_count})' if unmatched_count else ''}")
    if unmatched_count > 0 and verbose:
        for m in matched:
            if not m.get('match_score', 0) and not m.get('po_item_ref'):
                sku = m.get('supplier_item', '?')
                desc = (m.get('supplier_item_desc') or '')[:60]
                print(f"      UNMATCHED: {sku} - {desc}")

    # 4b. Apply product_context from format spec to item descriptions
    #      This gives the classifier context when descriptions are opaque
    #      (e.g. wig model names like "Toby All Colors" → "synthetic wig: Toby All Colors")
    fmt_classification = (format_spec or {}).get('classification', {})
    product_context = fmt_classification.get('product_context', '')
    if product_context and matched:
        print(f"    Product context: {product_context}")
        for m in matched:
            desc = m.get('supplier_item_desc', '')
            if desc:
                m['supplier_item_desc'] = f"{product_context}: {desc}"
            po_desc = m.get('po_item_desc', '')
            if po_desc:
                m['po_item_desc'] = f"{product_context}: {po_desc}"

    # 5. Classify items for tariff codes
    # Build config dict to enable web/LLM classification fallback
    base_dir = os.path.dirname(PIPELINE_DIR)  # project root
    classification_config = {
        'base_dir': base_dir,
        'web_verify': {'enabled': True},
        'llm_classification': {'enabled': True},
    }
    classified_count = classify_matched_items(matched, rules, noise_words, config=classification_config)

    # 5b. Apply default classification from format spec for unclassified items
    default_code = fmt_classification.get('default_code', '')
    default_category = fmt_classification.get('default_category', '')
    if default_code:
        for m in matched:
            code = m.get('tariff_code', '00000000')
            if code == '00000000' or not code:
                from classifier import validate_and_correct_code
                m['tariff_code'] = validate_and_correct_code(default_code, base_dir)
                m['category'] = default_category or m.get('category', 'PRODUCTS')
                m['classification_source'] = 'format_default'
                classified_count += 1

    unclassified = len(matched) - classified_count
    print(f"    Classified: {classified_count}"
          f"{f' (unclassified: {unclassified})' if unclassified else ''}")

    # 6. Build supplier info: invoice PDF → suppliers.json → web search
    supplier_info = get_supplier_info(supplier_name, supplier_db, invoice_data)
    display_name = supplier_info.get('name', '') or supplier_name

    # Persist resolved supplier data back to suppliers.json (in-memory)
    update_supplier_entry(supplier_db, supplier_info['code'], supplier_info)

    # 7. Generate XLSX with Packages=1 (default, updated later by BL)
    invoice_num = invoice_data['invoice_num']
    # Fall back to PDF filename if invoice number is blank
    if not invoice_num.strip():
        invoice_num = os.path.splitext(os.path.basename(pdf_path))[0]
        invoice_data['invoice_num'] = invoice_num
    # Sanitize invoice number for safe filesystem use (replace / \ : etc.)
    safe_num = re.sub(r'[<>:"/\\|?*]', '_', invoice_num)
    # Collision detection: append _2, _3, etc. if XLSX already exists
    xlsx_path = os.path.join(output_dir, f"{safe_num}.xlsx")
    pdf_out_path = os.path.join(output_dir, f"{safe_num}.pdf")
    if os.path.exists(xlsx_path):
        suffix = 2
        while os.path.exists(os.path.join(output_dir, f"{safe_num}_{suffix}.xlsx")):
            suffix += 1
        safe_num = f"{safe_num}_{suffix}"
        xlsx_path = os.path.join(output_dir, f"{safe_num}.xlsx")
        pdf_out_path = os.path.join(output_dir, f"{safe_num}.pdf")
        print(f"    Collision: renamed to {safe_num}")
    xlsx_name = f"{safe_num}.xlsx"
    pdf_name = f"{safe_num}.pdf"

    # Skip XLSX generation and PDF copy when no items extracted —
    # the caller handles 0-item results as failures
    if not matched:
        print(f"    Skipping XLSX generation: 0 items")
        return InvoiceResult(
            pdf_file=pdf_file,
            invoice_num=invoice_num,
            invoice_data=invoice_data,
            matched_items=[],
            supplier_info=supplier_info,
            xlsx_path='',
            pdf_output_path='',
            classified_count=0,
            matched_count=0,
            format_name=invoice_data.get('format', ''),
            freight=0,
            packages=1,
        )

    # 7b. Prorate invoice-level amounts when items were filtered (e.g. skip_patterns).
    #     Only truly unshipped items (e.g. Canceled) should be skipped. Items that were
    #     physically shipped (Reported lost, Refunded) stay in the XLSX.
    #
    #     Walmart pricing:
    #       Subtotal       = pre-rollback prices for non-canceled items
    #       Savings        = rollback savings (already in displayed per-item prices)
    #       Non-canceled   = Subtotal - Savings  (sum of displayed prices, excl canceled)
    #       Tax            = calculated on non-canceled total
    #       Canceled items = excluded from Subtotal/Savings/Tax entirely
    #       Invoice Total  = Non-canceled items + Tax  (Savings is NOT a deduction)
    #
    #     Proration base = Subtotal - Savings = non-canceled items at displayed prices.
    #     Fall back to shipped + skipped total if subtotal/savings aren't available.
    item_sum = sum(m.get('total_cost', 0) or 0 for m in matched)
    skipped_total = invoice_data.get('skipped_items_total', 0) or 0
    subtotal = invoice_data.get('sub_total', 0) or 0
    savings = invoice_data.get('savings', 0) or 0
    discount = invoice_data.get('discount', 0) or 0
    # Prefer subtotal - savings as base (gives non-canceled displayed total)
    if subtotal and savings and subtotal > savings:
        proration_base = subtotal - savings
    elif subtotal and discount and subtotal > discount:
        proration_base = subtotal - discount
    else:
        proration_base = item_sum + skipped_total
    if skipped_total and item_sum and proration_base:
        ratio = item_sum / proration_base
        for field in ['tax', 'discount']:
            if invoice_data.get(field):
                invoice_data[field] = round(invoice_data[field] * ratio, 2)
        # Recalculate total from items + prorated tax - prorated discount
        invoice_data['invoice_total'] = round(
            item_sum - (invoice_data.get('discount', 0) or 0) + (invoice_data.get('tax', 0) or 0), 2)
        print(f"    Prorated for shipped items (ratio={ratio:.3f}): "
              f"total=${invoice_data['invoice_total']:.2f} "
              f"tax=${invoice_data.get('tax', 0):.2f} "
              f"discount=${invoice_data.get('discount', 0):.2f}")

    # No declaration: zero out invoice total so variance check = 0
    if no_declaration:
        invoice_data['invoice_total'] = 0
        invoice_data['total'] = 0

    generate_bl_xlsx(
        invoice_data, matched, display_name,
        supplier_info, xlsx_path,
        document_type=document_type,
    )

    # 7c. Write .meta.json sidecar for regression testing
    #     Persists classification_source per item and format provenance so the
    #     regression test can distinguish LLM-volatile changes from deterministic ones.
    meta_path = xlsx_path.rsplit('.', 1)[0] + '.meta.json'
    try:
        meta = {
            'format_name': invoice_data.get('format', ''),
            'format_source': format_source,
            'items': [
                {
                    'tariff_code': m.get('tariff_code', '00000000'),
                    'classification_source': m.get('classification_source', ''),
                    'description': (m.get('supplier_item_desc') or m.get('description') or '')[:80],
                }
                for m in matched
            ],
        }
        with open(meta_path, 'w', encoding='utf-8') as mf:
            json.dump(meta, mf, indent=2)
    except Exception as e:
        logger.debug(f"Failed to write meta.json: {e}")

    # 8. Copy source PDF to output dir renamed by invoice number
    # Skip if same file, or if source is already in the output dir (avoids duplicates)
    src_dir = os.path.dirname(os.path.abspath(pdf_path))
    out_dir = os.path.abspath(output_dir)
    if src_dir != out_dir and os.path.abspath(pdf_path) != os.path.abspath(pdf_out_path):
        shutil.copy2(pdf_path, pdf_out_path)
    elif src_dir == out_dir:
        # Source already in output dir — use original path for attachments
        pdf_out_path = pdf_path

    # 9. Verification checks (same logic as XLSX formulas)
    # ADJUSTMENTS = T(freight) + U(credits x -1) + V(tax) - W(discount + free_shipping)
    # NET TOTAL = item_sum + ADJUSTMENTS
    # VARIANCE = InvoiceTotal - NET TOTAL  (must be $0.00)
    item_cost_sum = sum(
        m.get('total_cost', 0) or (m.get('unit_price', 0) or 0) * (m.get('quantity', 1) or 1)
        for m in matched
    )
    inv_total = invoice_data.get('invoice_total', 0) or 0
    subtotal = invoice_data.get('sub_total', 0) or 0
    inv_freight = invoice_data.get('freight', 0) or 0
    tax = invoice_data.get('tax', 0) or 0
    other_cost = invoice_data.get('other_cost', 0) or 0
    discount = invoice_data.get('discount', 0) or 0
    credits = invoice_data.get('credits', 0) or 0
    free_shipping = invoice_data.get('free_shipping', 0) or 0
    total_deduction = discount + free_shipping

    # credits x -1 in Column U: adds negative to adjustments
    adjustments = inv_freight + (-credits) + tax + other_cost - total_deduction
    net_total = item_cost_sum + adjustments
    variance = round(inv_total - net_total, 2) if inv_total else 0

    if subtotal and abs(item_cost_sum - subtotal) > 0.02:
        print(f"    WARNING: Item sum ${item_cost_sum:.2f} != Subtotal ${subtotal:.2f} "
              f"(diff: ${abs(item_cost_sum - subtotal):.2f})")
    if inv_total and abs(variance) > 0.02:
        print(f"    WARNING: Variance ${variance:.2f} "
              f"(InvTotal ${inv_total:.2f} - NetTotal ${net_total:.2f})")
        _print_variance_report(matched, invoice_data, item_cost_sum, inv_total,
                               adjustments, net_total, variance)
    if credits:
        print(f"    Credits: ${credits:.2f} (customer-induced -> Col U x -1)")
    if free_shipping:
        print(f"    Free Shipping: ${free_shipping:.2f} (supplier-induced -> Col W deduction)")

    print(f"    Generated: {xlsx_name}  (Packages=1 default)")
    print()

    return InvoiceResult(
        pdf_file=pdf_file,
        invoice_num=invoice_num,
        invoice_data=invoice_data,
        matched_items=matched,
        supplier_info=supplier_info,
        xlsx_path=xlsx_path,
        pdf_output_path=pdf_out_path,
        classified_count=classified_count,
        matched_count=matched_count,
        format_name=invoice_data['format'],
        freight=invoice_data.get('freight', 0) or 0,
        packages=1,
    )


def _print_variance_report(matched: List[Dict], invoice_data: Dict,
                           item_cost_sum: float, inv_total: float,
                           adjustments: float, net_total: float,
                           variance: float) -> None:
    """Print a detailed line-by-line item report when variance is detected."""
    try:
        items = matched
        inv_num = invoice_data.get('invoice_number', '?')
        supplier = invoice_data.get('supplier_name') or invoice_data.get('supplier', '?')
        freight = invoice_data.get('freight', 0) or 0
        tax = invoice_data.get('tax', 0) or 0
        other_cost = invoice_data.get('other_cost', 0) or 0
        credits_val = invoice_data.get('credits', 0) or 0
        discount = invoice_data.get('discount', 0) or 0
        free_shipping = invoice_data.get('free_shipping', 0) or 0

        w = 110
        print()
        print("    " + "=" * w)
        print(f"    VARIANCE REPORT — Invoice {inv_num} — {supplier}")
        print("    " + "=" * w)
        print(f"    {'#':>3} | {'Description':<52} | {'Qty':>5} | {'Unit':>8} | {'Total':>10}")
        print("    " + "-" * w)

        running_sum = 0.0
        for idx, item in enumerate(items, 1):
            desc = (item.get('description') or item.get('supplier_item') or '?')[:52]
            qty = item.get('quantity', 0) or 0
            unit = item.get('unit_price', 0) or item.get('unit_cost', 0) or 0
            total = item.get('total_cost', 0) or 0
            running_sum += total
            print(f"    {idx:>3} | {desc:<52} | {qty:>5} | ${unit:>7.2f} | ${total:>9.2f}")

        print("    " + "-" * w)
        print(f"    {'':>3} | {'ITEMS SUM':<52} | {'':>5} | {'':>8} | ${item_cost_sum:>9.2f}")

        # Show adjustment breakdown
        if adjustments != 0:
            print(f"    {'':>3} | {'ADJUSTMENTS BREAKDOWN:':<52} |")
            if freight:
                print(f"    {'':>3} |   {'Freight (Col T)':<50} | {'':>5} | {'':>8} | ${freight:>9.2f}")
            if credits_val:
                print(f"    {'':>3} |   {'Credits x -1 (Col U)':<50} | {'':>5} | {'':>8} | ${-credits_val:>9.2f}")
            if tax:
                print(f"    {'':>3} |   {'Tax (Col V)':<50} | {'':>5} | {'':>8} | ${tax:>9.2f}")
            if other_cost:
                print(f"    {'':>3} |   {'Other Cost (Col V)':<50} | {'':>5} | {'':>8} | ${other_cost:>9.2f}")
            if discount:
                print(f"    {'':>3} |   {'Discount (Col W deduction)':<50} | {'':>5} | {'':>8} | ${-discount:>9.2f}")
            if free_shipping:
                print(f"    {'':>3} |   {'Free Shipping (Col W deduction)':<50} | {'':>5} | {'':>8} | ${-free_shipping:>9.2f}")
            print(f"    {'':>3} | {'ADJUSTMENTS TOTAL':<52} | {'':>5} | {'':>8} | ${adjustments:>9.2f}")

        print("    " + "-" * w)
        print(f"    {'':>3} | {'NET TOTAL (Items + Adjustments)':<52} | {'':>5} | {'':>8} | ${net_total:>9.2f}")
        print(f"    {'':>3} | {'INVOICE TOTAL':<52} | {'':>5} | {'':>8} | ${inv_total:>9.2f}")
        print(f"    {'':>3} | {'VARIANCE (InvTotal - NetTotal)':<52} | {'':>5} | {'':>8} | ${variance:>9.2f}")
        print("    " + "=" * w)

        # Hint about likely cause
        if abs(variance) > 0.50:
            subtotal = invoice_data.get('sub_total', 0) or 0
            if subtotal and abs(item_cost_sum - subtotal) > 0.02:
                print(f"    LIKELY CAUSE: Item sum ${item_cost_sum:.2f} != PDF subtotal "
                      f"${subtotal:.2f} — missing or mis-parsed items")
            elif abs(item_cost_sum - inv_total) < abs(variance):
                print(f"    LIKELY CAUSE: Adjustment values may be incorrect "
                      f"(freight/tax/credits/discount extraction)")
            else:
                print(f"    LIKELY CAUSE: Items sum ${item_cost_sum:.2f} differs from "
                      f"invoice total ${inv_total:.2f} by ${inv_total - item_cost_sum:.2f} "
                      f"— check format spec extraction")
        print()
    except Exception as e:
        logger.debug(f"Variance report failed: {e}")


def _items_without_po(invoice_data: Dict) -> List[Dict]:
    """Convert invoice items to PO-matched format without actual PO matching."""
    matched = []
    for idx, item in enumerate(invoice_data.get('items', [])):
        supplier_item = (item.get('supplier_item') or
                         item.get('sku') or
                         f'ITEM-{idx + 1:03d}')
        qty = item.get('quantity', 0) or 0
        unit = item.get('unit_price') or item.get('unit_cost') or 0
        total = item.get('total_cost') or item.get('total') or item.get('amount') or 0
        # Derive unit_price from total_cost if missing
        if not unit and total and qty:
            unit = round(total / qty, 2)
        matched.append({
            'po_item_ref': '',
            'po_item_desc': '',
            'po_number': '',
            'supplier_item': supplier_item,
            'supplier_item_desc': item.get('description', ''),
            'quantity': qty,
            'unit_price': unit,
            'total_cost': total,
            'uom': '',
            'match_score': 0,
        })
    return matched
