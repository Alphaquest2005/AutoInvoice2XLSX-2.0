#!/usr/bin/env python3
"""
BL (Bill of Lading) XLSX Generator

Generates CARICOM-format XLSX files for BL shipment processing.
All column definitions, styles, formulas, and totals structure are loaded
dynamically from config/columns.yaml and config/grouping.yaml via spec_loader.

Supports document type configuration from config/document_types.json:
  - grouping=true  (e.g. 4000-000): group header + detail row pairs
  - grouping=false (e.g. 7400-000): one row per item, no grouping

Usage:
    from bl_xlsx_generator import generate_bl_xlsx
    generate_bl_xlsx(invoice_data, matched_items, supplier_name,
                     supplier_info, output_path, document_type="7400-000")
"""

import json
import os
import logging
import re
from typing import Any, Dict, List, Optional

try:
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.comments import Comment
    from openpyxl.utils import get_column_letter
except ImportError:
    openpyxl = None
    Comment = None

logger = logging.getLogger(__name__)

import sys
import sqlite3
_pipeline_dir = os.path.dirname(os.path.abspath(__file__))
if _pipeline_dir not in sys.path:
    sys.path.insert(0, _pipeline_dir)

from spec_loader import get_spec

# ─── Load spec (singleton) ────────────────────────────────
_spec = get_spec()

# ─── Build openpyxl style objects from spec ────────────────
def _build_styles():
    """Build openpyxl Font/Fill/Border objects from spec config."""
    hs = _spec.header_style
    gs = _spec.group_style
    ds = _spec.detail_style

    header_fill = PatternFill(
        start_color=hs.get('fill_color', '4472C4'),
        end_color=hs.get('fill_color', '4472C4'),
        fill_type='solid'
    )
    header_font = Font(
        bold=hs.get('font_bold', True),
        color=hs.get('font_color', 'FFFFFF'),
        size=hs.get('font_size', 10)
    )
    group_fill = PatternFill(
        start_color=gs.get('fill_color', 'D9E1F2'),
        end_color=gs.get('fill_color', 'D9E1F2'),
        fill_type=gs.get('fill_type', 'solid')
    )
    group_font = Font(
        bold=gs.get('font_bold', True),
        size=gs.get('font_size', 11)
    )
    detail_font = Font(
        bold=ds.get('font_bold', False),
        size=ds.get('font_size', 10)
    )
    bold_font = Font(
        bold=True,
        size=_spec.totals_default_formatting.get('font_size', 11)
    )
    border = Border(
        left=Side(style=_spec.border_style),
        right=Side(style=_spec.border_style),
        top=Side(style=_spec.border_style),
        bottom=Side(style=_spec.border_style),
    )
    return header_fill, header_font, group_fill, group_font, detail_font, bold_font, border

HEADER_FILL, HEADER_FONT, GROUP_FILL, GROUP_FONT, DETAIL_FONT, BOLD_FONT, THIN_BORDER = _build_styles()

# Uncertainty markers — applied to cells whose value was recovered from OCR
# via the orphan-price scan or absorbed into ADJUSTMENTS.  Visible so a
# reviewer immediately sees which numbers are reconstructed vs directly
# extracted from the invoice text.
UNCERTAIN_FILL = PatternFill(
    start_color='FFEBCC', end_color='FFEBCC', fill_type='solid'
)
RECOVERED_FILL = PatternFill(
    start_color='FFF2CC', end_color='FFF2CC', fill_type='solid'
)


# ─── CET description & rate cache ──────────────────────────
_cet_desc_cache: Dict[str, str] = {}
_cet_rate_cache: Dict[str, Optional[float]] = {}
_cet_db_loaded = False

# XCD exchange rate (Eastern Caribbean Dollar)
XCD_RATE = 2.7169
# Fixed ASYCUDA duty rates
CSC_RATE = 0.06   # Customs Service Charge: 6%
VAT_RATE = 0.15   # Value Added Tax: 15%

def _parse_cet_rate(rate_str: Optional[str]) -> Optional[float]:
    """Parse a duty_rate string from the CET database into a float (0.0–1.0).
    Returns None if unparseable."""
    if not rate_str:
        return None
    rate_str = rate_str.strip()
    if rate_str.lower() == 'free':
        return 0.0
    # Letter categories (A, C, D) — cannot resolve to a numeric rate
    if rate_str.isalpha():
        return None
    # Range like "0-5%" — take the upper bound as conservative estimate
    if '-' in rate_str and '%' in rate_str:
        try:
            upper = rate_str.replace('%', '').split('-')[1].strip()
            return float(upper) / 100.0
        except (ValueError, IndexError):
            return None
    # Decimal rate (0.2 = 20%, 0.05 = 5%)
    try:
        val = float(rate_str)
        # Values > 1 are percentages (e.g. "20"), values <= 1 are already ratios
        return val if val <= 1.0 else val / 100.0
    except ValueError:
        return None

def _load_cet_descriptions() -> None:
    """Load CET code descriptions and duty rates from the database."""
    global _cet_db_loaded
    if _cet_db_loaded:
        return
    db_path = os.path.join(os.path.dirname(_pipeline_dir), 'data', 'cet.db')
    if not os.path.exists(db_path):
        logger.warning(f"[CET] Database not found: {db_path}")
        _cet_db_loaded = True
        return
    try:
        conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True)
        rows = conn.execute('SELECT hs_code, description, duty_rate FROM cet_codes').fetchall()
        for code, desc, rate in rows:
            if desc:
                _cet_desc_cache[code] = desc
            _cet_rate_cache[code] = _parse_cet_rate(rate)
        conn.close()
        logger.info(f"[CET] Loaded {len(_cet_desc_cache)} descriptions, {sum(1 for v in _cet_rate_cache.values() if v is not None)} rates from database")
        _cet_db_loaded = True
    except Exception as e:
        logger.warning(f"[CET] Read-only connect failed: {e}, trying temp copy")
        try:
            import shutil, tempfile
            tmp = os.path.join(tempfile.gettempdir(), 'cet_readonly.db')
            shutil.copy2(db_path, tmp)
            conn = sqlite3.connect(tmp)
            rows = conn.execute('SELECT hs_code, description, duty_rate FROM cet_codes').fetchall()
            for code, desc, rate in rows:
                if desc:
                    _cet_desc_cache[code] = desc
                _cet_rate_cache[code] = _parse_cet_rate(rate)
            conn.close()
            logger.info(f"[CET] Loaded {len(_cet_desc_cache)} descriptions from temp copy")
            _cet_db_loaded = True
        except Exception as e2:
            logger.error(f"[CET] Temp copy also failed: {e2}")

def get_cet_category(tariff_code: str) -> str:
    """Look up the CET description for a tariff code. Falls back to heading level."""
    _load_cet_descriptions()
    if not tariff_code or tariff_code == '00000000':
        return ''
    _GENERIC = {'OTHER', 'OTHER:', 'VIRGIN', 'NONE', ''}

    def _is_useful(desc: str) -> bool:
        cleaned = desc.strip().rstrip(':').strip()
        if cleaned.upper() in _GENERIC:
            return False
        if re.match(r'^Of\s+\w+$', cleaned):
            return False
        if len(cleaned) < 3:
            return False
        if cleaned.upper().startswith('INVALID:'):
            return False
        return True

    def _clean(desc: str) -> str:
        if not desc:
            return ''
        desc = re.sub(r'[\n\r\t]+', ' ', desc).strip()
        desc = re.sub(r' {2,}', ' ', desc)
        if desc.upper().startswith('CATEGORY:'):
            desc = desc[9:].strip()
        desc = desc.rstrip(':').strip()
        for sep in [' - ', '; ', ' (see ']:
            if sep in desc:
                desc = desc.split(sep)[0].strip()
        if len(desc) > 80:
            desc = desc[:77] + '...'
        return desc

    if tariff_code in _cet_desc_cache:
        desc = _cet_desc_cache[tariff_code]
        if _is_useful(desc):
            return _clean(desc)
    heading = tariff_code[:4] + '0000'
    if heading in _cet_desc_cache:
        desc = _cet_desc_cache[heading]
        if _is_useful(desc):
            return _clean(desc)
    chapter = tariff_code[:2] + '000000'
    if chapter in _cet_desc_cache:
        desc = _cet_desc_cache[chapter]
        if _is_useful(desc):
            return _clean(desc)
    prefix = tariff_code[:4]
    for code, desc in _cet_desc_cache.items():
        if code.startswith(prefix) and desc and _is_useful(desc):
            return _clean(desc)
    return ''


def get_cet_rate(tariff_code: str) -> Optional[float]:
    """Look up the CET duty rate for a tariff code. Returns rate as 0.0–1.0.
    Falls back through heading → chapter → prefix. Returns 0.20 default for
    consumer goods if not found (most personal imports are 20% CET)."""
    _load_cet_descriptions()
    if not tariff_code or tariff_code == '00000000':
        return 0.20  # default consumer goods rate

    # Exact match
    rate = _cet_rate_cache.get(tariff_code)
    if rate is not None:
        return rate

    # Heading level (first 4 digits + 0000)
    heading = tariff_code[:4] + '0000'
    rate = _cet_rate_cache.get(heading)
    if rate is not None:
        return rate

    # Chapter level (first 2 digits + 000000)
    chapter = tariff_code[:2] + '000000'
    rate = _cet_rate_cache.get(chapter)
    if rate is not None:
        return rate

    # Prefix search — find any code with same 4-digit prefix that has a rate
    prefix = tariff_code[:4]
    for code, r in _cet_rate_cache.items():
        if code.startswith(prefix) and r is not None:
            return r

    # Default: 20% for consumer goods (most common for personal imports)
    return 0.20


def calculate_duties(cif_usd: float, cet_rate: float,
                     customs_freight: float = 0, insurance: float = 0) -> Dict:
    """Calculate ASYCUDA duties for a given CIF value.

    Returns dict with: cif_usd, cif_xcd, cet, csc, vat, total_duties,
    effective_rate, cet_rate_used.
    """
    cif_total_usd = round(cif_usd + customs_freight + insurance, 2)
    cif_xcd = round(cif_total_usd * XCD_RATE, 2)

    cet = round(cif_xcd * cet_rate, 2)
    csc = round(cif_xcd * CSC_RATE, 2)
    vat = round((cif_xcd + cet + csc) * VAT_RATE, 2)
    total_duties = round(cet + csc + vat, 2)
    effective_rate = total_duties / cif_xcd if cif_xcd else 0

    return {
        'cif_usd': cif_total_usd,
        'cif_xcd': cif_xcd,
        'cet_rate': cet_rate,
        'cet': cet,
        'csc_rate': CSC_RATE,
        'csc': csc,
        'vat_rate': VAT_RATE,
        'vat': vat,
        'total_duties': total_duties,
        'effective_rate': effective_rate,
    }


def _normalize_date(date_str: str) -> str:
    """Normalize date string to the format specified in columns.yaml (YYYY-MM-DD)."""
    if not date_str or not isinstance(date_str, str):
        return date_str or ''
    date_str = date_str.strip()
    # Already in YYYY-MM-DD
    if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        return date_str
    from datetime import datetime
    # Try common date formats, output as YYYY-MM-DD per spec
    for fmt in (
        '%m/%d/%Y',        # 01/16/2026
        '%M/%d/%Y',        # handle edge case
        '%B %d, %Y',       # January 16, 2026
        '%b %d, %Y',       # Jan 16, 2026
        '%B %d %Y',        # January 16 2026
        '%b %d %Y',        # Jan 16 2026
        '%d-%m-%Y',        # 16-01-2026
        '%m-%d-%Y',        # 01-16-2026
        '%d %B %Y',        # 16 January 2026
        '%d %b %Y',        # 16 Jan 2026
    ):
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue
    # Handle M/D/YYYY, M/DD/YYYY, MM/D/YYYY
    m = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', date_str)
    if m:
        from datetime import datetime
        try:
            dt = datetime(int(m.group(3)), int(m.group(1)), int(m.group(2)))
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            pass
    return date_str


def _resolve_value(template: str, context: dict) -> Any:
    """Resolve a ${...} template string against a context dict.
    Returns the resolved value, or None if the template is null/empty.
    Handles prefix+template patterns like "    ${supplier_item_desc}".
    """
    if template is None:
        return None
    if not isinstance(template, str):
        return template
    if template.startswith('${'):
        # Simple template: ${key}
        key = template[2:-1] if template.endswith('}') else template[2:]
        return context.get(key)
    # Check for prefix + ${key} pattern (e.g. "    ${supplier_item_desc}")
    idx = template.find('${')
    if idx > 0:
        prefix = template[:idx]
        rest = template[idx:]
        key = rest[2:-1] if rest.endswith('}') else rest[2:]
        val = context.get(key, '')
        return f"{prefix}{val}"
    return template  # Literal value like "4000-000" or "USD"


def load_document_type_config(document_type: str) -> Dict:
    """Load document type settings from config/document_types.json."""
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    config_path = os.path.join(base_dir, 'config', 'document_types.json')

    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            doc_types = config.get('document_types', {})
            if document_type in doc_types:
                return doc_types[document_type]
            logger.warning(f"Document type '{document_type}' not found in config, "
                           f"using default settings")
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to load document_types.json: {e}")

    return {'grouping': document_type == '4000-000', 'description': 'Unknown'}


def generate_bl_xlsx(
    invoice_data: Dict,
    matched_items: List[Dict],
    supplier_name: str,
    supplier_info: Dict,
    output_path: str,
    document_type: str = "7400-000",
    reference_items: Optional[List[Dict]] = None,
    reference_label: str = "Items on other declaration(s)",
    reference_adjustments: Optional[Dict] = None,
) -> str:
    """Generate a CARICOM-format XLSX file for one BL invoice.

    Args:
        reference_items: Optional items from other declarations in the same
            invoice.  Rendered as a read-only reference section below the
            totals so the reviewer can see the full invoice and verify
            that the split is correct.  These items are NOT included in
            the subtotals or variance check.
        reference_label: Header label for the reference section.
    """
    if not openpyxl:
        raise ImportError("openpyxl is required. Run: pip install openpyxl")

    doc_config = load_document_type_config(document_type)
    grouping = doc_config.get('grouping', False)

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Invoice Data"

    _write_headers(ws)

    if not matched_items:
        # Even with no main items, write reference section if provided
        # (e.g. declaration with no items split to it but needs full invoice context)
        if reference_items:
            _write_reference_section(ws, reference_items, reference_label,
                                     invoice_data, matched_items,
                                     reference_adjustments)
            for col_idx, width in _spec.column_widths.items():
                ws.column_dimensions[get_column_letter(col_idx)].width = width
        wb.save(output_path)
        return output_path

    if grouping:
        row_num = _write_items_grouped(ws, matched_items, invoice_data,
                                       supplier_name, supplier_info,
                                       document_type)
        _write_subtotals_grouped(ws, row_num, len(matched_items), invoice_data)
    else:
        row_num = _write_items_ungrouped(ws, matched_items, invoice_data,
                                         supplier_name, supplier_info,
                                         document_type)
        _write_subtotals_ungrouped(ws, row_num, len(matched_items), invoice_data)

    # Duty estimation section — classification cross-check
    _write_duty_estimation_section(ws, matched_items, invoice_data)

    # Reference items section — appended after totals for context
    if reference_items:
        _write_reference_section(ws, reference_items, reference_label,
                                 invoice_data, matched_items,
                                 reference_adjustments)

    # Column widths from spec
    for col_idx, width in _spec.column_widths.items():
        ws.column_dimensions[get_column_letter(col_idx)].width = width

    ws.freeze_panes = "A2"

    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    wb.save(output_path)
    logger.info(f"Generated BL XLSX: {output_path}")

    return output_path


# ─── Ungrouped Mode (e.g. 7400-000) ──────────────────────

def _write_items_ungrouped(
    ws, matched_items: List[Dict], invoice_data: Dict,
    supplier_name: str, supplier_info: Dict, document_type: str,
) -> int:
    """Write one row per item — no group/detail pairs."""
    row_num = 2
    col_count = _spec.col_count()

    # Pre-resolve column indices
    COL_F = _spec.col_index('F')
    COL_Q = _spec.col_index('Q')
    COL_R = _spec.col_index('R')
    COL_AK = _spec.col_index('AK')

    q_formula = _spec.formula_spec('Q')  # "=O{row}*K{row}"
    r_formula = _spec.formula_spec('R')  # "=P{row}-Q{row}"

    for idx, item in enumerate(matched_items):
        tariff_code = str(item.get('tariff_code', '00000000'))
        cet_desc = get_cet_category(tariff_code)
        category = cet_desc or _spec.category_name(tariff_code) or item.get('category', '')

        # Build context for value resolution
        ctx = _build_item_context(item, invoice_data, supplier_name, supplier_info,
                                  document_type, tariff_code, category,
                                  group_label=None, group_qty=None, group_total_cost=None)

        # Write columns using spec
        for letter, cfg in _spec._col_list:
            col_idx = cfg['index']
            populate_on = cfg.get('populate_on')

            # First-row-only columns
            if populate_on == 'first_group_per_invoice' and row_num != 2:
                continue

            # Formula columns
            formula = cfg.get('formula')
            if formula:
                ws.cell(row=row_num, column=col_idx, value=formula.format(row=row_num))
                continue

            # Static value (e.g., "USD")
            static = cfg.get('value')
            if static and not static.startswith('${'):
                ws.cell(row=row_num, column=col_idx, value=static)
                continue

            # For ungrouped, use detail_value if available, otherwise group_value
            val_template = cfg.get('detail_value') or cfg.get('group_value') or static
            if val_template is None:
                # Check default
                default = cfg.get('default')
                if default is not None and (populate_on != 'first_group_per_invoice' or row_num == 2):
                    ws.cell(row=row_num, column=col_idx, value=default)
                continue

            val = _resolve_value(val_template, ctx)
            if val is not None:
                ws.cell(row=row_num, column=col_idx, value=val)

        ws.cell(row=row_num, column=COL_AK, value=tariff_code)

        # Apply styling
        for col in range(1, col_count + 1):
            cell = ws.cell(row=row_num, column=col)
            cell.font = DETAIL_FONT
            cell.border = THIN_BORDER
        for col in _spec.currency_columns_all:
            ws.cell(row=row_num, column=col).number_format = _spec.currency_format

        # Uncertainty marker for orphan-price recovered items (ungrouped).
        dq = (item.get('data_quality') or '').strip()
        if dq and Comment is not None:
            col_o = _spec.col_index('O')
            col_p = _spec.col_index('P')
            for col_idx in (col_o, col_p):
                cell = ws.cell(row=row_num, column=col_idx)
                cell.fill = RECOVERED_FILL
                cell.comment = Comment(
                    f"Data quality: {dq}\n"
                    f"This value was recovered from OCR text via the "
                    f"orphan-price scan. Review and correct if wrong.",
                    "AutoInvoice",
                )

        row_num += 1

    return row_num


# ─── Grouped Mode (e.g. 4000-000) ────────────────────────

def _write_items_grouped(
    ws, matched_items: List[Dict], invoice_data: Dict,
    supplier_name: str, supplier_info: Dict, document_type: str,
) -> int:
    """Write group row + detail rows, grouped by tariff code."""
    from collections import OrderedDict

    groups = OrderedDict()
    for item in matched_items:
        tariff_code = str(item.get('tariff_code', '00000000'))
        groups.setdefault(tariff_code, []).append(item)

    row_num = 2
    is_first_row = True
    col_count = _spec.col_count()

    COL_AK = _spec.col_index('AK')

    q_formula = _spec.formula_spec('Q')
    r_formula = _spec.formula_spec('R')

    for tariff_code, group_items in groups.items():
        group_qty = sum(it['quantity'] for it in group_items)
        group_total_cost = sum(it['total_cost'] for it in group_items)
        first_item = group_items[0]
        n_items = len(group_items)

        # Category: spec mappings -> CET -> item category
        spec_cat = _spec.category_name(tariff_code)
        cet_desc = get_cet_category(tariff_code)
        category = spec_cat if spec_cat != _spec.category_default else (cet_desc or first_item.get('category', '') or spec_cat)

        # Build group label
        group_desc = first_item['supplier_item_desc']
        if n_items > 1:
            group_label = f"{group_desc} (+{n_items - 1} more) ({n_items} items)"
        else:
            group_label = f"{group_desc} (1 items)"

        # Average cost — full precision per spec (NO rounding)
        average_unit_cost = group_total_cost / group_qty if group_qty else ''

        # Build context
        ctx = _build_item_context(first_item, invoice_data, supplier_name, supplier_info,
                                  document_type, tariff_code, category,
                                  group_label=group_label, group_qty=group_qty,
                                  group_total_cost=group_total_cost)
        ctx['average_unit_cost'] = average_unit_cost
        ctx['sum_quantity'] = group_qty
        ctx['sum_total_cost'] = group_total_cost

        # ── GROUP ROW ──
        for letter, cfg in _spec._col_list:
            col_idx = cfg['index']
            populate_on = cfg.get('populate_on')

            # First-row-only columns
            if populate_on == 'first_group_per_invoice' and not is_first_row:
                continue

            # Formula columns — use spec formula for group rows too
            formula = cfg.get('formula')
            if formula:
                ws.cell(row=row_num, column=col_idx, value=formula.format(row=row_num))
                continue

            # Static value
            static = cfg.get('value')
            if static and not static.startswith('${'):
                ws.cell(row=row_num, column=col_idx, value=static)
                continue

            # Group value
            val_template = cfg.get('group_value') or static
            if val_template is None:
                default = cfg.get('default')
                if default is not None and (populate_on != 'first_group_per_invoice' or is_first_row):
                    ws.cell(row=row_num, column=col_idx, value=default)
                continue

            val = _resolve_value(val_template, ctx)
            if val is not None:
                ws.cell(row=row_num, column=col_idx, value=val)

        ws.cell(row=row_num, column=COL_AK, value=tariff_code)

        if is_first_row:
            is_first_row = False

        # Apply group row styling
        for col in range(1, col_count + 1):
            cell = ws.cell(row=row_num, column=col)
            cell.fill = GROUP_FILL
            cell.font = GROUP_FONT
            cell.border = THIN_BORDER
        for col in _spec.currency_columns_all:
            ws.cell(row=row_num, column=col).number_format = _spec.currency_format
        row_num += 1

        # ── DETAIL ROWS ──
        for item in group_items:
            detail_ctx = {
                'tariff_code': tariff_code,
                'supplier_item': item['supplier_item'],
                'supplier_item_desc': item['supplier_item_desc'],
                'item_quantity': item['quantity'],
                'unit_price': item['unit_price'],
                'item_total_cost': item['total_cost'],
                'uom': item.get('uom', 'Unit'),
            }

            detail_blank = set(_spec.detail_blank_columns())

            for letter, cfg in _spec._col_list:
                col_idx = cfg['index']

                # Skip columns that must be blank on detail rows
                if letter in detail_blank:
                    continue

                # Formula columns
                formula = cfg.get('formula')
                if formula:
                    ws.cell(row=row_num, column=col_idx, value=formula.format(row=row_num))
                    continue

                # Static value
                static = cfg.get('value')
                if static and not static.startswith('${'):
                    ws.cell(row=row_num, column=col_idx, value=static)
                    continue

                # Detail value
                val_template = cfg.get('detail_value')
                if val_template is None:
                    continue

                val = _resolve_value(val_template, detail_ctx)
                if val is not None:
                    ws.cell(row=row_num, column=col_idx, value=val)

            # Apply detail styling
            for col in range(1, col_count + 1):
                cell = ws.cell(row=row_num, column=col)
                cell.font = DETAIL_FONT
                cell.border = THIN_BORDER
            for col in _spec.currency_columns_detail:
                ws.cell(row=row_num, column=col).number_format = _spec.currency_format

            # Uncertainty marker: if this item was recovered from OCR by
            # the orphan-price scanner, fill its P (total_cost) and O
            # (unit_cost) cells with the RECOVERED_FILL and attach a
            # comment so a reviewer can see it at a glance.
            dq = (item.get('data_quality') or '').strip()
            if dq and Comment is not None:
                col_o = _spec.col_index('O')
                col_p = _spec.col_index('P')
                for col_idx in (col_o, col_p):
                    cell = ws.cell(row=row_num, column=col_idx)
                    cell.fill = RECOVERED_FILL
                    cell.comment = Comment(
                        f"Data quality: {dq}\n"
                        f"This value was recovered from OCR text via the "
                        f"orphan-price scan. Review and correct if wrong.",
                        "AutoInvoice",
                    )
            row_num += 1

    return row_num


def _build_item_context(item, invoice_data, supplier_name, supplier_info,
                        document_type, tariff_code, category,
                        group_label=None, group_qty=None, group_total_cost=None) -> dict:
    """Build a context dict for resolving ${...} templates in column specs."""
    # Invoice-level fields
    insurance = invoice_data.get('insurance', 0)
    credits = invoice_data.get('credits', 0)
    insurance_or_credits = insurance if insurance else (-credits if credits else 0)

    tax = invoice_data.get('tax', 0) or 0
    other_cost = invoice_data.get('other_cost', 0) or 0
    tax_plus_other_cost = tax + other_cost

    discount = invoice_data.get('discount', 0) or 0
    free_shipping = invoice_data.get('free_shipping', 0) or 0
    discount_plus_free_shipping = discount + free_shipping

    return {
        # Document
        'document_type': document_type,
        # Invoice
        'invoice_number': str(invoice_data.get('invoice_num', '')),
        'invoice_date': _normalize_date(invoice_data.get('invoice_date', '')),
        'invoice_total': invoice_data.get('invoice_total', 0),
        # Category
        'category_name': category,
        'tariff_code': tariff_code,
        # PO fields
        'po_number': item.get('po_number', ''),
        'po_item_ref': item.get('po_item_ref', ''),
        'po_item_desc': item.get('po_item_desc', ''),
        # Supplier item
        'supplier_item': item.get('supplier_item', ''),
        'supplier_item_desc': item.get('supplier_item_desc', ''),
        # Quantities and costs
        'item_quantity': item.get('quantity', 0),
        'unit_price': item.get('unit_price', 0),
        'item_total_cost': item.get('total_cost', 0),
        'uom': item.get('uom', 'Unit'),
        # Group aggregates
        'group_label': group_label or '',
        'sum_quantity': group_qty,
        'sum_total_cost': group_total_cost,
        'average_unit_cost': (group_total_cost / group_qty) if group_qty else '',
        # Invoice-level costs
        'freight': invoice_data.get('freight', 0) or 0,
        'insurance_or_credits': insurance_or_credits if insurance_or_credits else 0,
        'tax_plus_other_cost': tax_plus_other_cost if tax_plus_other_cost else 0,
        'discount_plus_free_shipping': discount_plus_free_shipping if discount_plus_free_shipping else 0,
        'packages': 1,
        # Supplier info
        'supplier_code': supplier_info.get('code', ''),
        'supplier_name': supplier_name,
        'supplier_address': supplier_info.get('address', ''),
        'country_code': supplier_info.get('country', 'US'),
    }


# ─── Headers ─────────────────────────────────────────────

def _write_headers(ws) -> None:
    """Write column headers from spec."""
    hs = _spec.header_style
    for col_idx, header in enumerate(_spec.headers, 1):
        cell = ws.cell(row=1, column=col_idx, value=header)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = Alignment(
            horizontal=hs.get('alignment', 'center'),
            wrap_text=hs.get('wrap_text', True)
        )
        cell.border = THIN_BORDER


# ─── Subtotals (Ungrouped) ───────────────────────────────

def _write_subtotals_ungrouped(ws, row_num: int, item_count: int,
                                invoice_data: Dict = None) -> None:
    """Write subtotals for ungrouped mode — no group verification needed."""
    if item_count == 0:
        return

    first_row = 2
    last_item_row = first_row + item_count - 1
    col_count = _spec.col_count()
    label_col = _spec.ungrouped_totals_label_column
    COL_P = _spec.col_index('P')

    p_refs = "+".join([f"P{first_row + i}" for i in range(item_count)])

    totals_rows = _spec.ungrouped_totals_rows
    row_refs = {}  # label -> row_num for formula cross-references

    for row_cfg in totals_rows:
        label = row_cfg.get('label', '')
        formula_p = row_cfg.get('column_P', '')

        ws.cell(row=row_num, column=label_col, value=label)

        # Resolve formula with references
        resolved = formula_p
        if '{all_P_refs}' in resolved:
            resolved = resolved.replace('{all_P_refs}', p_refs)
        if '{first_row}' in resolved:
            resolved = resolved.replace('{first_row}', str(first_row))
        if '{subtotal_row}' in resolved:
            resolved = resolved.replace('{subtotal_row}', str(row_refs.get('SUBTOTAL', row_num)))
        if '{adjustments_row}' in resolved:
            resolved = resolved.replace('{adjustments_row}', str(row_refs.get('ADJUSTMENTS', row_num)))
        if '{net_total_row}' in resolved:
            resolved = resolved.replace('{net_total_row}', str(row_refs.get('NET TOTAL', row_num)))

        if resolved:
            ws.cell(row=row_num, column=COL_P, value=resolved)

        row_refs[label] = row_num

        # Style
        for col in range(1, col_count + 1):
            cell = ws.cell(row=row_num, column=col)
            cell.font = BOLD_FONT
            cell.border = THIN_BORDER
        ws.cell(row=row_num, column=COL_P).number_format = _spec.currency_format

        row_num += 1

    # INVOICE NOTES row for ungrouped mode (see grouped writer for rationale)
    if invoice_data:
        notes = invoice_data.get('data_quality_notes') or []
        uncertain = bool(invoice_data.get('invoice_total_uncertain'))
        if notes or uncertain:
            row_num += 1
            note_text = ' | '.join(notes) if notes else 'Invoice total uncertain'
            label_cell = ws.cell(
                row=row_num, column=label_col,
                value=f"INVOICE NOTES: {note_text}"
            )
            label_cell.font = Font(bold=True, size=BOLD_FONT.size, color='9C5700')
            label_cell.fill = UNCERTAIN_FILL
            for col in range(1, col_count + 1):
                cell = ws.cell(row=row_num, column=col)
                if col != label_col:
                    cell.fill = UNCERTAIN_FILL
                cell.border = THIN_BORDER
            row_num += 1


# ─── Subtotals (Grouped) ─────────────────────────────────

def _write_subtotals_grouped(ws, row_num: int, item_count: int,
                              invoice_data: Dict = None) -> None:
    """Write subtotals for grouped mode from spec totals_section."""
    if item_count == 0:
        return

    first_row = 2
    last_data_row = row_num - 1
    col_count = _spec.col_count()
    label_col = _spec.totals_label_column
    COL_P = _spec.col_index('P')
    COL_Q = _spec.col_index('Q')

    # Identify group vs detail rows by fill color
    group_fill_color = _spec.group_style.get('fill_color', 'D9E1F2')
    group_rows = []
    detail_rows = []
    for r in range(2, row_num):
        fill = ws.cell(row=r, column=1).fill
        is_group = (fill and fill.start_color and fill.start_color.rgb
                    and group_fill_color in str(fill.start_color.rgb))
        if is_group:
            group_rows.append(r)
        else:
            detail_rows.append(r)

    group_p_refs = "+".join([f"P{r}" for r in group_rows]) if group_rows else "0"
    group_q_refs = "+".join([f"Q{r}" for r in group_rows]) if group_rows else "0"

    # Get invoice-level values for the totals section
    freight_val = invoice_data.get('freight', 0) or 0 if invoice_data else 0
    insurance = invoice_data.get('insurance', 0) or 0 if invoice_data else 0
    credits_val = invoice_data.get('credits', 0) or 0 if invoice_data else 0
    insurance_val = insurance if insurance else (-credits_val if credits_val else 0)
    tax = invoice_data.get('tax', 0) or 0 if invoice_data else 0
    other_cost = invoice_data.get('other_cost', 0) or 0 if invoice_data else 0
    other_cost_val = tax + other_cost
    discount = invoice_data.get('discount', 0) or 0 if invoice_data else 0
    free_shipping = invoice_data.get('free_shipping', 0) or 0 if invoice_data else 0
    deduction_val = discount + free_shipping
    invoice_total = invoice_data.get('invoice_total', 0) if invoice_data else 0

    row_refs = {}  # label -> row_num

    for row_cfg in _spec.totals_rows:
        # Handle blank rows
        if row_cfg.get('type') == 'blank_row':
            row_num += 1
            continue

        label = row_cfg.get('label', '')
        formula_p = row_cfg.get('column_P', '')
        formula_q = row_cfg.get('column_Q', '')
        formatting = row_cfg.get('formatting', {})

        ws.cell(row=row_num, column=label_col, value=label)

        # Resolve P formula
        resolved_p = _resolve_totals_formula(
            formula_p, first_row, last_data_row,
            group_p_refs, group_q_refs, row_refs,
            freight_val, insurance_val, other_cost_val, deduction_val,
            invoice_total, 'P'
        )
        if resolved_p is not None:
            ws.cell(row=row_num, column=COL_P, value=resolved_p)

        # Resolve Q formula
        if formula_q:
            resolved_q = _resolve_totals_formula(
                formula_q, first_row, last_data_row,
                group_p_refs, group_q_refs, row_refs,
                freight_val, insurance_val, other_cost_val, deduction_val,
                invoice_total, 'Q'
            )
            if resolved_q is not None:
                ws.cell(row=row_num, column=COL_Q, value=resolved_q)

        row_refs[label] = row_num

        # Apply formatting
        font_color = formatting.get('font_color')
        font_bold = formatting.get('font_bold', True)

        for col in range(1, col_count + 1):
            cell = ws.cell(row=row_num, column=col)
            if font_color:
                cell.font = Font(bold=font_bold, size=BOLD_FONT.size, color=font_color)
            else:
                cell.font = BOLD_FONT
            cell.border = THIN_BORDER

        ws.cell(row=row_num, column=COL_P).number_format = _spec.currency_format
        ws.cell(row=row_num, column=COL_Q).number_format = _spec.currency_format

        row_num += 1

    # INVOICE NOTES row — appended when the invoice carries data_quality
    # notes (orphan recovery, uncertain totals, variance absorbed into
    # ADJUSTMENTS).  Visible marker so reviewers can see AT A GLANCE that
    # this block has reconstructed numbers.
    if invoice_data:
        notes = invoice_data.get('data_quality_notes') or []
        uncertain = bool(invoice_data.get('invoice_total_uncertain'))
        if notes or uncertain:
            row_num += 1  # blank row for readability
            note_text = ' | '.join(notes) if notes else 'Invoice total uncertain'
            label_cell = ws.cell(
                row=row_num, column=label_col,
                value=f"INVOICE NOTES: {note_text}"
            )
            label_cell.font = Font(bold=True, size=BOLD_FONT.size, color='9C5700')
            label_cell.fill = UNCERTAIN_FILL
            for col in range(1, col_count + 1):
                cell = ws.cell(row=row_num, column=col)
                if col != label_col:
                    cell.fill = UNCERTAIN_FILL
                cell.border = THIN_BORDER
            row_num += 1


def _resolve_totals_formula(formula: str, first_row: int, last_data_row: int,
                             group_p_refs: str, group_q_refs: str,
                             row_refs: dict,
                             freight_val, insurance_val, other_cost_val,
                             deduction_val, invoice_total,
                             col_letter: str = 'P') -> Any:
    """Resolve a totals formula template into a concrete Excel formula or value."""
    if not formula:
        return None

    # Literal value references
    if formula == '${freight_value}':
        return freight_val
    if formula == '${insurance_value}':
        return insurance_val
    if formula == '${other_cost_value}':
        return other_cost_val
    if formula == '${deduction_value}':
        return deduction_val
    if formula == '${invoice_total}':
        return invoice_total

    resolved = formula
    # Replace group refs
    if '{group_P_refs}' in resolved:
        resolved = resolved.replace('{group_P_refs}', group_p_refs)
    if '{group_Q_refs}' in resolved:
        resolved = resolved.replace('{group_Q_refs}', group_q_refs)
    # Replace row references
    if '{first_row}' in resolved:
        resolved = resolved.replace('{first_row}', str(first_row))
    if '{last_data_row}' in resolved:
        resolved = resolved.replace('{last_data_row}', str(last_data_row))
    if '{subtotal_grouped_row}' in resolved:
        resolved = resolved.replace('{subtotal_grouped_row}',
                                     str(row_refs.get('SUBTOTAL (GROUPED)', first_row)))
    if '{subtotal_details_row}' in resolved:
        resolved = resolved.replace('{subtotal_details_row}',
                                     str(row_refs.get('SUBTOTAL (DETAILS)', first_row)))
    if '{adjustments_row}' in resolved:
        resolved = resolved.replace('{adjustments_row}',
                                     str(row_refs.get('ADJUSTMENTS', first_row)))
    if '{net_total_row}' in resolved:
        resolved = resolved.replace('{net_total_row}',
                                     str(row_refs.get('NET TOTAL', first_row)))
    return resolved


# ─── Duty Estimation Section ───────────────────────────────

DUTY_HEADER_FILL = PatternFill(
    start_color='D6E4F0', end_color='D6E4F0', fill_type='solid'
)
DUTY_HEADER_FONT = Font(bold=True, size=10, color='1F4E79')
DUTY_LABEL_FONT = Font(bold=False, size=10, color='1F4E79')
DUTY_VALUE_FONT = Font(bold=True, size=10, color='1F4E79')
DUTY_WARN_FONT = Font(bold=True, size=10, color='CC0000')


def _write_duty_estimation_section(
    ws, matched_items: List[Dict], invoice_data: Dict,
) -> None:
    """Write a duty estimation section below totals for classification cross-check.

    Calculates expected ASYCUDA duties (CET + CSC + VAT) based on CIF and
    tariff codes.  When the client's declared duty value is provided via
    invoice_data['_client_declared_duties'], shows the variance — a large
    difference signals potential tariff misclassification.
    """
    col_count = _spec.col_count()
    COL_J = _spec.col_index('J') if hasattr(_spec, 'col_index') else 10
    COL_P = _spec.col_index('P') if hasattr(_spec, 'col_index') else 16

    # Find the last used row
    row_num = ws.max_row + 2  # blank row gap

    # Gather values
    invoice_total = invoice_data.get('invoice_total', 0) or 0
    customs_freight = invoice_data.get('_customs_freight', 0) or 0
    customs_insurance = invoice_data.get('_customs_insurance', 0) or 0
    client_declared = invoice_data.get('_client_declared_duties')

    # Calculate weighted CET rate from items' tariff codes
    total_cost = 0
    weighted_rate = 0
    tariff_rates = {}  # tariff -> (rate, cost)
    for item in matched_items:
        tc = str(item.get('tariff_code', '00000000'))
        cost = float(item.get('total_cost', 0) or 0)
        rate = get_cet_rate(tc)
        if rate is None:
            rate = 0.20  # default
        total_cost += cost
        weighted_rate += cost * rate
        if tc not in tariff_rates:
            tariff_rates[tc] = [rate, 0]
        tariff_rates[tc][1] += cost

    avg_cet_rate = (weighted_rate / total_cost) if total_cost > 0 else 0.20

    # Calculate duties
    duties = calculate_duties(
        cif_usd=invoice_total,
        cet_rate=avg_cet_rate,
        customs_freight=customs_freight,
        insurance=customs_insurance,
    )

    # ── Header row ──
    ws.cell(row=row_num, column=COL_J, value='DUTY ESTIMATION (Classification Cross-Check)')
    for col in range(1, col_count + 1):
        cell = ws.cell(row=row_num, column=col)
        cell.fill = DUTY_HEADER_FILL
        cell.font = DUTY_HEADER_FONT
        cell.border = THIN_BORDER
    row_num += 1

    def _write_duty_row(label, value, fmt='$#,##0.00', font=None):
        nonlocal row_num
        ws.cell(row=row_num, column=COL_J, value=label).font = font or DUTY_LABEL_FONT
        val_cell = ws.cell(row=row_num, column=COL_P, value=value)
        val_cell.font = font or DUTY_VALUE_FONT
        val_cell.number_format = fmt
        for col in range(1, col_count + 1):
            ws.cell(row=row_num, column=col).border = THIN_BORDER
        row_num += 1

    # CIF breakdown
    _write_duty_row(f'CIF (USD) = InvTotal + Freight + Insurance', duties['cif_usd'])
    _write_duty_row(f'CIF (XCD) = CIF × {XCD_RATE}', duties['cif_xcd'])

    # Per-tariff CET rates (show what rates were used)
    if len(tariff_rates) == 1:
        tc, (rate, _) = list(tariff_rates.items())[0]
        _write_duty_row(f'CET ({rate*100:.0f}%) — Tariff {tc}', duties['cet'])
    else:
        _write_duty_row(f'CET (weighted avg {avg_cet_rate*100:.1f}%)', duties['cet'])
        for tc, (rate, cost) in sorted(tariff_rates.items()):
            pct = (cost / total_cost * 100) if total_cost else 0
            _write_duty_row(
                f'  └ {tc}: {rate*100:.0f}% CET ({pct:.0f}% of value)',
                round(duties['cif_xcd'] * (cost / total_cost) * rate, 2) if total_cost else 0,
            )

    _write_duty_row(f'CSC ({CSC_RATE*100:.0f}%)', duties['csc'])
    _write_duty_row(f'VAT ({VAT_RATE*100:.0f}%) on CIF+CET+CSC', duties['vat'])

    # Total
    ws.cell(row=row_num, column=COL_J, value='ESTIMATED TOTAL DUTIES').font = DUTY_HEADER_FONT
    val_cell = ws.cell(row=row_num, column=COL_P, value=duties['total_duties'])
    val_cell.font = DUTY_HEADER_FONT
    val_cell.number_format = '$#,##0.00'
    for col in range(1, col_count + 1):
        cell = ws.cell(row=row_num, column=col)
        cell.fill = DUTY_HEADER_FILL
        cell.border = THIN_BORDER
    row_num += 1

    # Effective rate
    _write_duty_row(
        f'Effective Duty Rate',
        duties['effective_rate'],
        fmt='0.0%',
    )

    # Client declared value comparison
    if client_declared is not None and client_declared > 0:
        _write_duty_row('CLIENT DECLARED DUTIES', client_declared)
        variance = round(client_declared - duties['total_duties'], 2)
        variance_font = DUTY_WARN_FONT if abs(variance) > 1.0 else DUTY_VALUE_FONT
        _write_duty_row('DUTY VARIANCE (Client − Estimated)', variance, font=variance_font)

        # Reverse-engineer client's implied CET rate
        if duties['cif_xcd'] > 0 and client_declared > 0:
            # total = cif*r + cif*0.06 + (cif + cif*r + cif*0.06)*0.15
            # total = cif*r + cif*0.06 + cif*0.15 + cif*r*0.15 + cif*0.06*0.15
            # total = cif*(r + 0.06 + 0.15 + 0.15r + 0.009)
            # total = cif*(1.15r + 0.219)
            # r = (total/cif - 0.219) / 1.15
            implied_r = (client_declared / duties['cif_xcd'] - 0.219) / 1.15
            if 0 <= implied_r <= 1.0:
                _write_duty_row(
                    f'IMPLIED CET RATE (from client value)',
                    implied_r,
                    fmt='0.0%',
                )
                if abs(implied_r - avg_cet_rate) > 0.02:
                    ws.cell(
                        row=row_num, column=COL_J,
                        value=f'⚠ CET MISMATCH: System={avg_cet_rate*100:.0f}% vs Client≈{implied_r*100:.0f}% — review classification'
                    ).font = DUTY_WARN_FONT
                    for col in range(1, col_count + 1):
                        ws.cell(row=row_num, column=col).border = THIN_BORDER
                    row_num += 1


# ─── Reference Section (other declaration items) ──────────

# Muted styling for reference items — visible but clearly not part of totals.
REF_HEADER_FILL = PatternFill(
    start_color='E2EFDA', end_color='E2EFDA', fill_type='solid'
)
REF_HEADER_FONT = Font(bold=True, size=10, color='375623')
REF_DETAIL_FONT = Font(bold=False, size=10, color='808080')


def _write_reference_section(
    ws, reference_items: List[Dict], label: str,
    invoice_data: Optional[Dict] = None,
    main_items: Optional[List[Dict]] = None,
    reference_adjustments: Optional[Dict] = None,
) -> None:
    """Append a read-only reference section showing items from other declarations.

    These items are rendered below the totals block so they do NOT affect
    any formulas.  The section uses muted green styling to distinguish it
    from the active data above.

    When ``invoice_data`` and ``main_items`` are provided, a combined
    variance check is appended so the reviewer can verify the full invoice
    reconciliation on each sheet.

    ``reference_adjustments`` optionally provides the other declaration's
    prorated adjustments: ``{'freight': ..., 'insurance': ...,
    'other_cost': ..., 'deduction': ...}``.  When present, the combined
    section sums main + reference values directly (no reverse proration).
    """
    from collections import OrderedDict

    if not reference_items:
        return

    # Find the current last row
    row_num = ws.max_row + 2  # blank row separator
    col_count = _spec.col_count()
    COL_J = _spec.col_index('J')
    COL_K = _spec.col_index('K')
    COL_N = _spec.col_index('N')
    COL_O = _spec.col_index('O')
    COL_P = _spec.col_index('P')

    def _ref_style_row(r, font=REF_HEADER_FONT, fill=None):
        for col in range(1, col_count + 1):
            cell = ws.cell(row=r, column=col)
            cell.font = font
            cell.border = THIN_BORDER
            if fill:
                cell.fill = fill
        ws.cell(row=r, column=COL_P).number_format = _spec.currency_format

    # Section header
    ws.cell(row=row_num, column=COL_J, value=label)
    _ref_style_row(row_num, fill=REF_HEADER_FILL)
    row_num += 1

    # Group reference items by tariff code
    groups: OrderedDict = OrderedDict()
    for item in reference_items:
        tc = str(item.get('tariff_code', '00000000'))
        groups.setdefault(tc, []).append(item)

    for tariff_code, items in groups.items():
        n = len(items)
        group_total = sum(it.get('total_cost', 0) for it in items)
        first = items[0]
        cet_desc = get_cet_category(tariff_code)
        category = cet_desc or first.get('category', '')

        desc = first.get('supplier_item_desc', '')
        if n > 1:
            group_label = f"{desc} (+{n - 1} more) ({n} items)"
        else:
            group_label = f"{desc} ({n} items)"

        # Group row — no date (col D), no invoice# (col C)
        ws.cell(row=row_num, column=_spec.col_index('E'), value=category)
        ws.cell(row=row_num, column=_spec.col_index('F'), value=tariff_code)
        ws.cell(row=row_num, column=COL_J, value=group_label)
        ws.cell(row=row_num, column=COL_K, value=sum(it.get('quantity', 1) for it in items))
        ws.cell(row=row_num, column=COL_N, value='USD')
        avg_cost = group_total / n if n else 0
        ws.cell(row=row_num, column=COL_O, value=avg_cost)
        ws.cell(row=row_num, column=COL_P, value=group_total)
        _ref_style_row(row_num, fill=REF_HEADER_FILL)
        for col in _spec.currency_columns_all:
            ws.cell(row=row_num, column=col).number_format = _spec.currency_format
        row_num += 1

        # Detail rows
        for item in items:
            ws.cell(row=row_num, column=_spec.col_index('I'),
                    value=item.get('supplier_item', ''))
            ws.cell(row=row_num, column=COL_J,
                    value=item.get('supplier_item_desc', ''))
            ws.cell(row=row_num, column=COL_K, value=item.get('quantity', 1))
            ws.cell(row=row_num, column=COL_N, value='USD')
            ws.cell(row=row_num, column=COL_O, value=item.get('unit_price', 0))
            ws.cell(row=row_num, column=COL_P, value=item.get('total_cost', 0))
            _ref_style_row(row_num, font=REF_DETAIL_FONT)
            for col in _spec.currency_columns_detail:
                ws.cell(row=row_num, column=col).number_format = _spec.currency_format
            row_num += 1

    # Reference subtotal
    ref_total = sum(it.get('total_cost', 0) for it in reference_items)
    ws.cell(row=row_num, column=COL_J, value='REFERENCE SUBTOTAL')
    ws.cell(row=row_num, column=COL_P, value=ref_total)
    _ref_style_row(row_num)
    ref_subtotal_row = row_num
    row_num += 1

    # Reference adjustments — the other declaration's prorated freight, etc.
    ref_adj = reference_adjustments or {}
    ref_freight_row = ref_insurance_row = ref_other_row = ref_deduction_row = None
    if ref_adj:
        ws.cell(row=row_num, column=COL_J, value='REFERENCE FREIGHT')
        ws.cell(row=row_num, column=COL_P, value=ref_adj.get('freight', 0))
        _ref_style_row(row_num)
        ref_freight_row = row_num
        row_num += 1

        ws.cell(row=row_num, column=COL_J, value='REFERENCE INSURANCE')
        ws.cell(row=row_num, column=COL_P, value=ref_adj.get('insurance', 0))
        _ref_style_row(row_num)
        ref_insurance_row = row_num
        row_num += 1

        ws.cell(row=row_num, column=COL_J, value='REFERENCE OTHER COST')
        ws.cell(row=row_num, column=COL_P, value=ref_adj.get('other_cost', 0))
        _ref_style_row(row_num)
        ref_other_row = row_num
        row_num += 1

        ws.cell(row=row_num, column=COL_J, value='REFERENCE DEDUCTION')
        ws.cell(row=row_num, column=COL_P, value=ref_adj.get('deduction', 0))
        _ref_style_row(row_num)
        ref_deduction_row = row_num
        row_num += 1

        ws.cell(row=row_num, column=COL_J, value='REFERENCE ADJUSTMENTS')
        ws.cell(row=row_num, column=COL_P,
                value=f'=P{ref_freight_row}+P{ref_insurance_row}+P{ref_other_row}-P{ref_deduction_row}')
        _ref_style_row(row_num)
        ref_adj_row = row_num
        row_num += 1

        ws.cell(row=row_num, column=COL_J, value='REFERENCE NET TOTAL')
        ws.cell(row=row_num, column=COL_P,
                value=f'=P{ref_subtotal_row}+P{ref_adj_row}')
        _ref_style_row(row_num)
        row_num += 1

    # ── Combined variance check (all formulas) ───────────────
    # Shows the full invoice reconciliation across both declarations.
    # When reference_adjustments is present, sums main + reference directly.
    # Otherwise falls back to reverse proration.
    if invoice_data and main_items is not None:
        # Find key rows from the main totals section by label
        main_rows = {}
        for r in range(2, ref_subtotal_row):
            lbl = str(ws.cell(row=r, column=COL_J).value or '')
            if lbl in ('SUBTOTAL (GROUPED)', 'SUBTOTAL',
                       'TOTAL INTERNAL FREIGHT', 'TOTAL INSURANCE',
                       'TOTAL OTHER COST', 'TOTAL DEDUCTION',
                       'ADJUSTMENTS', 'NET TOTAL'):
                main_rows[lbl] = r

        # Support both grouped (separate freight/insurance rows) and
        # ungrouped (single ADJUSTMENTS row) modes.
        subtotal_row = main_rows.get('SUBTOTAL (GROUPED)') or main_rows.get('SUBTOTAL')
        freight_row = main_rows.get('TOTAL INTERNAL FREIGHT')
        insurance_row = main_rows.get('TOTAL INSURANCE')
        other_cost_row = main_rows.get('TOTAL OTHER COST')
        deduction_row = main_rows.get('TOTAL DEDUCTION')
        adjustments_row = main_rows.get('ADJUSTMENTS')  # ungrouped mode

        if not subtotal_row:
            return  # can't build formulas without the main section

        full_invoice_total = invoice_data.get('_full_invoice_total', 0)

        row_num += 1  # blank separator

        # COMBINED ITEMS TOTAL = main subtotal + reference subtotal
        ws.cell(row=row_num, column=COL_J, value='COMBINED ITEMS TOTAL')
        ws.cell(row=row_num, column=COL_P,
                value=f'=P{subtotal_row}+P{ref_subtotal_row}')
        _ref_style_row(row_num)
        combined_row = row_num
        row_num += 1

        # Grouped mode has separate freight/insurance/other/deduction rows.
        # Ungrouped mode has a single ADJUSTMENTS row instead.
        is_grouped = bool(freight_row)

        if is_grouped and ref_adj and ref_freight_row:
            # Direct sum: main adjustment rows + reference adjustment rows
            ws.cell(row=row_num, column=COL_J, value='FULL INVOICE FREIGHT')
            ws.cell(row=row_num, column=COL_P,
                    value=f'=P{freight_row}+P{ref_freight_row}')
            _ref_style_row(row_num)
            full_freight_row = row_num
            row_num += 1

            ws.cell(row=row_num, column=COL_J, value='FULL INVOICE INSURANCE')
            ws.cell(row=row_num, column=COL_P,
                    value=f'=P{insurance_row}+P{ref_insurance_row}')
            _ref_style_row(row_num)
            full_insurance_row = row_num
            row_num += 1

            ws.cell(row=row_num, column=COL_J, value='FULL INVOICE OTHER COST')
            ws.cell(row=row_num, column=COL_P,
                    value=f'=P{other_cost_row}+P{ref_other_row}')
            _ref_style_row(row_num)
            full_other_row = row_num
            row_num += 1

            ws.cell(row=row_num, column=COL_J, value='FULL INVOICE DEDUCTION')
            ws.cell(row=row_num, column=COL_P,
                    value=f'=P{deduction_row}+P{ref_deduction_row}')
            _ref_style_row(row_num)
            full_deduction_row = row_num
            row_num += 1

            # FULL ADJUSTMENTS = freight + insurance + other_cost - deduction
            ws.cell(row=row_num, column=COL_J, value='FULL ADJUSTMENTS')
            ws.cell(row=row_num, column=COL_P,
                    value=f'=P{full_freight_row}+P{full_insurance_row}+P{full_other_row}-P{full_deduction_row}')
            _ref_style_row(row_num)
            full_adj_row = row_num
            row_num += 1

        elif is_grouped:
            # Grouped mode, no reference adjustments — reverse proration
            ws.cell(row=row_num, column=COL_J, value='FULL INVOICE FREIGHT')
            ws.cell(row=row_num, column=COL_P,
                    value=f'=P{freight_row}/P{subtotal_row}*P{combined_row}')
            _ref_style_row(row_num)
            full_freight_row = row_num
            row_num += 1

            ws.cell(row=row_num, column=COL_J, value='FULL INVOICE INSURANCE')
            ws.cell(row=row_num, column=COL_P,
                    value=f'=P{insurance_row}/P{subtotal_row}*P{combined_row}')
            _ref_style_row(row_num)
            full_insurance_row = row_num
            row_num += 1

            ws.cell(row=row_num, column=COL_J, value='FULL INVOICE OTHER COST')
            ws.cell(row=row_num, column=COL_P,
                    value=f'=P{other_cost_row}/P{subtotal_row}*P{combined_row}')
            _ref_style_row(row_num)
            full_other_row = row_num
            row_num += 1

            ws.cell(row=row_num, column=COL_J, value='FULL INVOICE DEDUCTION')
            ws.cell(row=row_num, column=COL_P,
                    value=f'=P{deduction_row}/P{subtotal_row}*P{combined_row}')
            _ref_style_row(row_num)
            full_deduction_row = row_num
            row_num += 1

            # FULL ADJUSTMENTS = freight + insurance + other_cost - deduction
            ws.cell(row=row_num, column=COL_J, value='FULL ADJUSTMENTS')
            ws.cell(row=row_num, column=COL_P,
                    value=f'=P{full_freight_row}+P{full_insurance_row}+P{full_other_row}-P{full_deduction_row}')
            _ref_style_row(row_num)
            full_adj_row = row_num
            row_num += 1

        else:
            # Ungrouped mode — single ADJUSTMENTS row in main section.
            # Sum main adjustments + reference adjustments directly.
            if ref_adj and ref_freight_row:
                # Show individual full-invoice lines using reference values
                # + main's portion from invoice_data
                ws.cell(row=row_num, column=COL_J, value='FULL INVOICE FREIGHT')
                ws.cell(row=row_num, column=COL_P,
                        value=f'=T2+P{ref_freight_row}')
                _ref_style_row(row_num)
                full_freight_row = row_num
                row_num += 1

                ws.cell(row=row_num, column=COL_J, value='FULL INVOICE INSURANCE')
                ws.cell(row=row_num, column=COL_P,
                        value=f'=U2+P{ref_insurance_row}')
                _ref_style_row(row_num)
                full_insurance_row = row_num
                row_num += 1

                ws.cell(row=row_num, column=COL_J, value='FULL INVOICE OTHER COST')
                ws.cell(row=row_num, column=COL_P,
                        value=f'=V2+P{ref_other_row}')
                _ref_style_row(row_num)
                full_other_row = row_num
                row_num += 1

                ws.cell(row=row_num, column=COL_J, value='FULL INVOICE DEDUCTION')
                ws.cell(row=row_num, column=COL_P,
                        value=f'=W2+P{ref_deduction_row}')
                _ref_style_row(row_num)
                full_deduction_row = row_num
                row_num += 1

                ws.cell(row=row_num, column=COL_J, value='FULL ADJUSTMENTS')
                ws.cell(row=row_num, column=COL_P,
                        value=f'=P{full_freight_row}+P{full_insurance_row}+P{full_other_row}-P{full_deduction_row}')
                _ref_style_row(row_num)
                full_adj_row = row_num
                row_num += 1
            elif adjustments_row:
                # No reference adjustments — reverse proration via ratio
                ws.cell(row=row_num, column=COL_J, value='FULL ADJUSTMENTS')
                ws.cell(row=row_num, column=COL_P,
                        value=f'=P{adjustments_row}/P{subtotal_row}*P{combined_row}')
                _ref_style_row(row_num)
                full_adj_row = row_num
                row_num += 1
            else:
                return  # can't build combined section

        # COMBINED NET TOTAL = combined items + full adjustments
        ws.cell(row=row_num, column=COL_J, value='COMBINED NET TOTAL')
        ws.cell(row=row_num, column=COL_P,
                value=f'=P{combined_row}+P{full_adj_row}')
        _ref_style_row(row_num)
        combined_net_row = row_num
        row_num += 1

        # FULL INVOICE TOTAL — the original un-split invoice total (input value)
        ws.cell(row=row_num, column=COL_J, value='FULL INVOICE TOTAL')
        ws.cell(row=row_num, column=COL_P, value=full_invoice_total)
        _ref_style_row(row_num)
        full_total_row = row_num
        row_num += 1

        # COMBINED VARIANCE CHECK = invoice total - net total (formula)
        ws.cell(row=row_num, column=COL_J, value='COMBINED VARIANCE CHECK')
        ws.cell(row=row_num, column=COL_P,
                value=f'=P{full_total_row}-P{combined_net_row}')
        _ref_style_row(row_num)


# ─── BL Package Update ─────────────────────────────────────

def update_xlsx_packages(xlsx_path: str, packages: int,
                         freight: float = 0, insurance: float = 0) -> None:
    """Update BL-level fields on an existing XLSX file (row 2)."""
    wb = openpyxl.load_workbook(xlsx_path)
    ws = wb.active
    COL_X = _spec.col_index('X')
    ws.cell(row=2, column=COL_X, value=packages)
    if freight:
        # BL freight stored in extra column (38) to avoid corrupting variance
        ws.cell(row=1, column=_spec.col_count() + 1, value='BLFreight')
        ws.cell(row=2, column=_spec.col_count() + 1, value=round(freight, 2))
    if insurance:
        ws.cell(row=1, column=_spec.col_count() + 2, value='BLInsurance')
        ws.cell(row=2, column=_spec.col_count() + 2, value=round(insurance, 2))
    wb.save(xlsx_path)
