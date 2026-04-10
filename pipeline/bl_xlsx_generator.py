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
    from openpyxl.utils import get_column_letter
except ImportError:
    openpyxl = None

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


# ─── CET description cache ─────────────────────────────────
_cet_desc_cache: Dict[str, str] = {}
_cet_db_loaded = False

def _load_cet_descriptions() -> None:
    """Load CET code descriptions from the database."""
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
        rows = conn.execute('SELECT hs_code, description FROM cet_codes WHERE description != ""').fetchall()
        for code, desc in rows:
            _cet_desc_cache[code] = desc
        conn.close()
        logger.info(f"[CET] Loaded {len(_cet_desc_cache)} descriptions from database")
        _cet_db_loaded = True
    except Exception as e:
        logger.warning(f"[CET] Read-only connect failed: {e}, trying temp copy")
        try:
            import shutil, tempfile
            tmp = os.path.join(tempfile.gettempdir(), 'cet_readonly.db')
            shutil.copy2(db_path, tmp)
            conn = sqlite3.connect(tmp)
            rows = conn.execute('SELECT hs_code, description FROM cet_codes WHERE description != ""').fetchall()
            for code, desc in rows:
                _cet_desc_cache[code] = desc
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
) -> str:
    """Generate a CARICOM-format XLSX file for one BL invoice."""
    if not openpyxl:
        raise ImportError("openpyxl is required. Run: pip install openpyxl")

    doc_config = load_document_type_config(document_type)
    grouping = doc_config.get('grouping', False)

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Invoice Data"

    _write_headers(ws)

    if not matched_items:
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
        _write_subtotals_ungrouped(ws, row_num, len(matched_items))

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

def _write_subtotals_ungrouped(ws, row_num: int, item_count: int) -> None:
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
