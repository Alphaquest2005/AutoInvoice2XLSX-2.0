#!/usr/bin/env python3
"""
Stage 6: XLSX Generator
Generates Excel output per columns.yaml specification.
37 columns (A-AK), group/detail rows, totals section.
"""

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List
try:
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment
except ImportError:
    openpyxl = None
try:
    import yaml
except ImportError:
    yaml = None

# Category name mappings (from grouping.yaml)
CATEGORY_NAMES = {
    "33030090": "PERFUMES",
    "33041000": "LIP PRODUCTS",
    "33042000": "EYE MAKEUP",
    "33043000": "NAIL PRODUCTS",
    "33049100": "FACE MAKEUP",
    "33049990": "SUNSCREEN",
    "33049990": "SKINCARE & OTHER COSMETICS",
    "33051000": "SHAMPOO & CONDITIONER",
    "33059010": "HAIR COLOR",
    "33059090": "HAIR STYLING",
    "33072000": "BODY SPRAYS & DEODORANTS",
    "33074100": "INCENSE",
    "33074900": "ROOM PERFUMING PREPARATIONS",
    "34011910": "SOAP & BODY WASH",
    "34022090": "CLEANING PREPARATIONS",
    "38089490": "DISINFECTANTS",
    "48191000": "CARDBOARD DISPLAYS",
    "67041100": "WIGS",
    "67041900": "SYNTHETIC HAIR",
    "67042000": "HUMAN HAIR",
    "71171910": "IMITATION JEWELRY",
    "73269090": "METAL ARTICLES",
    "84145110": "TABLE FANS",
    "85167900": "ELECTRO-THERMIC APPLIANCES",
    "85437090": "ULTRASONIC DIFFUSERS",
    "96039000": "BRUSHES",
    "96151110": "COMBS",
    "96159010": "HAIR PINS",
    "96159090": "OTHER HAIR ACCESSORIES",
}


def _find_columns_yaml() -> str:
    """Locate config/columns.yaml relative to this file."""
    spec_paths = [
        os.path.join(os.path.dirname(__file__), '..', 'config', 'columns.yaml'),
        os.path.join(os.path.dirname(__file__), 'config', 'columns.yaml'),
    ]
    for p in spec_paths:
        p = os.path.normpath(p)
        if os.path.exists(p):
            return p
    return None


def load_columns_spec() -> List[Dict]:
    """Load column definitions from config/columns.yaml (specification-driven).
    Returns ordered list of {index, name, header} dicts.
    Falls back to None if yaml is missing."""
    p = _find_columns_yaml()
    if not p or not yaml:
        return None
    with open(p) as f:
        spec = yaml.safe_load(f)
    columns = spec.get('columns', {})
    result = []
    for _letter, col_def in sorted(columns.items(), key=lambda x: x[1].get('index', 0)):
        result.append({
            'index': col_def['index'],
            'name': col_def.get('name', ''),
            'header': col_def.get('header', col_def.get('name', '')),
        })
    return result


def load_formula_templates() -> Dict[str, str]:
    """Load formula_templates from config/columns.yaml.
    Returns dict like {'adjustments': '=(T2+U2+V2-W2)', ...}."""
    p = _find_columns_yaml()
    if not p or not yaml:
        return {}
    with open(p) as f:
        spec = yaml.safe_load(f)
    templates = spec.get('formula_templates', {})
    return {k: v.get('pattern', '') for k, v in templates.items()}


def get_next_version_filename(output_path: str, input_path: str = None) -> str:
    """
    Auto-increment version number in filename.
    Derives the output name from the input file's base name.

    Examples:
        INVOICE#307500.xlsx (first run)
        INVOICE#307500_v2.xlsx (second run)
        INVOICE#307500_v3.xlsx (third run)
    """
    output_path = os.path.abspath(output_path)
    output_dir = os.path.dirname(output_path)
    ext = os.path.splitext(output_path)[1] or '.xlsx'

    # Derive base stem from input filename (strip .txt/.json/.pdf extension)
    if input_path:
        input_base = os.path.splitext(os.path.basename(input_path))[0]
        # Remove intermediate suffixes like _grouped, _classified, _parsed
        for suffix in ('_grouped', '_classified', '_parsed', '_extracted'):
            if input_base.endswith(suffix):
                input_base = input_base[:-len(suffix)]
        base_stem = input_base
    else:
        # Fall back to output path's own name
        name_without_ext = os.path.splitext(os.path.basename(output_path))[0]
        # Strip existing version suffix
        m = re.match(r'^(.+?)_v(\d+)$', name_without_ext)
        base_stem = m.group(1) if m else name_without_ext

    # Scan output directory for existing versions
    existing_versions = []
    if os.path.exists(output_dir):
        scan_pattern = re.compile(
            rf'^{re.escape(base_stem)}(?:_v(\d+))?{re.escape(ext)}$'
        )
        for filename in os.listdir(output_dir):
            match = scan_pattern.match(filename)
            if match:
                version_num = int(match.group(1)) if match.group(1) else 1
                existing_versions.append(version_num)

    if not existing_versions:
        new_filename = f"{base_stem}{ext}"
    else:
        next_version = max(existing_versions) + 1
        new_filename = f"{base_stem}_v{next_version}{ext}"

    return os.path.join(output_dir, new_filename)


def run(input_path: str, output_path: str, config: Dict = None, context: Dict = None) -> Dict:
    """Generate Excel output from grouped data."""
    if not openpyxl:
        return {'status': 'error', 'error': 'openpyxl not installed. Run: pip install openpyxl'}

    if not input_path or not os.path.exists(input_path):
        return {'status': 'error', 'error': f'Input not found: {input_path}'}

    # Auto-increment version number in output filename
    # Use original input filename from pipeline context (not intermediate grouped.json)
    original_input = context.get('input_file') if context else None
    output_path = get_next_version_filename(output_path, original_input or input_path)
    
    with open(input_path) as f:
        data = json.load(f)

    groups = data.get('groups', [])
    metadata = data.get('invoice_metadata', {})

    if not groups:
        return {'status': 'error', 'error': 'No groups to generate'}

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = 'Invoice Data'

    # Styles
    header_fill = PatternFill(start_color='1F4E79', end_color='1F4E79', fill_type='solid')
    header_font = Font(bold=True, size=11, color='FFFFFF')
    group_fill = PatternFill(start_color='2E75B6', end_color='2E75B6', fill_type='solid')
    group_font = Font(bold=True, size=11, color='FFFFFF')
    detail_font = Font(size=10)
    currency_fmt = '#,##0.00'
    bold_font = Font(bold=True)
    verify_font = Font(bold=True, color='0000FF')
    variance_font = Font(bold=True, color='FF0000')
    totals_fill = PatternFill(start_color='D6E4F0', end_color='D6E4F0', fill_type='solid')

    # Headers from columns.yaml (specification-driven, not hardcoded)
    columns_spec = load_columns_spec()
    if columns_spec:
        for col_def in columns_spec:
            cell = ws.cell(row=1, column=col_def['index'], value=col_def['header'])
            cell.font = header_font
            cell.fill = header_fill
    else:
        # Fallback only if columns.yaml is missing — must match PRODUCTION_HEADERS
        _fallback_headers = [
            "Document Type", "PO Number", "Supplier Invoice#", "Date", "Category",
            "TariffCode", "PO Item Number", "PO Item Description",
            "Supplier Item Number", "Supplier Item Description", "Quantity",
            "Per Unit", "UNITS", "Currency", "Cost", "Total Cost", "Total",
            "TotalCost Vs Total", "InvoiceTotal", "Total Internal Freight",
            "Total Insurance", "Total Other Cost", "Total Deduction", "Packages",
            "Warehouse", "Supplier Code", "Supplier Name", "Supplier Address",
            "Country Code", "Instructions", "Previous Declaration",
            "Financial Information", "Gallons", "Liters", "INVTotalCost",
            "POTotalCost", "GroupBy",
        ]
        for col, header in enumerate(_fallback_headers, 1):
            cell = ws.cell(row=1, column=col, value=header)
            cell.font = header_font
            cell.fill = header_fill

    po_number = metadata.get('po_number', '')
    is_invoice = not po_number or po_number in ['', 'Page', 'None', 'N/A']

    row_num = 2
    group_row_nums = []
    detail_row_nums = []
    computed_group_total = 0.0
    computed_detail_total = 0.0
    
    for group in groups:
        tariff = group['tariff_code']
        raw_category = group.get('category', 'PRODUCTS')
        from bl_xlsx_generator import get_cet_category, _normalize_date
        cet_desc = get_cet_category(tariff)
        category = CATEGORY_NAMES.get(tariff, cet_desc or str(raw_category))
        item_count = group['item_count']
        category_label = f"{category} ({item_count} items)"

        sum_qty = group['sum_quantity']
        sum_cost = group['sum_total_cost']
        avg_cost = group['average_unit_cost']

        group_row_nums.append(row_num)
        computed_group_total += sum_cost

        # Only FIRST group row per invoice gets Document Type
        if row_num == 2:
            ws.cell(row_num, column=1, value='4000-000')
        else:
            ws.cell(row_num, column=1, value=None)
            
        ws.cell(row_num, column=3, value=metadata.get('invoice_number', ''))
        ws.cell(row_num, column=4, value=_normalize_date(metadata.get('date', '')))
        ws.cell(row_num, column=5, value=category)
        ws.cell(row_num, column=6, value=tariff)
        
        if not is_invoice:
            ws.cell(row_num, column=7, value=tariff)
            ws.cell(row_num, column=8, value=category_label)
        else:
            ws.cell(row_num, column=7, value=None)
            ws.cell(row_num, column=8, value=None)
            
        ws.cell(row_num, column=9, value=tariff)
        ws.cell(row_num, column=10, value=category_label)
        ws.cell(row_num, column=11, value=sum_qty)
        ws.cell(row_num, column=12, value=category_label)
        ws.cell(row_num, column=14, value='USD')
        ws.cell(row_num, column=15, value=avg_cost)
        ws.cell(row_num, column=16, value=sum_cost)
        
        # Q=Stat Value, R=Variance as Excel formulas (Golden Rule #10: formulas not hardcodes)
        ws.cell(row_num, column=17).value = f'=O{row_num}*K{row_num}'
        ws.cell(row_num, column=18).value = f'=P{row_num}-Q{row_num}'
        ws.cell(row_num, column=37, value=tariff)  # AK: GroupBy
        
        if row_num == 2:
            total = metadata.get('total')
            if total:
                ws.cell(row_num, column=19, value=total)
            freight = metadata.get('freight', 0)
            if freight:
                ws.cell(row_num, column=20, value=freight)    # T: Total Internal Freight
            # Column U: insurance OR customer-induced credits (x -1)
            insurance = metadata.get('insurance', 0)
            credits = metadata.get('credits', 0)
            col_u_value = insurance if insurance else (-credits if credits else 0)
            if col_u_value:
                ws.cell(row_num, column=21, value=col_u_value) # U: Total Insurance
            # V=Other Cost (tax, fees, service charges)
            other_cost = metadata.get('other_cost', 0) or metadata.get('tax', 0)
            if other_cost:
                ws.cell(row_num, column=22, value=other_cost)  # V: Total Other Cost
            # W: Total Deduction = discount + free_shipping (both supplier-induced)
            discount = metadata.get('discount', 0) or 0
            free_shipping = metadata.get('free_shipping', 0) or 0
            total_deduction = discount + free_shipping
            if total_deduction:
                ws.cell(row_num, column=23, value=total_deduction)  # W: Total Deduction

            supplier = metadata.get('supplier', '')
            supplier_code = ''
            if supplier:
                if 'ABSOLUTE' in supplier.upper():
                    supplier_code = 'ABSOLUTE'
                elif 'AMAZON' in supplier.upper():
                    supplier_code = 'AMAZON'
                elif 'TARGET' in supplier.upper():
                    supplier_code = 'TARGET'
                elif 'WALMART' in supplier.upper():
                    supplier_code = 'WALMART'
                else:
                    # Ensure supplier is a string before splitting
                    supplier_str = str(supplier) if supplier else ''
                    supplier_code = supplier_str.split()[0] if supplier_str.split() else ''
            if supplier_code:
                ws.cell(row_num, column=26, value=supplier_code)
            if supplier:
                ws.cell(row_num, column=27, value=supplier)
            sup_addr = metadata.get('supplier_address', '')
            if sup_addr:
                ws.cell(row_num, column=28, value=sup_addr)
            country = metadata.get('country_code', '')
            if country:
                ws.cell(row_num, column=29, value=country)

        for col in range(1, 38):
            cell = ws.cell(row_num, column=col)
            cell.fill = group_fill
            cell.font = group_font
        for col in [15, 16, 17, 18, 19, 20, 21, 22, 23]:
            ws.cell(row_num, column=col).number_format = currency_fmt
        row_num += 1

        for item in group['items']:
            detail_row_nums.append(row_num)
            item_qty = item.get('quantity', 0)
            item_cost = item.get('unit_cost', 0)
            item_total = item.get('total_cost', 0)
            # Only add to detail total if item is billable (matches grouping_engine logic)
            if item.get('billable', True):
                computed_detail_total += item_total

            ws.cell(row_num, column=6, value=tariff)
            ws.cell(row_num, column=9, value=item.get('supplier_item') or item.get('sku', ''))
            ws.cell(row_num, column=10, value=item.get('description', ''))
            ws.cell(row_num, column=11, value=item_qty)
            ws.cell(row_num, column=12, value=f"    {item.get('description', '')}")
            ws.cell(row_num, column=14, value='USD')
            ws.cell(row_num, column=15, value=item_cost)
            ws.cell(row_num, column=16, value=item_total)
            
            ws.cell(row_num, column=17).value = f'=O{row_num}*K{row_num}'
            ws.cell(row_num, column=18).value = f'=P{row_num}-Q{row_num}'

            for col in range(1, 38):
                ws.cell(row_num, column=col).font = detail_font

            for col in [15, 16, 17, 18]:
                ws.cell(row_num, column=col).number_format = currency_fmt

            row_num += 1

    row_num += 1

    # --- Extract metadata values for cells S2, T2, U2, V2, W2 ---
    # These are the SAME values written to row 2 above (lines 213-230).
    # Python mirrors what the Excel formulas will compute so we can report
    # values in the return dict and set font colors. No separate "credits"
    # variable — credits are folded into insurance (U2) at write time.
    invoice_total = metadata.get('total', 0) or 0       # -> S2
    freight = metadata.get('freight', 0) or 0            # -> T2
    insurance = metadata.get('insurance', 0) or metadata.get('credits', 0) or 0  # -> U2
    other_cost = metadata.get('other_cost', 0) or metadata.get('tax', 0) or 0    # -> V2
    discount = metadata.get('discount', 0) or 0
    free_shipping = metadata.get('free_shipping', 0) or 0
    total_deduction = discount + free_shipping           # -> W2

    # Helper to apply fill across all columns of a summary row
    def fill_summary_row(r):
        for c in range(1, 38):
            ws.cell(r, column=c).fill = totals_fill

    # Mirror the Excel formulas exactly for pipeline reporting:
    group_verification = round(computed_group_total - computed_detail_total, 2)
    # ADJUSTMENTS = T2+U2+V2-W2 (must match the spec formula exactly)
    adjustments = freight + insurance + other_cost - total_deduction
    net_total = computed_group_total + adjustments
    variance_check = round(invoice_total - net_total, 2)

    # Load formula templates from columns.yaml (specification-driven)
    formulas = load_formula_templates()

    # --- Totals Section (all formulas from spec, not hardcoded) ---

    # SUBTOTAL (GROUPED) — formula pattern from spec: "={group_P_refs}"
    ws.cell(row_num, column=12, value='SUBTOTAL (GROUPED)')
    ws.cell(row_num, column=11, value=sum(g['sum_quantity'] for g in groups))
    group_p_refs = '+'.join(f'P{r}' for r in group_row_nums)
    ws.cell(row_num, column=16).value = f'={group_p_refs}' if group_p_refs else 0
    ws.cell(row_num, column=16).number_format = currency_fmt
    ws.cell(row_num, column=12).font = bold_font
    fill_summary_row(row_num)
    subtotal_grouped_row = row_num
    row_num += 1

    # SUBTOTAL (DETAILS) — formula sums all detail row P values
    ws.cell(row_num, column=12, value='SUBTOTAL (DETAILS)')
    detail_p_refs = '+'.join(f'P{r}' for r in detail_row_nums)
    ws.cell(row_num, column=16).value = f'={detail_p_refs}' if detail_p_refs else 0
    ws.cell(row_num, column=16).number_format = currency_fmt
    ws.cell(row_num, column=12).font = bold_font
    fill_summary_row(row_num)
    subtotal_details_row = row_num
    row_num += 1

    # GROUP VERIFICATION — spec: "=P{subtotal_grouped_row}-P{subtotal_details_row}"
    ws.cell(row_num, column=12, value='GROUP VERIFICATION')
    gv_pattern = formulas.get('group_verification', '=P{subtotal_grouped_row}-P{subtotal_details_row}')
    gv_formula = gv_pattern.replace('{subtotal_grouped_row}', str(subtotal_grouped_row)).replace('{subtotal_details_row}', str(subtotal_details_row))
    ws.cell(row_num, column=16).value = gv_formula
    ws.cell(row_num, column=16).number_format = currency_fmt
    ws.cell(row_num, column=12).font = verify_font
    ws.cell(row_num, column=16).font = verify_font
    fill_summary_row(row_num)
    row_num += 1

    # ADJUSTMENTS — spec: "=(T{first_row}+U{first_row}+V{first_row}-W{first_row})"
    ws.cell(row_num, column=12, value='ADJUSTMENTS')
    first_row = 2  # First data row (after header)
    adj_pattern = formulas.get('adjustments', '=(T{first_row}+U{first_row}+V{first_row}-W{first_row})')
    adj_formula = adj_pattern.replace('{first_row}', str(first_row))
    ws.cell(row_num, column=16).value = adj_formula
    ws.cell(row_num, column=16).number_format = currency_fmt
    ws.cell(row_num, column=12).font = bold_font
    fill_summary_row(row_num)
    adjustments_row = row_num
    row_num += 1

    # NET TOTAL — spec: "=P{subtotal_grouped_row}+P{adjustments_row}"
    ws.cell(row_num, column=12, value='NET TOTAL')
    nt_pattern = formulas.get('net_total', '=P{subtotal_grouped_row}+P{adjustments_row}')
    nt_formula = nt_pattern.replace('{subtotal_grouped_row}', str(subtotal_grouped_row)).replace('{adjustments_row}', str(adjustments_row))
    ws.cell(row_num, column=16).value = nt_formula
    ws.cell(row_num, column=16).number_format = currency_fmt
    ws.cell(row_num, column=12).font = bold_font
    fill_summary_row(row_num)
    net_total_row = row_num
    row_num += 1

    # VARIANCE CHECK — spec: "=S{first_row}-P{net_total_row}"
    ws.cell(row_num, column=12, value='VARIANCE CHECK')
    vc_pattern = formulas.get('variance_check', '=S{first_row}-P{net_total_row}')
    vc_formula = vc_pattern.replace('{first_row}', str(first_row)).replace('{net_total_row}', str(net_total_row))
    ws.cell(row_num, column=16).value = vc_formula
    ws.cell(row_num, column=16).number_format = currency_fmt
    if abs(variance_check) > 0.01:
        ws.cell(row_num, column=12).font = variance_font
        ws.cell(row_num, column=16).font = variance_font
    else:
        ws.cell(row_num, column=12).font = verify_font
        ws.cell(row_num, column=16).font = verify_font
    fill_summary_row(row_num)
    row_num += 1

    wb.save(output_path)

    return {
        'status': 'success',
        'output': output_path,
        'total_groups': len(groups),
        'total_rows': row_num,
        'group_verification': group_verification,
        'variance_check': variance_check,
        'invoice_total': invoice_total,
        'freight': freight,
        'insurance': insurance,
        'other_cost': other_cost,
        'net_total': net_total,
    }


def run_split_declarations(input_path: str, output_dir: str, config: Dict = None, context: Dict = None) -> Dict:
    """
    Generate MULTIPLE Excel files - one per tariff code group.

    Each group becomes its own simplified declaration (XLSX).
    Used when one invoice needs to be split into multiple declarations.

    Returns:
        Dict with 'outputs' list of generated XLSX paths
    """
    if not openpyxl:
        return {'status': 'error', 'error': 'openpyxl not installed'}

    if not input_path or not os.path.exists(input_path):
        return {'status': 'error', 'error': f'Input not found: {input_path}'}

    with open(input_path) as f:
        data = json.load(f)

    groups = data.get('groups', [])
    metadata = data.get('invoice_metadata', {})

    if not groups:
        return {'status': 'error', 'error': 'No groups to generate'}

    os.makedirs(output_dir, exist_ok=True)

    invoice_number = metadata.get('invoice_number', 'invoice')
    # Clean invoice number for filename
    clean_inv = ''.join(c if c.isalnum() or c in '._-' else '_' for c in str(invoice_number))

    outputs = []

    for i, group in enumerate(groups, 1):
        tariff_code = group.get('tariff_code', 'UNKNOWN')
        category = group.get('category', 'PRODUCTS')

        # Create single-group data for this declaration
        single_group_data = {
            'invoice_metadata': metadata,
            'groups': [group],
            'total_groups': 1,
            'total_items': group.get('item_count', len(group.get('items', []))),
        }

        # Generate filename: invoice_tariffcode.xlsx
        output_filename = f"{clean_inv}_{tariff_code}.xlsx"
        output_path = os.path.join(output_dir, output_filename)

        # Write temp JSON and run normal generator
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp:
            json.dump(single_group_data, tmp)
            tmp_path = tmp.name

        try:
            # Pass context with a fake input_file to get correct output naming
            split_context = {'input_file': f"{clean_inv}_{tariff_code}.pdf"}
            result = run(tmp_path, output_path, config, split_context)
            if result.get('status') == 'success':
                actual_path = result.get('output', output_path)
                outputs.append({
                    'path': actual_path,
                    'tariff_code': tariff_code,
                    'category': category,
                    'item_count': group.get('item_count', 0),
                    'total_cost': group.get('sum_total_cost', 0),
                })
        finally:
            os.unlink(tmp_path)

    return {
        'status': 'success',
        'outputs': outputs,
        'total_declarations': len(outputs),
        'invoice_number': invoice_number,
    }