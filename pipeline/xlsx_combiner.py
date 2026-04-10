#!/usr/bin/env python3
"""
XLSX Combiner - Combine multiple invoice XLSX files into one.

Creates a combined file with:
- One header row at the top
- Each invoice's FULL data including verification totals
- Blank row between invoices
- Grand total with variance check at the end
"""

import argparse
import json
import os
import re
from typing import List, Dict, Any
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from copy import copy


def copy_cell_style(src_cell, dest_cell):
    """Copy all styling from source cell to destination cell."""
    if src_cell.has_style:
        dest_cell.font = copy(src_cell.font)
        dest_cell.border = copy(src_cell.border)
        dest_cell.fill = copy(src_cell.fill)
        dest_cell.number_format = src_cell.number_format
        dest_cell.protection = copy(src_cell.protection)
        dest_cell.alignment = copy(src_cell.alignment)


def _shift_formula(formula: str, row_offset: int) -> str:
    """Shift all row references in an Excel formula by row_offset.

    E.g. '=O2*K2' with offset 5 → '=O7*K7'
         '=P2+P15' with offset 5 → '=P7+P20'
         '=SUM(P2:P19)-P21' with offset 5 → '=SUM(P7:P24)-P26'
    """
    if not formula or not formula.startswith('='):
        return formula

    def replace_ref(m):
        col = m.group(1)
        row_num = int(m.group(2))
        return f'{col}{row_num + row_offset}'

    # Match column letter(s) followed by row number, but not inside quotes
    return re.sub(r'([A-Z]{1,3})(\d+)', replace_ref, formula)


def combine_xlsx_files(file_paths: List[str], output_path: str,
                       ocr_notes: List[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Combine multiple XLSX invoice files into one.

    Args:
        file_paths: List of paths to XLSX files to combine
        output_path: Path for the combined output file
        ocr_notes: Optional list of dicts with OCR quality data per invoice:
                   [{'pdf_file': str, 'score': int, 'rating': str,
                     'details': str, 'raw_text': str}]

    Returns:
        Dict with status and details
    """
    # Deduplicate by absolute path
    seen = set()
    unique_paths = []
    for p in file_paths:
        ap = os.path.abspath(p)
        if ap not in seen:
            seen.add(ap)
            unique_paths.append(p)
    file_paths = unique_paths

    if len(file_paths) < 2:
        return {'status': 'error', 'error': 'Need at least 2 files to combine'}

    # Debug: log files being combined
    print(f"    [combiner] Combining {len(file_paths)} files:", flush=True)
    for fp in file_paths:
        try:
            wb_dbg = load_workbook(fp)
            ws_dbg = wb_dbg.active
            print(f"      {os.path.basename(fp)}: {ws_dbg.max_row} rows, {ws_dbg.max_column} cols, sheet={ws_dbg.title}", flush=True)
            # Check first data row structure
            j2 = ws_dbg.cell(2, 10).value
            l2 = ws_dbg.cell(2, 12).value
            fill2 = ws_dbg.cell(2, 1).fill
            is_grp = fill2 and fill2.start_color and 'D9E1F2' in str(fill2.start_color.rgb or '')
            print(f"        Row2: J={str(j2)[:30] if j2 else None!r} L={str(l2)[:30] if l2 else None!r} grouped={is_grp}", flush=True)
            wb_dbg.close()
        except Exception as e:
            print(f"      {os.path.basename(fp)}: ERROR {e}", flush=True)

    # Create output workbook
    out_wb = Workbook()
    out_ws = out_wb.active
    out_ws.title = 'Combined Invoices'

    # Track totals for grand summary
    invoice_summaries = []
    grand_total_cost = 0.0
    grand_total_variance = 0.0

    current_row = 1
    header_written = False
    max_col = 17  # Default, will be updated from first file

    # Styles for separator and grand summary
    separator_fill = PatternFill(start_color='E0E0E0', end_color='E0E0E0', fill_type='solid')
    summary_font = Font(bold=True, size=11)
    error_font = Font(bold=True, color='FF0000')
    ok_font = Font(bold=True, color='008000')

    for file_idx, file_path in enumerate(file_paths):
        if not os.path.exists(file_path):
            return {'status': 'error', 'error': f'File not found: {file_path}'}

        # First pass: read computed values for grand summary
        try:
            wb_data = load_workbook(file_path, data_only=True)
            ws_data = wb_data.active
        except Exception as e:
            return {'status': 'error', 'error': f'Failed to open {file_path}: {str(e)}'}

        invoice_num = ''
        net_total = 0.0
        variance = 0.0
        # Fallback: compute totals from data rows when formulas aren't evaluated
        # (openpyxl data_only=True only returns cached values from Excel saves)
        sum_line_items = 0.0
        invoice_total_from_col_s = 0.0
        summary_labels = {'SUBTOTAL', 'ADJUSTMENTS', 'NET TOTAL', 'VARIANCE CHECK'}

        for r in range(2, ws_data.max_row + 1):
            if not invoice_num:
                inv_cell = ws_data.cell(row=r, column=3).value
                if inv_cell and str(inv_cell).strip():
                    invoice_num = str(inv_cell).strip()

            # Check if this is a summary row (skip from line-item sum)
            is_summary = False
            for label_col in (10, 12):
                label_cell = ws_data.cell(row=r, column=label_col).value
                if label_cell and isinstance(label_cell, str):
                    label_upper = label_cell.upper().strip()
                    if any(kw in label_upper for kw in summary_labels):
                        is_summary = True
                        if 'NET TOTAL' in label_upper:
                            val = ws_data.cell(row=r, column=16).value
                            net_total = float(val) if val else 0.0
                        elif 'VARIANCE CHECK' in label_upper:
                            val = ws_data.cell(row=r, column=16).value
                            variance = float(val) if val else 0.0

            if not is_summary:
                # Sum line-item costs (Col P) for fallback total
                cost_val = ws_data.cell(row=r, column=16).value
                if cost_val and isinstance(cost_val, (int, float)):
                    sum_line_items += cost_val
                # Get InvoiceTotal from Col S (same for all rows in an invoice)
                inv_total_val = ws_data.cell(row=r, column=19).value
                if inv_total_val and isinstance(inv_total_val, (int, float)) and not invoice_total_from_col_s:
                    invoice_total_from_col_s = inv_total_val

        # Fallback: if NET TOTAL formula wasn't evaluated, use InvoiceTotal from Col S
        # (this is the authoritative invoice total from the PDF)
        if net_total == 0.0 and invoice_total_from_col_s > 0:
            net_total = invoice_total_from_col_s
            # Variance = sum of line items - invoice total
            variance = round(sum_line_items - invoice_total_from_col_s, 2)

        invoice_summaries.append({
            'file': os.path.basename(file_path),
            'invoice_num': invoice_num,
            'total': net_total,
            'variance': variance,
        })
        grand_total_cost += net_total
        grand_total_variance += variance
        wb_data.close()

        # Second pass: read with formulas preserved
        try:
            wb = load_workbook(file_path)
            ws = wb.active
        except Exception as e:
            return {'status': 'error', 'error': f'Failed to open {file_path}: {str(e)}'}

        max_col = max(max_col, ws.max_column)

        # Determine row range to copy
        start_row = 1 if not header_written else 2  # Skip header for subsequent files
        end_row = ws.max_row

        # Calculate row offset for formula adjustment
        # Source rows start at start_row, destination rows start at current_row
        row_offset = current_row - start_row

        # Add blank separator between invoices (not before first)
        if header_written:
            for c in range(1, max_col + 1):
                cell = out_ws.cell(row=current_row, column=c)
                cell.fill = separator_fill
            current_row += 1
            # Recalculate offset after separator
            row_offset = current_row - start_row

        # Copy rows from this file, adjusting formula row references
        for r in range(start_row, end_row + 1):
            for c in range(1, ws.max_column + 1):
                src_cell = ws.cell(row=r, column=c)
                dest_cell = out_ws.cell(row=current_row, column=c)

                value = src_cell.value
                # Adjust formula row references
                if isinstance(value, str) and value.startswith('='):
                    value = _shift_formula(value, row_offset)

                dest_cell.value = value
                copy_cell_style(src_cell, dest_cell)

            current_row += 1

        if not header_written:
            header_written = True

        wb.close()

    # Add grand summary section
    current_row += 1  # Blank row

    # Separator before grand summary
    for c in range(1, max_col + 1):
        out_ws.cell(row=current_row, column=c).fill = PatternFill(
            start_color='4472C4', end_color='4472C4', fill_type='solid'
        )
    current_row += 1

    # Grand Summary Header
    header_cell = out_ws.cell(row=current_row, column=12, value='═══ COMBINED GRAND SUMMARY ═══')
    header_cell.font = Font(bold=True, size=12, color='FFFFFF')
    header_cell.fill = PatternFill(start_color='4472C4', end_color='4472C4', fill_type='solid')
    for c in range(1, max_col + 1):
        out_ws.cell(row=current_row, column=c).fill = PatternFill(
            start_color='4472C4', end_color='4472C4', fill_type='solid'
        )
    current_row += 1

    # List each invoice with its totals
    for summary in invoice_summaries:
        label = f"Invoice {summary['invoice_num'] or summary['file']}"
        out_ws.cell(row=current_row, column=12, value=label)
        out_ws.cell(row=current_row, column=12).font = Font(bold=True)

        # Net total
        total_cell = out_ws.cell(row=current_row, column=16, value=summary['total'])
        total_cell.number_format = '"$"#,##0.00'

        # Variance
        if abs(summary['variance']) > 0.01:
            var_cell = out_ws.cell(row=current_row, column=17, value=summary['variance'])
            var_cell.number_format = '"$"#,##0.00'
            var_cell.font = error_font

        current_row += 1

    current_row += 1  # Blank row

    # Grand totals row
    out_ws.cell(row=current_row, column=12, value='GRAND TOTAL')
    out_ws.cell(row=current_row, column=12).font = summary_font
    grand_cell = out_ws.cell(row=current_row, column=16, value=grand_total_cost)
    grand_cell.number_format = '"$"#,##0.00'
    grand_cell.font = summary_font
    current_row += 1

    # Grand variance check
    out_ws.cell(row=current_row, column=12, value='GRAND VARIANCE CHECK')
    var_font = error_font if abs(grand_total_variance) > 0.01 else ok_font
    out_ws.cell(row=current_row, column=12).font = var_font
    grand_var_cell = out_ws.cell(row=current_row, column=16, value=grand_total_variance)
    grand_var_cell.number_format = '"$"#,##0.00'
    grand_var_cell.font = var_font
    current_row += 1

    # File count note
    note_cell = out_ws.cell(row=current_row, column=12, value=f'Combined {len(file_paths)} invoice files')
    note_cell.font = Font(italic=True, color='666666')

    # Copy column widths from first file
    try:
        first_wb = load_workbook(file_paths[0])
        first_ws = first_wb.active
        for col_idx in range(1, first_ws.max_column + 1):
            col_letter = get_column_letter(col_idx)
            if first_ws.column_dimensions[col_letter].width:
                out_ws.column_dimensions[col_letter].width = first_ws.column_dimensions[col_letter].width
        first_wb.close()
    except:
        # Fallback column widths
        col_widths = {
            1: 10, 2: 10, 3: 12, 4: 12, 5: 25, 6: 12, 7: 10, 8: 10, 9: 15,
            10: 40, 11: 10, 12: 45, 13: 8, 14: 8, 15: 12, 16: 14, 17: 14
        }
        for col, width in col_widths.items():
            out_ws.column_dimensions[get_column_letter(col)].width = width

    # ─── OCR Notes Sheet ──────────────────────────────────────
    # When OCR quality data is provided, add a sheet showing raw OCR text
    # per invoice so users can see extraction quality and identify rescans needed.
    any_poor = False
    if ocr_notes:
        ocr_ws = out_wb.create_sheet('OCR Notes')
        ocr_header_font = Font(bold=True, size=11, color='FFFFFF')
        ocr_header_fill = PatternFill(start_color='4472C4', end_color='4472C4', fill_type='solid')
        poor_fill = PatternFill(start_color='FFC7CE', end_color='FFC7CE', fill_type='solid')
        poor_font = Font(bold=True, color='9C0006')
        fair_fill = PatternFill(start_color='FFEB9C', end_color='FFEB9C', fill_type='solid')
        good_fill = PatternFill(start_color='C6EFCE', end_color='C6EFCE', fill_type='solid')
        wrap_align = Alignment(wrap_text=True, vertical='top')

        # Headers
        headers = ['PDF File', 'Quality Score', 'Rating', 'Details', 'OCR Text (first 2000 chars)']
        for c, h in enumerate(headers, 1):
            cell = ocr_ws.cell(row=1, column=c, value=h)
            cell.font = ocr_header_font
            cell.fill = ocr_header_fill

        ocr_ws.column_dimensions['A'].width = 25
        ocr_ws.column_dimensions['B'].width = 14
        ocr_ws.column_dimensions['C'].width = 10
        ocr_ws.column_dimensions['D'].width = 60
        ocr_ws.column_dimensions['E'].width = 100

        for idx, note in enumerate(ocr_notes, 2):
            rating = note.get('rating', 'unknown')
            score = note.get('score', 0)

            ocr_ws.cell(row=idx, column=1, value=note.get('pdf_file', ''))
            score_cell = ocr_ws.cell(row=idx, column=2, value=f"{score}/100")
            rating_cell = ocr_ws.cell(row=idx, column=3, value=rating.upper())
            ocr_ws.cell(row=idx, column=4, value=note.get('details', ''))

            raw_text = note.get('raw_text', '')[:2000]
            text_cell = ocr_ws.cell(row=idx, column=5, value=raw_text)
            text_cell.alignment = wrap_align

            # Color-code by rating
            if rating == 'poor' or rating == 'unusable':
                any_poor = True
                for c in range(1, 6):
                    ocr_ws.cell(row=idx, column=c).fill = poor_fill
                rating_cell.font = poor_font
            elif rating == 'fair':
                for c in range(1, 6):
                    ocr_ws.cell(row=idx, column=c).fill = fair_fill
            elif rating == 'good':
                for c in range(1, 6):
                    ocr_ws.cell(row=idx, column=c).fill = good_fill

        # Add warning row if any poor quality
        if any_poor:
            warn_row = len(ocr_notes) + 3
            warn_cell = ocr_ws.cell(row=warn_row, column=1,
                                     value='WARNING: OCR text quality is too bad for some pages. '
                                           'Consider rescanning and resubmitting the email.')
            warn_cell.font = Font(bold=True, color='FF0000', size=12)
            ocr_ws.merge_cells(start_row=warn_row, start_column=1,
                               end_row=warn_row, end_column=5)

    # Save
    out_wb.save(output_path)

    return {
        'status': 'success',
        'output': output_path,
        'files_combined': len(file_paths),
        'grand_total': grand_total_cost,
        'grand_variance': grand_total_variance,
        'any_poor_ocr': any_poor,
    }


def main():
    parser = argparse.ArgumentParser(description='Combine multiple XLSX invoice files')
    parser.add_argument('--files', required=True, help='JSON array of file paths')
    parser.add_argument('--output', required=True, help='Output file path')

    args = parser.parse_args()

    try:
        file_paths = json.loads(args.files)
    except json.JSONDecodeError as e:
        print(f'Error parsing files JSON: {e}', flush=True)
        exit(1)

    result = combine_xlsx_files(file_paths, args.output)

    if result['status'] == 'success':
        print(f"Combined {result['files_combined']} files into {result['output']}", flush=True)
        print(f"Grand Total: ${result['grand_total']:.2f}", flush=True)
        print(f"Grand Variance: ${result['grand_variance']:.2f}", flush=True)
        exit(0)
    else:
        print(f"Error: {result['error']}", flush=True)
        exit(1)


if __name__ == '__main__':
    main()
