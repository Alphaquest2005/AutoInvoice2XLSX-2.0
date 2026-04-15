"""
Post-generation XLSX validation and LLM-assisted auto-fix.

Detects and fixes:
  1. Missing tariff codes (col F empty/placeholder)
  2. Invalid tariff codes (not 8-digit or not in CET valid codes)
  3. Zero-value items (qty>0 but $0 price, not backordered)
  4. Variance (InvoiceTotal != calculated net total)

Usage:
    from xlsx_validator import validate_and_fix
    summary = validate_and_fix(results, base_dir)
"""

import json
import logging
import os
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# ── Column constants (match bl_xlsx_generator.py) ──────────────────────────
COL_TARIFF = 6        # F = TariffCode
COL_SUPP_DESC = 10    # J = Supplier Item Description
COL_QTY = 11          # K = Quantity
COL_UNIT_PRICE = 12   # L = Per Unit
COL_COST = 15         # O = Cost
COL_TOTAL_COST = 16   # P = Total Cost
COL_INV_TOTAL = 19    # S = InvoiceTotal (row 2)
COL_FREIGHT = 20      # T = Freight (row 2)
COL_INSURANCE = 21    # U = Insurance (row 2)
COL_TAX = 22          # V = Tax (row 2)
COL_DEDUCTION = 23    # W = Deduction (row 2)
COL_PACKAGES = 24     # X = Packages
COL_GROUPBY = 37      # AK = GroupBy (tariff code for grouping)

# Issue type constants
MISSING_TARIFF = 'MISSING_TARIFF'
INVALID_TARIFF = 'INVALID_TARIFF'
ZERO_VALUE = 'ZERO_VALUE'
VARIANCE = 'VARIANCE'
PACKAGE_MISMATCH = 'PACKAGE_MISMATCH'
DUPLICATE_FILE = 'DUPLICATE_FILE'
COLUMN_SPEC = 'COLUMN_SPEC'

# ── Tariff LLM prompt ─────────────────────────────────────────────────────
TARIFF_SYSTEM_PROMPT = """You are a CARICOM CET tariff classification specialist.
Given item descriptions from a commercial invoice, assign the correct 8-digit CARICOM CET tariff code.

Rules:
- Codes must be exactly 8 digits (e.g., 79070090)
- Use end-node codes only (leaf level with duty rates, not parent headings)
- Common chapters: 39=plastics, 42=leather/bags, 61/62=apparel, 63=textiles,
  64=footwear, 73=iron/steel, 79=zinc, 82=tools/cutlery, 84=machinery,
  85=electrical, 90=instruments, 95=toys/sports, 96=miscellaneous
- When multiple codes could apply, choose the most specific one
- For multi-tools or combination items, classify by primary function

Respond with JSON only:
{"classifications": [
  {"row": <row_number>, "code": "<8_digit_code>", "reasoning": "Brief explanation"}
]}"""


def _num(v):
    """Safe numeric conversion."""
    return v if isinstance(v, (int, float)) else 0


# ── Main entry point ───────────────────────────────────────────────────────

def validate_and_fix(results: list, base_dir: str = '.', bl_alloc=None,
                     manifest_meta: dict = None) -> dict:
    """
    Validate all generated XLSX files and auto-fix issues via LLM.

    Args:
        results: List of InvoiceResult objects from the pipeline
        base_dir: Project base directory (for loading CET codes, etc.)
        bl_alloc: BLAllocation object (has .packages as total BL packages)
        manifest_meta: Manifest metadata dict (has 'packages' key)

    Returns:
        Summary dict: {total_issues, fixed, unfixed, per_file: [...]}
    """
    try:
        import openpyxl
    except ImportError:
        logger.warning("openpyxl not available — skipping XLSX validation")
        return {'total_issues': 0, 'fixed': 0, 'unfixed': 0, 'per_file': []}

    if not results:
        return {'total_issues': 0, 'fixed': 0, 'unfixed': 0, 'per_file': []}

    all_file_results = []
    total_issues = 0
    total_fixed = 0
    total_unfixed = 0

    for r in results:
        xlsx_path = r.xlsx_path
        if not xlsx_path or not os.path.exists(xlsx_path):
            continue

        fname = os.path.basename(xlsx_path)
        supplier = r.supplier_info.get('name', '') if hasattr(r, 'supplier_info') else ''

        try:
            wb = openpyxl.load_workbook(xlsx_path)
            ws = wb.active
        except Exception as e:
            logger.warning(f"Cannot open {fname}: {e}")
            continue

        # Phase 1: Detect issues
        issues = _detect_issues(ws, base_dir)

        # For combined multi-invoice XLSX, each block was variance-fixed
        # individually in invoice_processor before combining. The grand
        # variance computed by _detect_issues is not a per-invoice target
        # that variance_fixer can act on, and aggregating has known edge
        # cases with the ADJUSTMENTS-correction parser. Replace the global
        # variance check with a per-block check that only flags blocks
        # with residual variance.
        if getattr(r, '_combined_pdf_paths', None):
            issues = [i for i in issues if i['type'] != VARIANCE]
            block_variance_issues = _detect_combined_block_variance(ws)
            issues.extend(block_variance_issues)

        if not issues:
            all_file_results.append({
                'file': fname,
                'supplier': supplier,
                'issues': [],
                'fixes': [],
                'fixed': 0,
                'unfixed': 0,
            })
            wb.close()
            continue

        total_issues += len(issues)
        fixes = []

        # Phase 2a: Fix tariff issues
        tariff_issues = [i for i in issues if i['type'] in (MISSING_TARIFF, INVALID_TARIFF)]
        if tariff_issues:
            tariff_fixes = _fix_tariff_issues(ws, tariff_issues, r, base_dir)
            fixes.extend(tariff_fixes)
            wb.save(xlsx_path)

        wb.close()

        # Phase 2b: Fix variance issues (uses its own wb open/save)
        # Skip for combined multi-invoice XLSX: variance_fixer is designed
        # for single-invoice files and will scribble corrections into the
        # wrong block. Per-invoice variance fixing already ran in
        # invoice_processor before the files were combined.
        is_combined = bool(getattr(r, '_combined_pdf_paths', None))
        variance_issues = [i for i in issues if i['type'] == VARIANCE]
        zero_value_issues = [i for i in issues if i['type'] == ZERO_VALUE]
        if (variance_issues or zero_value_issues) and not is_combined:
            variance_amount = variance_issues[0]['variance'] if variance_issues else 0
            var_fix = _fix_variance_issues(xlsx_path, r, variance_amount)
            if var_fix:
                fixes.append(var_fix)

        # Phase 3: Re-validate
        try:
            wb = openpyxl.load_workbook(xlsx_path)
            ws = wb.active
            remaining = _detect_issues(ws, base_dir)
            if getattr(r, '_combined_pdf_paths', None):
                remaining = [i for i in remaining if i['type'] != VARIANCE]
                remaining.extend(_detect_combined_block_variance(ws))
            wb.close()
        except Exception:
            remaining = issues  # couldn't re-check

        fixed_count = len(issues) - len(remaining)
        unfixed_count = len(remaining)
        total_fixed += fixed_count
        total_unfixed += unfixed_count

        all_file_results.append({
            'file': fname,
            'supplier': supplier,
            'issues': issues,
            'fixes': fixes,
            'remaining': remaining,
            'fixed': fixed_count,
            'unfixed': unfixed_count,
        })

    # Phase 4: Cross-file package total check
    pkg_check = _check_package_totals(results, bl_alloc, manifest_meta)

    # Phase 4b: Duplicate file check
    dup_check = _check_duplicates(results)

    # Phase 5: Print report
    _print_report(all_file_results, pkg_check, dup_check)

    dup_issues = len(dup_check.get('duplicates', []))
    return {
        'total_issues': total_issues + (1 if pkg_check.get('mismatch') else 0) + dup_issues,
        'fixed': total_fixed,
        'unfixed': total_unfixed + (1 if pkg_check.get('mismatch') else 0) + dup_issues,
        'per_file': all_file_results,
        'package_check': pkg_check,
        'duplicate_check': dup_check,
    }


# ── Issue detection ────────────────────────────────────────────────────────

def _detect_issues(ws, base_dir: str) -> list:
    """
    Scan XLSX data rows for all issue types.
    Skips formula/summary rows (supports combined XLSX with per-invoice sections).
    """
    issues = []
    cet_codes = _load_cet_codes(base_dir)

    FORMULA_LABELS = {'Subtotal', 'Adjustments', 'Net Total', 'Variance Check',
                      'Subtotal Grouped', 'Subtotal Details', 'Group Verification',
                      'Grand Subtotal', 'Grand Adjustments', 'Grand Net Total',
                      'Grand Invoice Total', 'Grand Variance',
                      # Full uppercase labels used in generated XLSX
                      'SUBTOTAL (GROUPED)', 'SUBTOTAL (DETAILS)', 'GROUP VERIFICATION',
                      'ADJUSTMENTS', 'NET TOTAL', 'INVOICE TOTAL', 'VARIANCE CHECK',
                      'TOTAL INTERNAL FREIGHT', 'TOTAL INSURANCE',
                      'TOTAL OTHER COST', 'TOTAL DEDUCTION',
                      'GRAND SUBTOTAL (GROUPED)', 'GRAND SUBTOTAL (DETAILS)',
                      'GRAND VARIANCE CHECK'}

    # Duty estimation section labels — these are informational rows, not item data
    DUTY_ESTIMATION_PREFIXES = (
        'DUTY ESTIMATION', 'CIF ', 'CET ', 'CSC ', 'VAT ',
        'ESTIMATED TOTAL', 'CLIENT DECLARED', 'DUTY VARIANCE',
        'IMPLIED CET', 'Effective Duty', 'CET MISMATCH', '\u26a0',
        # Reference/combined variance section labels (informational, not item data)
        'REFERENCE SUBTOTAL', 'REFERENCE FREIGHT', 'REFERENCE INSURANCE',
        'REFERENCE OTHER COST', 'REFERENCE DEDUCTION', 'REFERENCE ADJUSTMENTS',
        'REFERENCE NET TOTAL', 'COMBINED ', 'FULL INVOICE', 'FULL ADJUSTMENTS',
        'Items on other',
    )

    for row in range(2, ws.max_row + 1):
        # Skip formula rows and label rows (combined XLSX has these between invoices)
        tc_val = ws.cell(row, COL_TOTAL_COST).value
        if isinstance(tc_val, str) and tc_val.startswith('='):
            continue
        # Check both column J (COL_SUPP_DESC) and column L for summary labels
        label_j = ws.cell(row, COL_SUPP_DESC).value
        label_l = ws.cell(row, 12).value
        if isinstance(label_j, str) and label_j.strip() in FORMULA_LABELS:
            continue
        if isinstance(label_l, str) and label_l.strip() in FORMULA_LABELS:
            continue
        # Skip invoice-level metadata rows (notes, totals appended by parser)
        if isinstance(label_j, str) and label_j.strip().startswith('INVOICE NOTES'):
            continue
        # Skip duty estimation section rows (informational, not item data)
        if isinstance(label_j, str) and any(label_j.strip().startswith(p) for p in DUTY_ESTIMATION_PREFIXES):
            continue

        tariff = str(ws.cell(row, COL_TARIFF).value or '').strip()
        desc = str(ws.cell(row, COL_SUPP_DESC).value or '').strip()
        qty = _num(ws.cell(row, COL_QTY).value)
        unit_price = _num(ws.cell(row, COL_COST).value)
        total_cost = _num(ws.cell(row, COL_TOTAL_COST).value)

        # Skip rows with no description (empty/padding rows)
        if not desc:
            continue

        # 1. MISSING_TARIFF
        if not tariff or tariff == 'None' or tariff == '00000000':
            issues.append({
                'type': MISSING_TARIFF,
                'row': row,
                'current': tariff,
                'description': desc,
                'qty': qty,
                'total_cost': total_cost,
                'why': f'No tariff code assigned. Customs requires a valid 8-digit HS code.',
                'how': f'Classify "{desc[:60]}" using CARICOM CET schedule based on material and function.',
            })

        # 2. INVALID_TARIFF (only if not already flagged as missing)
        elif len(tariff) != 8 or not tariff.isdigit():
            issues.append({
                'type': INVALID_TARIFF,
                'row': row,
                'current': tariff,
                'description': desc,
                'qty': qty,
                'total_cost': total_cost,
                'why': f'Tariff code must be exactly 8 digits. Got: "{tariff}" ({len(tariff)} chars).',
                'how': f'Provide the full 8-digit CARICOM CET end-node code for "{desc[:60]}".',
            })
        elif cet_codes and tariff not in cet_codes:
            issues.append({
                'type': INVALID_TARIFF,
                'row': row,
                'current': tariff,
                'description': desc,
                'qty': qty,
                'total_cost': total_cost,
                'why': f'Code {tariff} not in CARICOM CET valid codes list (not an end-node code).',
                'how': f'Find the correct end-node under heading {tariff[:4]}.{tariff[4:6]}.',
            })

        # 3. ZERO_VALUE (qty > 0 but price = 0, not backordered)
        if qty > 0 and total_cost == 0 and unit_price == 0:
            # Skip items that look like free/promo/backordered
            desc_lower = desc.lower()
            if not any(kw in desc_lower for kw in ('free', 'promo', 'sample', 'backorder', 'b/o', 'cancel')):
                issues.append({
                    'type': ZERO_VALUE,
                    'row': row,
                    'description': desc,
                    'qty': qty,
                    'why': f'Item has qty={qty} but $0.00 unit price and $0.00 total. Likely OCR missed the price.',
                    'how': f'Look for "{desc[:40]}" price in the invoice text. Set unit_price and total_cost = qty * unit_price.',
                })

    # 4. COLUMN_SPEC: columns.yaml conformance check
    # Per columns.yaml:
    #   Col L (12, Per Unit): group = "${category_name} (N items)", detail = "    ${description}" — always text
    #   Col O (15, Cost):     group = total_cost/sum_quantity, detail = unit_price — always numeric
    for row in range(2, ws.max_row + 1):
        tc_val = ws.cell(row, COL_TOTAL_COST).value
        if isinstance(tc_val, str) and tc_val.startswith('='):
            continue
        label = ws.cell(row, 12).value
        if isinstance(label, str) and label.strip() in FORMULA_LABELS:
            continue
        desc = str(ws.cell(row, COL_SUPP_DESC).value or '').strip()
        if not desc:
            continue

        per_unit_val = ws.cell(row, COL_UNIT_PRICE).value  # Col L
        cost_val = ws.cell(row, COL_COST).value             # Col O

        # Per Unit (col L) must be text, never numeric
        if isinstance(per_unit_val, (int, float)):
            issues.append({
                'type': COLUMN_SPEC,
                'row': row,
                'column': 'L (Per Unit)',
                'expected': 'text (description)',
                'actual': f'numeric ({per_unit_val})',
                'why': f'Col L (Per Unit) must be text per columns.yaml: group="category (N items)", detail="    description". Got numeric value {per_unit_val}.',
                'how': 'Fix bl_xlsx_generator.py to write description text to col L, not unit_price.',
            })

        # Cost (col O) must be numeric, never text (unless empty)
        if cost_val is not None and isinstance(cost_val, str) and cost_val.strip():
            issues.append({
                'type': COLUMN_SPEC,
                'row': row,
                'column': 'O (Cost)',
                'expected': 'numeric (unit price)',
                'actual': f'text ("{cost_val[:30]}")',
                'why': f'Col O (Cost) must be numeric per columns.yaml: group=total_cost/sum_quantity, detail=unit_price. Got text "{cost_val[:30]}".',
                'how': 'Fix bl_xlsx_generator.py to write numeric unit_price to col O.',
            })

    # 5. VARIANCE (invoice-level check)
    # For combined XLSX, sum metadata (S/T/U/V/W) from all invoice sections
    # BL freight is stored in col 38 (BLFreight), NOT in col T, so no
    # exclusion needed — col T only contains invoice-level freight.
    inv_total = 0.0
    freight = 0.0
    insurance = 0.0
    tax = 0.0
    deduction = 0.0
    for row in range(2, ws.max_row + 1):
        s_val = _num(ws.cell(row, COL_INV_TOTAL).value)
        if s_val:
            inv_total += s_val
            freight += _num(ws.cell(row, COL_FREIGHT).value)
            insurance += _num(ws.cell(row, COL_INSURANCE).value)
            tax += _num(ws.cell(row, COL_TAX).value)
            deduction += _num(ws.cell(row, COL_DEDUCTION).value)

    # Detect grouped XLSX: if there's a "SUBTOTAL (GROUPED)" label in column J,
    # then group rows already contain totals and detail rows should be excluded.
    # Plain "SUBTOTAL" does NOT indicate grouped mode — non-grouped XLSX files
    # also have a plain SUBTOTAL summary row.
    is_grouped = False
    for row in range(2, ws.max_row + 1):
        j_val = str(ws.cell(row, COL_SUPP_DESC).value or '')
        if 'SUBTOTAL (GROUPED)' in j_val:
            is_grouped = True
            break

    sum_items = 0.0
    import re as _re
    _group_re = _re.compile(r'\(\d+\s+items?\)$')
    if is_grouped:
        # Grouped mode: only sum group-header rows.
        # Group headers end with "(N items)" in column J (e.g. "slippers (27 items)").
        # Detail rows are sub-items whose totals are already included in the
        # group header's P value — skip them to avoid double-counting.
        for row in range(2, ws.max_row + 1):
            tc = ws.cell(row, COL_TOTAL_COST).value
            if isinstance(tc, str) and tc.startswith('='):
                continue
            j_val = str(ws.cell(row, COL_SUPP_DESC).value or '').strip()
            if j_val in FORMULA_LABELS or not j_val:
                continue
            # Group header: description ends with "(N items)"
            if _group_re.search(j_val):
                if isinstance(tc, (int, float)):
                    sum_items += tc
            # else: detail row — skip (already counted in group total)
    else:
        for row in range(2, ws.max_row + 1):
            tc = ws.cell(row, COL_TOTAL_COST).value
            if isinstance(tc, (int, float)):
                sum_items += tc
            elif isinstance(tc, str) and tc.startswith('='):
                continue  # skip formula rows (combined XLSX has per-invoice formulas)

    adjustments = freight + insurance + tax - deduction

    # The variance fixer writes a correction into the ADJUSTMENTS formula:
    #   =(T2+U2+V2-W2)+<correction>
    # Since openpyxl reads formula strings (not computed values), we must
    # parse the correction term and include it in the adjustments total.
    # This keeps the validator in sync with the formula that Excel evaluates.
    for row in range(2, ws.max_row + 1):
        j_val = str(ws.cell(row, COL_SUPP_DESC).value or '').strip()
        if j_val in ('ADJUSTMENTS', 'Adjustments'):
            adj_formula = ws.cell(row, COL_TOTAL_COST).value
            if isinstance(adj_formula, str) and adj_formula.startswith('='):
                # Extract correction after the base formula's closing ')'
                # e.g. "=(T2+U2+V2-W2)+-30.84" → correction = -30.84
                paren_idx = adj_formula.rfind(')')
                if paren_idx >= 0 and paren_idx < len(adj_formula) - 1:
                    tail = adj_formula[paren_idx + 1:]
                    # tail is like "+{value}" or "+-{value}"
                    # Evaluate by summing all numeric terms
                    for num_match in _re.finditer(r'[+\-]?\s*\d+\.?\d*', tail):
                        try:
                            adjustments += float(num_match.group().replace(' ', ''))
                        except ValueError:
                            pass
            break

    net_total = round(sum_items + adjustments, 2)
    variance = round(inv_total - net_total, 2) if inv_total else 0

    if abs(variance) > 0.01:
        issues.append({
            'type': VARIANCE,
            'variance': variance,
            'invoice_total': inv_total,
            'sum_items': sum_items,
            'freight': freight,
            'insurance': insurance,
            'tax': tax,
            'deduction': deduction,
            'why': f'Variance ${variance:+.2f}. InvoiceTotal(${inv_total:.2f}) != NetTotal(${net_total:.2f}).',
            'how': 'Check invoice text for unextracted freight, tax, fees, or misread item prices.',
        })

    return issues


_COMBINED_FORMULA_LABELS = {
    'SUBTOTAL (GROUPED)', 'SUBTOTAL (DETAILS)', 'GROUP VERIFICATION',
    'ADJUSTMENTS', 'NET TOTAL', 'INVOICE TOTAL', 'VARIANCE CHECK',
    'TOTAL INTERNAL FREIGHT', 'TOTAL INSURANCE',
    'TOTAL OTHER COST', 'TOTAL DEDUCTION',
    'GRAND SUBTOTAL (GROUPED)', 'GRAND SUBTOTAL (DETAILS)',
    'GRAND ADJUSTMENTS', 'GRAND NET TOTAL', 'GRAND INVOICE TOTAL',
    'GRAND VARIANCE CHECK', 'GRAND VARIANCE',
}


def _detect_combined_block_variance(ws) -> list:
    """
    Per-block variance check for combined multi-invoice XLSX.

    Uses each block's ADJUSTMENTS row as an anchor: its formula
    ``=(T{r}+U{r}+V{r}-W{r})[+correction]`` references the block's header
    row ``r``. Between the current block's header and the next block's
    header (or the ADJUSTMENTS row for the last block) we sum the item
    rows in col P, skipping formula rows, blank rows, and known summary
    labels. Compare against S{r} (invoice total) minus the adjustments
    (including any variance-fixer correction term).

    Blocks that already balance — because invoice_processor ran
    variance_fixer per-invoice before combining — produce no issue.
    variance_fixer must NOT be re-invoked on combined files because its
    search-from-bottom logic scribbles corrections into the wrong block.
    """
    import re as _re

    issues = []
    # Find block anchors via ADJUSTMENTS rows. Each has a formula whose
    # argument is the block's header row number.
    adj_re = _re.compile(r'^=\(T(\d+)\+U\d+\+V\d+-W\d+\)')
    blocks = []  # list of (header_row, adjustments_row)
    for row in range(2, ws.max_row + 1):
        j_val = str(ws.cell(row, COL_SUPP_DESC).value or '').strip().upper()
        if j_val == 'ADJUSTMENTS':
            tc = ws.cell(row, COL_TOTAL_COST).value
            if isinstance(tc, str):
                m = adj_re.match(tc)
                if m:
                    header_row = int(m.group(1))
                    blocks.append((header_row, row))

    if len(blocks) < 2:
        return issues  # not a multi-block combined file

    for block_idx, (header, adj_row) in enumerate(blocks, 1):
        s_val = _num(ws.cell(header, COL_INV_TOTAL).value)
        freight = _num(ws.cell(header, COL_FREIGHT).value)
        insurance = _num(ws.cell(header, COL_INSURANCE).value)
        tax = _num(ws.cell(header, COL_TAX).value)
        deduction = _num(ws.cell(header, COL_DEDUCTION).value)
        adjustments = freight + insurance + tax - deduction

        # Parse any variance-fixer correction term on the ADJUSTMENTS formula
        adj_formula = ws.cell(adj_row, COL_TOTAL_COST).value
        if isinstance(adj_formula, str) and adj_formula.startswith('='):
            paren_idx = adj_formula.rfind(')')
            if paren_idx >= 0 and paren_idx < len(adj_formula) - 1:
                tail = adj_formula[paren_idx + 1:]
                for m in _re.finditer(r'[+\-]?\s*\d+\.?\d*', tail):
                    try:
                        adjustments += float(m.group().replace(' ', ''))
                    except ValueError:
                        pass

        # Sum item costs for this block. The generator produces grouped
        # XLSX where row `header` is a group header and subsequent rows
        # are detail rows belonging to that header (detail P values are
        # already rolled up into the header's P). The SUBTOTAL (GROUPED)
        # formula is the source of truth — e.g. "=P2" or "=P124+P126" —
        # so we parse it and sum the referenced P cells.
        sum_items = 0.0
        grouped_ref_re = _re.compile(r'P(\d+)')
        found_grouped = False
        for row in range(header, adj_row):
            j_val = str(ws.cell(row, COL_SUPP_DESC).value or '').strip().upper()
            if j_val == 'SUBTOTAL (GROUPED)':
                grouped_formula = ws.cell(row, COL_TOTAL_COST).value
                if isinstance(grouped_formula, str) and grouped_formula.startswith('='):
                    for m in grouped_ref_re.finditer(grouped_formula):
                        ref_row = int(m.group(1))
                        ref_val = ws.cell(ref_row, COL_TOTAL_COST).value
                        if isinstance(ref_val, (int, float)):
                            sum_items += ref_val
                found_grouped = True
                break
        if not found_grouped:
            # Ungrouped block — sum non-formula item rows directly.
            for row in range(header, adj_row):
                j_val = str(ws.cell(row, COL_SUPP_DESC).value or '').strip()
                if j_val.upper() in _COMBINED_FORMULA_LABELS:
                    continue
                p_val = ws.cell(row, COL_TOTAL_COST).value
                if isinstance(p_val, str) and p_val.startswith('='):
                    continue
                if isinstance(p_val, (int, float)):
                    sum_items += p_val

        net_total = round(sum_items + adjustments, 2)
        variance = round(s_val - net_total, 2)
        # Match invoice_processor's $0.02 tolerance (covers cent rounding
        # between grouped subtotal and detail sum).
        if abs(variance) > 0.02:
            issues.append({
                'type': VARIANCE,
                'variance': variance,
                'invoice_total': s_val,
                'sum_items': sum_items,
                'freight': freight,
                'insurance': insurance,
                'tax': tax,
                'deduction': deduction,
                'block': block_idx,
                'block_header_row': header,
                'block_adjustments_row': adj_row,
                'why': (f'Block {block_idx} (row {header}) variance ${variance:+.2f}. '
                        f'InvoiceTotal(${s_val:.2f}) != NetTotal(${net_total:.2f}).'),
                'how': ('Open source PDF for this invoice and rerun pipeline — '
                        'per-invoice variance_fixer should resolve before combining.'),
            })

    return issues


# ── Tariff fix ─────────────────────────────────────────────────────────────

def _fix_tariff_issues(ws, issues: list, result, base_dir: str) -> list:
    """
    Fix tariff issues in two passes:
      1. Try classifier.validate_and_correct_code() for invalid codes
      2. LLM call for remaining unresolved items

    Returns list of fix records: {row, old, new, method, description}
    """
    fixes = []
    unresolved = []

    # Pass 1: Try CET auto-correction for INVALID codes
    try:
        from classifier import validate_and_correct_code
    except ImportError:
        validate_and_correct_code = None

    for issue in issues:
        if issue['type'] == INVALID_TARIFF and validate_and_correct_code:
            current = issue['current']
            corrected = validate_and_correct_code(current, base_dir)
            if corrected != current and corrected and corrected != 'UNKNOWN':
                # Apply CET correction
                ws.cell(row=issue['row'], column=COL_TARIFF).value = corrected
                ws.cell(row=issue['row'], column=COL_GROUPBY).value = corrected
                fixes.append({
                    'type': INVALID_TARIFF,
                    'row': issue['row'],
                    'old': current,
                    'new': corrected,
                    'method': 'CET auto-correction',
                    'description': issue['description'][:50],
                })
                continue

        # Still unresolved
        unresolved.append(issue)

    if not unresolved:
        return fixes

    # Pass 2: LLM classification for remaining items
    llm_fixes = _llm_classify_items(ws, unresolved, result, base_dir)
    fixes.extend(llm_fixes)

    return fixes


def _llm_classify_items(ws, unresolved: list, result, base_dir: str) -> list:
    """Call LLM to classify unresolved tariff items. Validate codes before applying."""
    try:
        from core.llm_client import get_llm_client
        llm = get_llm_client()
    except Exception as e:
        logger.warning(f"LLM client unavailable for tariff fix: {e}")
        return []

    cet_codes = _load_cet_codes(base_dir)

    # Collect already-classified items as reference examples
    classified_items = []
    for row in range(2, ws.max_row + 1):
        tc_val = ws.cell(row, COL_TOTAL_COST).value
        if isinstance(tc_val, str) and tc_val.startswith('='):
            continue  # skip formula rows (combined XLSX)
        tariff = str(ws.cell(row, COL_TARIFF).value or '').strip()
        desc = str(ws.cell(row, COL_SUPP_DESC).value or '').strip()
        if tariff and len(tariff) == 8 and tariff.isdigit() and desc:
            # Only include if it's actually valid (in CET or we don't have CET list)
            if not cet_codes or tariff in cet_codes:
                classified_items.append({'row': row, 'description': desc[:60], 'code': tariff})

    # Build prompt
    supplier = ''
    invoice_num = ''
    invoice_text = ''
    if hasattr(result, 'supplier_info'):
        supplier = result.supplier_info.get('name', '')
    if hasattr(result, 'invoice_num'):
        invoice_num = result.invoice_num
    if hasattr(result, 'invoice_data'):
        invoice_text = result.invoice_data.get('raw_text', '')[:3000]

    user_lines = [f"Invoice: {invoice_num} from {supplier}", ""]
    user_lines.append("Items needing tariff classification:")
    for issue in unresolved:
        user_lines.append(f"  Row {issue['row']}: \"{issue['description'][:60]}\"  qty={issue.get('qty', '?')}  ${issue.get('total_cost', 0):.2f}")
        user_lines.append(f"    WHY: {issue['why']}")
        user_lines.append(f"    HOW: {issue['how']}")
        user_lines.append("")

    if classified_items:
        user_lines.append("Already classified items from this invoice (for reference):")
        for ci in classified_items[:15]:
            user_lines.append(f"  Row {ci['row']}: \"{ci['description']}\" -> {ci['code']}")
        user_lines.append("")

    if invoice_text:
        user_lines.append("Invoice text (for additional context):")
        user_lines.append(invoice_text)

    user_message = '\n'.join(user_lines)

    # LLM call
    cache_extra = f"tariff_fix:{invoice_num}:{len(unresolved)}"
    try:
        response = llm.call_json(
            user_message=user_message,
            system_prompt=TARIFF_SYSTEM_PROMPT,
            cache_key_extra=cache_extra,
        )
    except Exception as e:
        logger.warning(f"LLM tariff classification call failed: {e}")
        return []

    if not response or 'classifications' not in response:
        logger.warning("LLM returned no classifications")
        return []

    # Apply validated fixes
    fixes = []
    unresolved_rows = {i['row']: i for i in unresolved}

    for cls in response.get('classifications', []):
        row = cls.get('row')
        code = str(cls.get('code', '')).strip()
        reasoning = cls.get('reasoning', '')

        if row not in unresolved_rows:
            continue

        # Validate: must be 8 digits
        if len(code) != 8 or not code.isdigit():
            logger.warning(f"LLM returned invalid code '{code}' for row {row}, skipping")
            continue

        # Validate against CET if available
        if cet_codes and code not in cet_codes:
            # Try suffix correction
            fallback = code[:6] + '00'
            if fallback in cet_codes:
                logger.info(f"LLM code {code} not in CET, using fallback {fallback}")
                code = fallback
            else:
                logger.warning(f"LLM code '{code}' not in CET valid codes for row {row}, skipping")
                continue

        issue = unresolved_rows[row]
        ws.cell(row=row, column=COL_TARIFF).value = code
        ws.cell(row=row, column=COL_GROUPBY).value = code
        fixes.append({
            'type': issue['type'],
            'row': row,
            'old': issue['current'],
            'new': code,
            'method': f'LLM: {reasoning[:60]}',
            'description': issue['description'][:50],
        })

    return fixes


# ── Variance fix ───────────────────────────────────────────────────────────

def _fix_variance_issues(xlsx_path: str, result, variance: float) -> Optional[dict]:
    """
    Delegate variance fixing to the existing variance_fixer module.
    Also handles zero-value items (they contribute to variance).
    """
    try:
        from workflow.variance_fixer import fix_variance
    except ImportError:
        logger.warning("variance_fixer module not available")
        return None

    invoice_text = ''
    if hasattr(result, 'invoice_data'):
        invoice_text = result.invoice_data.get('raw_text', '')

    if not invoice_text:
        logger.warning("No invoice text available for variance fix")
        return None

    try:
        fix_result = fix_variance(
            xlsx_path=xlsx_path,
            invoice_text=invoice_text,
            current_variance=variance,
        )
        if fix_result.get('success'):
            return {
                'type': VARIANCE,
                'old_variance': variance,
                'new_variance': fix_result.get('new_variance', 0),
                'fixes_applied': fix_result.get('fixes_applied', 0),
                'method': f"variance_fixer: {fix_result.get('analysis', '')[:60]}",
            }
        else:
            logger.warning(f"Variance fix failed: {fix_result.get('error', 'unknown')}")
            return None
    except Exception as e:
        logger.warning(f"Variance fix error: {e}")
        return None


# ── Duplicate file check ──────────────────────────────────────────────────

def _check_duplicates(results: list) -> dict:
    """
    Check for duplicate XLSX and PDF files in the output.

    Detects:
      1. Duplicate filenames — multiple invoices producing the same output file
      2. Duplicate content — different source PDFs producing XLSX with the same
         invoice number, supplier, and matching item data (overwrite / double-count)
      3. Stale files — leftover XLSX/PDF from a previous run

    Returns:
        {duplicates: [...], content_duplicates: [...], stale: [...], has_issues: bool}
    """
    try:
        import openpyxl
    except ImportError:
        return {'duplicates': [], 'content_duplicates': [], 'stale': [],
                'has_issues': False}

    # ── 1. Duplicate filenames ──
    seen_xlsx = {}
    seen_pdf = {}
    output_dir = None

    for r in results:
        src_pdf = getattr(r, 'pdf_file', '') or ''
        xlsx_path = r.xlsx_path
        pdf_out = getattr(r, 'pdf_output_path', '') or ''

        if xlsx_path:
            if output_dir is None:
                output_dir = os.path.dirname(xlsx_path)
            basename = os.path.basename(xlsx_path)
            seen_xlsx.setdefault(basename, []).append(src_pdf)

        if pdf_out and os.path.exists(pdf_out):
            basename = os.path.basename(pdf_out)
            seen_pdf.setdefault(basename, []).append(src_pdf)

    duplicates = []
    for fname, sources in seen_xlsx.items():
        if len(sources) > 1:
            duplicates.append({'filename': fname, 'type': 'xlsx', 'sources': sources})
    for fname, sources in seen_pdf.items():
        if len(sources) > 1:
            duplicates.append({'filename': fname, 'type': 'pdf', 'sources': sources})

    # ── 2. Duplicate content ──
    # Detect duplicate invoices/documents (same invoice number + supplier).
    # This does NOT flag product overlap between different invoices.
    content_duplicates = []
    file_fingerprints = []  # [(xlsx_basename, src_pdf, inv_num, supplier, total)]

    for r in results:
        xlsx_path = r.xlsx_path
        if not xlsx_path or not os.path.exists(xlsx_path):
            continue
        src_pdf = getattr(r, 'pdf_file', '') or ''
        basename = os.path.basename(xlsx_path)

        try:
            wb = openpyxl.load_workbook(xlsx_path, read_only=True)
            ws = wb.active
            inv_num = ws.cell(row=2, column=3).value    # C = Supplier Invoice#
            supplier = ws.cell(row=2, column=27).value   # AA = Supplier Name
            inv_total = _num(ws.cell(row=2, column=COL_INV_TOTAL).value)
            wb.close()

            file_fingerprints.append((basename, src_pdf, str(inv_num),
                                      str(supplier), inv_total))
        except Exception as e:
            logger.debug(f"Duplicate content check skipped for {basename}: {e}")

    # Compare all pairs — only flag same invoice number from same supplier
    for i in range(len(file_fingerprints)):
        for j in range(i + 1, len(file_fingerprints)):
            f1 = file_fingerprints[i]
            f2 = file_fingerprints[j]
            name1, src1, inv1, sup1, tot1 = f1
            name2, src2, inv2, sup2, tot2 = f2

            # Same invoice number from same supplier = duplicate document
            if inv1 and inv2 and inv1 == inv2 and sup1 == sup2:
                content_duplicates.append({
                    'type': 'same_invoice_number',
                    'invoice_num': inv1,
                    'supplier': sup1,
                    'files': [name1, name2],
                    'sources': [src1, src2],
                })

    # ── 2b. Auto-resolve content duplicates ──
    # When two source PDFs produce identical content and one filename prefix is
    # contained in the other (e.g. "bmgrn INV_..." vs "bmgrn tbm INV_..."),
    # the shorter-prefix version is the consignee-specific copy — keep it and
    # remove the other.
    resolved_dups = []
    for cd in content_duplicates:
        if len(cd.get('sources', [])) != 2:
            continue
        src_a, src_b = cd['sources']
        file_a, file_b = cd['files']
        # Compare the prefix before the invoice number (e.g. "bmgrn " vs "bmgrn tbm ")
        prefix_a = src_a.split('INV')[0].strip().lower() if 'INV' in src_a else src_a.lower()
        prefix_b = src_b.split('INV')[0].strip().lower() if 'INV' in src_b else src_b.lower()

        remove_file = None
        keep_src = None
        if prefix_a and prefix_b and prefix_a != prefix_b:
            if prefix_a in prefix_b:
                # prefix_a is shorter / base variant — keep it, remove b
                remove_file = file_b
                keep_src = src_a
            elif prefix_b in prefix_a:
                remove_file = file_a
                keep_src = src_b

        if remove_file:
            # Remove the duplicate from results
            for idx, r in enumerate(results):
                if r.xlsx_path and os.path.basename(r.xlsx_path) == remove_file:
                    # Delete the duplicate XLSX and PDF from output
                    if os.path.exists(r.xlsx_path):
                        os.remove(r.xlsx_path)
                        logger.info(f"Auto-resolved duplicate: removed {remove_file} (keeping {keep_src})")
                    pdf_out = getattr(r, 'pdf_output_path', '')
                    if pdf_out and os.path.exists(pdf_out):
                        os.remove(pdf_out)
                    results.pop(idx)
                    break
            resolved_dups.append(cd)
            print(f"    AUTO-RESOLVED duplicate: removed {remove_file} (keeping {keep_src})")

    # Remove resolved duplicates from the issue list
    for rd in resolved_dups:
        content_duplicates.remove(rd)

    # ── 3. Stale files ──
    stale = []
    if output_dir and os.path.isdir(output_dir):
        expected_files = set()
        for r in results:
            if r.xlsx_path:
                expected_files.add(os.path.basename(r.xlsx_path))
            pdf_out = getattr(r, 'pdf_output_path', '') or ''
            if pdf_out:
                expected_files.add(os.path.basename(pdf_out))
            # Combined entries stash per-invoice source PDFs here; they are
            # produced by the current run even though the individual XLSX
            # files were merged away.
            combined_pdfs = getattr(r, '_combined_pdf_paths', None) or []
            for cp in combined_pdfs:
                expected_files.add(os.path.basename(cp))
        expected_files.add('_email_params.json')

        for f in os.listdir(output_dir):
            fp = os.path.join(output_dir, f)
            if not os.path.isfile(fp):
                continue
            if f in expected_files:
                continue
            if f.endswith('-BL.pdf'):
                continue
            if f.endswith('.xlsx') or f.endswith('.pdf'):
                stale.append(f)

    return {
        'duplicates': duplicates,
        'content_duplicates': content_duplicates,
        'stale': stale,
        'has_issues': bool(duplicates or content_duplicates or stale),
    }


# ── Package total check ───────────────────────────────────────────────────

def _check_package_totals(results: list, bl_alloc=None, manifest_meta: dict = None) -> dict:
    """
    Verify that the sum of Packages (col X) across all XLSX files matches the
    expected total from BL or manifest.

    Priority: BL packages → manifest packages → 1 per entry (default).

    Returns:
        {expected, actual, source, per_file: [{file, packages}], mismatch: bool}
    """
    try:
        import openpyxl
    except ImportError:
        return {}

    # Determine expected total and source
    expected = None
    source = 'default (1 per entry)'

    if bl_alloc and getattr(bl_alloc, 'packages', None):
        try:
            expected = int(bl_alloc.packages)
            source = 'BL'
        except (ValueError, TypeError):
            pass

    if expected is None and manifest_meta and manifest_meta.get('packages'):
        try:
            expected = int(manifest_meta['packages'])
            source = 'manifest'
        except (ValueError, TypeError):
            pass

    if expected is None:
        # Default: 1 package per XLSX entry
        expected = len([r for r in results if r.xlsx_path and os.path.exists(r.xlsx_path)])
        source = 'default (1 per entry)'

    # Read actual packages from each XLSX col X (row 2)
    per_file = []
    actual_total = 0
    for r in results:
        xlsx_path = r.xlsx_path
        if not xlsx_path or not os.path.exists(xlsx_path):
            continue
        fname = os.path.basename(xlsx_path)
        try:
            wb = openpyxl.load_workbook(xlsx_path, read_only=True)
            ws = wb.active
            pkg_val = ws.cell(row=2, column=COL_PACKAGES).value
            wb.close()
            pkg = int(pkg_val) if pkg_val is not None else 1
        except Exception:
            pkg = 1
        per_file.append({'file': fname, 'packages': pkg})
        actual_total += pkg

    mismatch = actual_total != expected

    return {
        'expected': expected,
        'actual': actual_total,
        'source': source,
        'per_file': per_file,
        'mismatch': mismatch,
    }


# ── Report ─────────────────────────────────────────────────────────────────

def _print_report(all_results: list, pkg_check: dict = None,
                   dup_check: dict = None) -> None:
    """Print comprehensive validation report with before/after states."""
    if not all_results:
        return

    total_files = len(all_results)
    total_issues = sum(len(r.get('issues', [])) for r in all_results)
    total_fixed = sum(r.get('fixed', 0) for r in all_results)
    total_unfixed = sum(r.get('unfixed', 0) for r in all_results)
    clean_files = sum(1 for r in all_results if not r.get('issues'))

    print()
    print("XLSX VALIDATION")
    print("-" * 80)

    for fr in all_results:
        fname = fr['file']
        supplier = fr.get('supplier', '')
        issues = fr.get('issues', [])
        fixes = fr.get('fixes', [])
        remaining = fr.get('remaining', [])

        label = f"  {fname}"
        if supplier:
            label += f" ({supplier})"

        if not issues:
            print(f"{label}")
            print(f"    No issues found.")
            print()
            continue

        print(f"{label}")

        # Build fix lookup for display
        fix_by_row = {}
        for f in fixes:
            if isinstance(f, dict) and 'row' in f:
                fix_by_row[f['row']] = f

        remaining_types = set()
        for ri in remaining:
            remaining_types.add((ri.get('type'), ri.get('row', 0)))

        for issue in issues:
            itype = issue['type']
            row = issue.get('row', 0)

            if itype in (MISSING_TARIFF, INVALID_TARIFF):
                desc = issue.get('description', '')[:30]
                current = issue.get('current', '?')
                fix = fix_by_row.get(row)

                if fix:
                    new_code = fix.get('new', '?')
                    method = fix.get('method', '')
                    print(f"    Row {row:>3}  {itype:<16} \"{desc}\" [{current}] -> {new_code} ({method})")
                else:
                    is_remaining = (itype, row) in remaining_types
                    status = "UNFIXED" if is_remaining else "FIXED"
                    print(f"    Row {row:>3}  {itype:<16} \"{desc}\" [{current}]  {status}")

            elif itype == ZERO_VALUE:
                desc = issue.get('description', '')[:30]
                qty = issue.get('qty', 0)
                is_remaining = (itype, row) in remaining_types
                status = "UNFIXED" if is_remaining else "FIXED"
                print(f"    Row {row:>3}  {itype:<16} \"{desc}\" qty={qty} $0.00  {status}")

            elif itype == COLUMN_SPEC:
                col_name = issue.get('column', '?')
                expected = issue.get('expected', '?')
                actual = issue.get('actual', '?')
                print(f"    Row {row:>3}  {itype:<16} {col_name}: expected {expected}, got {actual}")

            elif itype == VARIANCE:
                old_var = issue.get('variance', 0)
                # Find variance fix in fixes list
                var_fix = next((f for f in fixes if isinstance(f, dict) and f.get('type') == VARIANCE), None)
                if var_fix:
                    new_var = var_fix.get('new_variance', 0)
                    print(f"    {itype:<20} ${old_var:+.2f} -> ${new_var:+.2f} ({var_fix.get('method', '')[:40]})")
                else:
                    has_remaining_var = any(ri.get('type') == VARIANCE for ri in remaining)
                    status = "UNFIXED" if has_remaining_var else "FIXED"
                    print(f"    {itype:<20} ${old_var:+.2f}  {status}")

        fixed_count = fr.get('fixed', 0)
        unfixed_count = fr.get('unfixed', 0)
        print(f"    Result: {len(issues)} issues, {fixed_count} fixed, {unfixed_count} unfixed")
        print()

    # Package total check
    if pkg_check and pkg_check.get('per_file'):
        expected = pkg_check.get('expected', '?')
        actual = pkg_check.get('actual', '?')
        source = pkg_check.get('source', '?')
        mismatch = pkg_check.get('mismatch', False)

        print(f"  PACKAGE CHECK (source: {source})")
        for pf in pkg_check['per_file']:
            print(f"    {pf['file']:<30} {pf['packages']:>3} packages")
        if mismatch:
            print(f"    MISMATCH: XLSX total = {actual}, expected = {expected} ({source})")
        else:
            print(f"    PASS: {actual} packages = {expected} ({source})")
        print()

    # Duplicate / stale file check
    if dup_check and dup_check.get('has_issues'):
        print(f"  DUPLICATE / STALE FILE CHECK")
        for dup in dup_check.get('duplicates', []):
            sources = ', '.join(dup['sources']) if dup['sources'] else 'unknown'
            print(f"    DUPLICATE {dup['type'].upper()}: {dup['filename']}  (from: {sources})")
        for cd in dup_check.get('content_duplicates', []):
            dtype = cd['type']
            files = ', '.join(cd['files'])
            sources = ', '.join(cd.get('sources', []))
            if dtype == 'same_invoice_number':
                print(f"    DUPLICATE CONTENT: same invoice #{cd['invoice_num']} ({cd['supplier']})")
                print(f"      Files: {files}")
                print(f"      Source PDFs: {sources}")
            elif dtype == 'identical_items':
                print(f"    DUPLICATE CONTENT: identical items ({cd['item_count']} items)")
                print(f"      Files: {files}")
                print(f"      Source PDFs: {sources}")
            elif dtype == 'high_overlap':
                print(f"    DUPLICATE CONTENT: {cd['pct']} item overlap ({cd['overlap']} of {cd['sizes']})")
                print(f"      Files: {files}")
                print(f"      Source PDFs: {sources}")
        for stale_f in dup_check.get('stale', []):
            print(f"    STALE: {stale_f}  (not produced by current run — leftover from previous run?)")
        print()
    elif dup_check:
        print(f"  DUPLICATE CHECK")
        print(f"    PASS: no duplicate or stale XLSX/PDF files")
        print()

    print("-" * 80)
    if total_issues == 0:
        print(f"  All {total_files} files clean - no issues detected.")
    elif total_unfixed == 0:
        print(f"  {total_files} files checked, {total_issues} issues found, all {total_fixed} fixed.")
    else:
        print(f"  {total_files} files checked, {total_issues} issues found, {total_fixed} fixed, {total_unfixed} unfixed.")
    print()


# ── Shipment Pre-Send Checklist ───────────────────────────────────────────

def _llm_review_email_params(email_params: dict, validation: dict, fail_fn) -> None:
    """
    Use LLM to review email params for obvious data quality issues that
    rule-based checks might miss (e.g. invoice text in address fields,
    garbled OCR in consignee name, nonsensical values).
    """
    try:
        from core.llm_client import get_llm_client
        client = get_llm_client()
    except Exception:
        return  # LLM not available, skip

    # Build a concise summary of what the email will contain
    review_data = {
        'waybill': email_params.get('waybill', ''),
        'consignee_name': email_params.get('consignee_name', ''),
        'consignee_address': str(email_params.get('consignee_address', ''))[:300],
        'consignee_code': email_params.get('consignee_code', ''),
        'packages': email_params.get('packages', ''),
        'weight': email_params.get('weight', ''),
        'freight': email_params.get('freight', ''),
        'total_invoices': email_params.get('total_invoices', ''),
        'man_reg': email_params.get('man_reg', ''),
    }
    # Add attachment filenames
    att_paths = email_params.get('attachment_paths', [])
    review_data['attachment_files'] = [os.path.basename(p) for p in att_paths]

    system_prompt = """You are a shipping data quality reviewer for CARICOM customs declaration emails.

Review the data for SEVERE problems only. Use "block" ONLY for data that is clearly
broken/corrupt (e.g. invoice line items dumped into address field, entirely garbled
consignee name, nonsensical values). Use "warn" for minor issues (typos, unusual
but potentially valid values).

Rules:
1. Consignee name: BLOCK if it contains invoice text, product codes, or is clearly
   not a company name. Short or unusual names are OK (warn at most).
2. Consignee address: BLOCK if it contains invoice data, product listings, or is
   clearly not an address. Minor OCR typos in real addresses are OK (warn at most).
   An EMPTY address is acceptable (warn at most) — some shipments use Simplified
   Declarations which do not include addresses.
3. Waybill: Some BL numbers are short (e.g. "04C", "HBL199462"). Do NOT block
   short waybills — only block if clearly garbled/nonsensical.
4. Weight/packages/freight: Only block if obviously impossible (negative, extremely
   large like >100000).
5. Attachment filenames: Only flag if clearly corrupt names.

IMPORTANT: Do NOT flag OCR typos in otherwise valid data. Do NOT flag unusual but
valid BL numbers. When in doubt, use "warn" not "block".

Respond with ONLY a JSON object:
{"ok": true} if everything looks reasonable, or
{"ok": false, "issues": [{"field": "field_name", "severity": "block"|"warn", "message": "description"}]}"""

    user_msg = f"Review this shipment email data:\n{json.dumps(review_data, indent=2)}"

    response = client.call(user_msg, system_prompt=system_prompt, max_tokens=500)
    if not response:
        return

    # Parse LLM response
    try:
        # Extract JSON from response (may have markdown fencing)
        json_str = response.strip()
        if '```' in json_str:
            json_str = json_str.split('```')[1]
            if json_str.startswith('json'):
                json_str = json_str[4:]
            json_str = json_str.strip()
        result = json.loads(json_str)

        if not result.get('ok', True):
            for issue in result.get('issues', []):
                fail_fn(
                    f'llm_review_{issue.get("field", "unknown")}',
                    issue.get('severity', 'warn'),
                    f'[LLM Review] {issue.get("message", "Data quality issue")}',
                    issue.get('field', ''),
                    str(review_data.get(issue.get('field', ''), '')),
                    'Review the source PDFs and correct the email params.'
                )
    except (json.JSONDecodeError, KeyError, TypeError):
        logger.debug(f"Could not parse LLM review response: {response[:200]}")


def shipment_checklist(email_params: dict, validation: dict = None) -> dict:
    """
    Final gate before sending a shipment email.
    Returns {passed: bool, failures: [{check, severity, message, field, value, fix_hint}]}

    Severity levels:
      - 'block'   : email MUST NOT be sent until fixed
      - 'warn'    : email can be sent but issue should be flagged
    """
    failures = []

    def fail(check, severity, message, field='', value='', fix_hint=''):
        failures.append({
            'check': check,
            'severity': severity,
            'message': message,
            'field': field,
            'value': str(value),
            'fix_hint': fix_hint,
        })

    # ── 1. Waybill / BL Number ──
    waybill = str(email_params.get('waybill', '')).strip()
    if not waybill:
        fail('waybill_missing', 'block',
             'Waybill/BL number is empty. Every shipment must have a BL number.',
             'waybill', waybill,
             'Read the BL PDF to extract the BL number. Look for "B/L No" or "BILL OF LADING".')
    elif len(waybill) < 6:
        fail('waybill_short', 'warn',
             f'Waybill "{waybill}" is unusually short ({len(waybill)} chars). Typical BL numbers are 10+ chars.',
             'waybill', waybill,
             'Verify BL number against the BL PDF.')

    # ── 2. Weight ──
    weight_str = str(email_params.get('weight', '0')).strip()
    try:
        weight = float(weight_str)
    except ValueError:
        weight = 0
    if weight <= 0:
        fail('weight_zero', 'block',
             f'Weight is {weight_str} kg. A shipment cannot weigh 0 kg.',
             'weight', weight_str,
             'Read the BL PDF and look for GRAND TOTAL line with weight in kg. '
             'Update _email_params.json field "weight" with the correct value.')

    # ── 3. Packages ──
    pkg_str = str(email_params.get('packages', '0')).strip()
    try:
        packages = int(pkg_str)
    except ValueError:
        packages = 0
    if packages <= 0:
        fail('packages_zero', 'block',
             f'Packages is {pkg_str}. A shipment must have at least 1 package.',
             'packages', pkg_str,
             'Read the BL PDF GRAND TOTAL line for the package count. '
             'Update _email_params.json field "packages" with the correct value.')

    # ── 4. Freight ──
    freight_str = str(email_params.get('freight', '0')).strip()
    try:
        freight = float(freight_str)
    except ValueError:
        freight = 0
    if freight <= 0:
        fail('freight_zero', 'warn',
             f'Freight is ${freight_str}. Ocean freight is typically > $0.',
             'freight', freight_str,
             'Check the BL cost breakdown for freight charges. '
             'If freight is genuinely $0 (e.g. prepaid/included), this may be OK.')

    # ── 5. Consignee ──
    consignee_name = str(email_params.get('consignee_name', '')).strip()
    CONSIGNEE_JUNK = ('unknown', 'consignee name not found', 'none',
                      'charges will be billed to consignee',
                      'to order', 'to the order of', 'same as above')
    if not consignee_name or consignee_name.lower() in CONSIGNEE_JUNK:
        fail('consignee_missing', 'block',
             f'Consignee name is missing or placeholder: "{consignee_name}".',
             'consignee_name', consignee_name,
             'Read the BL PDF or invoice headers to find the consignee/ship-to name.')

    # Consignee name sanity: should be a short company name, not invoice text
    if consignee_name and len(consignee_name) > 60:
        fail('consignee_name_garbage', 'block',
             f'Consignee name is suspiciously long ({len(consignee_name)} chars) — '
             f'likely contains invoice text instead of a company name.',
             'consignee_name', consignee_name[:80] + '...',
             'Read the BL PDF to find the correct consignee name. '
             'Update _email_params.json field "consignee_name".')

    # Consignee address sanity: should be a short address, not invoice dump
    consignee_address = str(email_params.get('consignee_address', '')).strip()
    if consignee_address and len(consignee_address) > 200:
        fail('consignee_address_garbage', 'block',
             f'Consignee address is suspiciously long ({len(consignee_address)} chars) — '
             f'likely contains invoice text instead of an address.',
             'consignee_address', consignee_address[:80] + '...',
             'Read the BL PDF to find the correct consignee address. '
             'Update _email_params.json field "consignee_address".')

    consignee_code = str(email_params.get('consignee_code', '')).strip()
    if not consignee_code:
        fail('consignee_code_missing', 'warn',
             'Consignee code is empty.',
             'consignee_code', consignee_code,
             'Check config/document_types.json for the consignee code matching this consignee.')

    # ── 6. Invoice count ──
    total_invoices = email_params.get('total_invoices', 0)
    if not total_invoices or int(total_invoices) < 1:
        fail('no_invoices', 'block',
             f'Total invoices is {total_invoices}. At least 1 invoice is required.',
             'total_invoices', str(total_invoices),
             'Check the pipeline output for failed invoice processing.')

    # ── 7. Attachments ──
    attachments = email_params.get('attachment_paths', [])
    if not attachments:
        fail('no_attachments', 'block',
             'No attachment files found. Email must have XLSX/PDF attachments.',
             'attachment_paths', '[]',
             'Check output directory for generated XLSX and PDF files.')
    else:
        def _path_exists(p):
            """Check if path exists, handling Windows paths on WSL."""
            if os.path.exists(p):
                return True
            # On WSL, Windows paths like C:\... need translation to /mnt/c/...
            if len(p) >= 3 and p[1] == ':' and p[2] in ('\\', '/'):
                wsl_path = '/mnt/' + p[0].lower() + '/' + p[3:].replace('\\', '/')
                return os.path.exists(wsl_path)
            return False
        missing = [p for p in attachments if not _path_exists(p)]
        if missing:
            fail('missing_attachments', 'block',
                 f'{len(missing)} attachment file(s) not found on disk.',
                 'attachment_paths', f'{len(missing)} missing',
                 f'Missing files: {", ".join(os.path.basename(p) for p in missing[:5])}')

    # ── 8. Unfixed validation issues ──
    if validation:
        unfixed = validation.get('unfixed', 0)
        total_issues = validation.get('total_issues', 0)
        if unfixed > 0:
            # Count blocking vs warning issues
            tariff_unfixed = 0
            variance_unfixed = 0
            for pf in validation.get('per_file', []):
                for rem in pf.get('remaining', []):
                    if rem.get('type') in ('MISSING_TARIFF', 'INVALID_TARIFF'):
                        tariff_unfixed += 1
                    elif rem.get('type') == 'VARIANCE':
                        variance_unfixed += 1

            if tariff_unfixed > 0:
                # Collect details for LLM context
                tariff_details = []
                for pf in validation.get('per_file', []):
                    for rem in pf.get('remaining', []):
                        if rem.get('type') in ('MISSING_TARIFF', 'INVALID_TARIFF'):
                            tariff_details.append(
                                f'  {pf["file"]} row {rem["row"]}: "{rem.get("description", "")[:50]}" '
                                f'[current: {rem.get("current", "none")}]')
                detail_str = '\n'.join(tariff_details[:20])  # cap at 20 for readability
                fail('unfixed_tariff', 'block',
                     f'{tariff_unfixed} item(s) have invalid/missing tariff codes.',
                     'validation.tariff', str(tariff_unfixed),
                     f'For each item below, use lookup_tariff with the item description to find '
                     f'the correct 8-digit CET end-node code. Then edit the XLSX file: set column F '
                     f'(TariffCode) and column AK (GroupBy) to the new code. After fixing, run '
                     f'validate_xlsx to confirm.\n{detail_str}')

            if variance_unfixed > 0:
                var_details = []
                # Per-block variance issues on a combined multi-invoice XLSX
                # are downgraded to a warning: invoice_processor already ran
                # variance_fixer per-invoice before combining, so any
                # residual variance is an OCR / source-data quality issue
                # that the operator must inspect directly in the combined
                # XLSX. Blocking the email would just prevent them from
                # seeing the problematic attachments.
                has_per_block = False
                for pf in validation.get('per_file', []):
                    for rem in pf.get('remaining', []):
                        if rem.get('type') == 'VARIANCE':
                            if rem.get('block'):
                                has_per_block = True
                            var_details.append(
                                f'  {pf["file"]}: {rem.get("why", "")}')
                detail_str = '\n'.join(var_details[:10])
                severity = 'warn' if has_per_block else 'block'
                fail('unfixed_variance', severity,
                     f'{variance_unfixed} block(s)/file(s) have unresolved variance.',
                     'validation.variance', str(variance_unfixed),
                     f'For each file: read_file the XLSX to see line items and totals (row 2 cols '
                     f'S=InvoiceTotal, T=Freight, U=Insurance, V=Tax, W=Deduction). Read the source '
                     f'invoice PDF to find unextracted charges. Use edit_file to correct values. '
                     f'Variance = InvoiceTotal - (SumItems + Freight + Insurance + Tax - Deduction). '
                     f'After fixing, run validate_xlsx to confirm.\n{detail_str}')

        # Package mismatch from validation
        pkg_check = validation.get('package_check', {})
        if pkg_check.get('mismatch') and pkg_check.get('source') != 'default (1 per entry)':
            per_file_str = '\n'.join(
                f'  {pf["file"]}: {pf["packages"]} pkg'
                for pf in pkg_check.get('per_file', [])[:15]
            )
            fail('package_mismatch', 'block',
                 f'Package count mismatch: XLSX total={pkg_check.get("actual")}, '
                 f'expected={pkg_check.get("expected")} (from {pkg_check.get("source")}).',
                 'packages', str(pkg_check.get('actual')),
                 f'Read the BL PDF to find the correct package count per invoice. Each XLSX has a '
                 f'Packages value in column X row 2. The sum across all XLSX files must equal the '
                 f'BL total. Edit the XLSX files to set the correct package count per invoice.\n'
                 f'{per_file_str}')

        # Duplicate content
        dup_check = validation.get('duplicate_check', {})
        if dup_check.get('has_issues'):
            content_dups = dup_check.get('content_duplicates', [])
            dup_count = len(content_dups)
            if dup_count > 0:
                dup_details = []
                for cd in content_dups:
                    files = ', '.join(cd.get('files', []))
                    sources = ', '.join(cd.get('sources', []))
                    dup_details.append(f'  {cd["type"]}: {files} (from PDFs: {sources})')
                detail_str = '\n'.join(dup_details[:10])
                fail('duplicate_content', 'block',
                     f'{dup_count} duplicate content issue(s) detected across XLSX files.',
                     'validation.duplicates', str(dup_count),
                     f'Compare the listed duplicate XLSX files. If they have the same invoice number '
                     f'and items, delete the one with fewer items or the one from the wrong format. '
                     f'Use list_files to see the output directory, read_file to compare contents, '
                     f'and delete the duplicate file. Then update _email_params.json to remove it '
                     f'from attachment_paths.\n{detail_str}')

    # ── 9. LLM sanity review of email params ──
    try:
        _llm_review_email_params(email_params, validation, fail)
    except Exception as e:
        logger.warning(f"LLM email review failed (non-blocking): {e}")

    # ── Summary ──
    has_blockers = any(f['severity'] == 'block' for f in failures)
    passed = not has_blockers

    # Print checklist report
    if failures:
        print()
        print("SHIPMENT CHECKLIST")
        print("-" * 80)
        for f in failures:
            icon = "BLOCK" if f['severity'] == 'block' else " WARN"
            print(f"  [{icon}] {f['check']}: {f['message']}")
        print("-" * 80)
        if passed:
            print(f"  PASSED with {len(failures)} warning(s). Email can be sent.")
        else:
            blockers = sum(1 for f in failures if f['severity'] == 'block')
            print(f"  FAILED: {blockers} blocker(s). Email will NOT be sent until fixed.")
        print()

    return {
        'passed': passed,
        'failures': failures,
        'blocker_count': sum(1 for f in failures if f['severity'] == 'block'),
        'warning_count': sum(1 for f in failures if f['severity'] == 'warn'),
    }


# ── Helpers ────────────────────────────────────────────────────────────────

def _load_cet_codes(base_dir: str) -> set:
    """Load CET valid codes. Try classifier module first, then direct file read."""
    try:
        from classifier import _load_cet_valid_codes
        return _load_cet_valid_codes(base_dir)
    except ImportError:
        pass

    # Fallback: direct file read
    codes = set()
    cet_path = os.path.join(base_dir, 'data', 'cet_valid_codes.txt')
    try:
        if os.path.exists(cet_path):
            with open(cet_path, 'r', encoding='utf-8') as f:
                for line in f:
                    code = line.strip().split('\t')[0]
                    if len(code) == 8 and code.isdigit():
                        codes.add(code)
    except Exception as e:
        logger.warning(f"Failed to load CET codes: {e}")
    return codes
