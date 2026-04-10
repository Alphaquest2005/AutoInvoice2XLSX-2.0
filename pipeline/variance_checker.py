#!/usr/bin/env python3
"""
Stage 7: Variance Checker
Validates formulas and checks variance equals $0.00.
"""

import json
import os
from typing import Any, Dict

try:
    import openpyxl
except ImportError:
    openpyxl = None


def run(input_path: str, output_path: str = None, config: Dict = None, context: Dict = None) -> Dict:
    """Validate Excel formulas and variance checks."""
    if not openpyxl:
        return {'status': 'error', 'error': 'openpyxl not installed'}

    if not input_path or not os.path.exists(input_path):
        return {'status': 'error', 'error': f'File not found: {input_path}'}

    wb = openpyxl.load_workbook(input_path, data_only=True)
    ws = wb.active

    result = {
        'status': 'success',
        'valid': True,
        'errors': [],
        'warnings': [],
        'checks': {},
    }

    # Check VARIANCE CHECK
    variance = find_labeled_value(ws, 'VARIANCE CHECK', 16)
    result['checks']['variance_check'] = variance
    if variance is not None and abs(float(variance)) > 0.001:
        result['valid'] = False
        result['errors'].append(f"VARIANCE CHECK = ${variance}, expected $0.00")

    # Check GROUP VERIFICATION
    group_var = find_labeled_value(ws, 'GROUP VERIFICATION', 16)
    result['checks']['group_verification'] = group_var
    if group_var is not None and abs(float(group_var)) > 0.001:
        result['valid'] = False
        result['errors'].append(f"GROUP VERIFICATION = ${group_var}, expected $0.00")

    # Scan for formula errors
    error_types = ['#REF!', '#VALUE!', '#DIV/0!', '#NAME?', '#N/A']
    formula_errors = []

    for row in range(1, ws.max_row + 1):
        for col in range(1, min(ws.max_column + 1, 40)):
            cell = ws.cell(row=row, column=col)
            if cell.value and str(cell.value) in error_types:
                col_letter = openpyxl.utils.get_column_letter(col)
                formula_errors.append(f"{cell.value} at {col_letter}{row}")

    result['checks']['formula_errors'] = len(formula_errors)
    if formula_errors:
        result['valid'] = False
        result['errors'].extend(formula_errors[:10])

    # Check Variance column (R) for non-zero values
    non_zero_variance = []
    for row in range(2, ws.max_row + 1):
        cell = ws.cell(row=row, column=18)  # Column R
        if cell.value is not None:
            try:
                val = float(cell.value)
                if abs(val) > 0.001:
                    non_zero_variance.append(f"Row {row}: ${val}")
            except (ValueError, TypeError):
                pass

    if non_zero_variance:
        result['warnings'].extend([f"Non-zero variance: {v}" for v in non_zero_variance[:5]])

    if not result['valid']:
        result['status'] = 'validation_error'

    return result


def find_labeled_value(ws, label: str, value_col: int):
    """Find a labeled row and return value from specified column."""
    for row in range(1, ws.max_row + 1):
        for col in range(1, 15):
            cell = ws.cell(row=row, column=col)
            if cell.value and label.upper() in str(cell.value).upper():
                val = ws.cell(row=row, column=value_col).value
                if val is not None:
                    try:
                        return float(val)
                    except (ValueError, TypeError):
                        return None
    return None
