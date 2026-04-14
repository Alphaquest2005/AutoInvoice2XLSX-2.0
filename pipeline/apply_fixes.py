#!/usr/bin/env python3
"""
Apply Fixes: replay pipeline with reviewer-edited Proposed Fixes YAML.

When the pipeline emits a ``proposed_fixes_<waybill>.yaml`` (see
``proposed_fixes.py``), the reviewer fills in the ``override`` blocks for
any item that needs correction, optionally adds brand-new items under
``add_items``, and attaches the edited file back to the shipment.  The
TypeScript mail watcher drops the YAML into the shipment's input
directory and re-runs the pipeline.

This module is the consumption side of that loop:

* ``load_fixes_yaml``     — parse + shallow-validate an edited patch
* ``discover_fixes``      — walk an input_dir and return every patch it finds
* ``build_fixes_map``     — flatten ``{invoice_num: invoice_fixes}``
* ``apply_fixes_to_result`` — mutate a pipeline result in-place
* ``archive_fixes_yaml``  — move the applied patch to ``data/learned_fixes``

``apply_fixes_to_result`` is pure w.r.t. the filesystem — it does not
regenerate the XLSX.  Callers (``run.py``) are responsible for calling
``bl_xlsx_generator.generate_bl_xlsx`` again after the mutation so the
output file reflects the corrections.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

try:
    import yaml
except ImportError:  # pragma: no cover — yaml is a first-class dep elsewhere
    yaml = None

logger = logging.getLogger(__name__)


# ─── Loading & discovery ───────────────────────────────────────────────


def load_fixes_yaml(path: str) -> Dict[str, Any]:
    """Load an edited Proposed Fixes YAML file.

    Shallow validation only: we check that the doc is a mapping with a
    ``waybill`` and an ``invoices`` list.  We do NOT reject unknown keys —
    reviewers are allowed to add comments or notes, and the shape of the
    override blocks is tolerated as long as it is a dict.
    """
    if yaml is None:
        raise RuntimeError("PyYAML not installed; cannot load proposed fixes")
    with open(path, 'r', encoding='utf-8') as f:
        doc = yaml.safe_load(f) or {}
    if not isinstance(doc, dict):
        raise ValueError(f"{path}: top-level YAML must be a mapping")
    if 'waybill' not in doc:
        raise ValueError(f"{path}: missing required 'waybill'")
    if 'invoices' not in doc or not isinstance(doc['invoices'], list):
        raise ValueError(f"{path}: 'invoices' must be a list")
    return doc


def discover_fixes(input_dir: str) -> List[str]:
    """Return every ``proposed_fixes_*.yaml`` file in ``input_dir``.

    We look for the exact filename pattern the pipeline emits so that an
    unrelated YAML dropped into the folder does not get mis-applied.
    """
    if not input_dir or not os.path.isdir(input_dir):
        return []
    found: List[str] = []
    for name in sorted(os.listdir(input_dir)):
        if not name.lower().endswith('.yaml'):
            continue
        if not name.lower().startswith('proposed_fixes_'):
            continue
        path = os.path.join(input_dir, name)
        if os.path.isfile(path):
            found.append(path)
    return found


def build_fixes_map(fixes_docs: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Flatten a list of parsed fixes docs into ``{invoice_num: invoice_fixes}``.

    When the same invoice_num appears in multiple docs (unlikely but
    possible), the later doc wins and a warning is logged.
    """
    out: Dict[str, Dict[str, Any]] = {}
    for doc in fixes_docs:
        for inv in doc.get('invoices', []) or []:
            if not isinstance(inv, dict):
                continue
            num = inv.get('invoice_num')
            if not num:
                continue
            if num in out:
                logger.warning(f"fixes: duplicate invoice_num {num} — later wins")
            out[num] = inv
    return out


# ─── Override application ─────────────────────────────────────────────


def _coerce_number(value: Any) -> Optional[float]:
    """Best-effort float coercion; returns None for null/empty strings."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        v = value.strip()
        if not v:
            return None
        try:
            return float(v.replace(',', ''))
        except ValueError:
            return None
    return None


def _apply_item_override(
    matched_item: Dict[str, Any],
    override: Dict[str, Any],
) -> List[str]:
    """Mutate ``matched_item`` in place from an ``override`` block.

    Returns a list of human-readable change descriptions.
    """
    changes: List[str] = []
    if not override:
        return changes

    new_qty = _coerce_number(override.get('quantity'))
    new_unit = _coerce_number(override.get('unit_cost'))
    new_total = _coerce_number(override.get('total_cost'))
    new_desc = override.get('description')

    if new_qty is not None and new_qty != matched_item.get('quantity'):
        changes.append(f"qty {matched_item.get('quantity')} → {new_qty}")
        matched_item['quantity'] = new_qty
    if new_unit is not None and new_unit != matched_item.get('unit_price'):
        changes.append(f"unit ${matched_item.get('unit_price')} → ${new_unit}")
        matched_item['unit_price'] = new_unit
    if new_total is not None and new_total != matched_item.get('total_cost'):
        changes.append(f"total ${matched_item.get('total_cost')} → ${new_total}")
        matched_item['total_cost'] = new_total
    if isinstance(new_desc, str) and new_desc.strip():
        changes.append("description updated")
        matched_item['supplier_item_desc'] = new_desc.strip()

    # If qty and unit were set but total was left null, recompute total.
    if new_qty is not None and new_unit is not None and new_total is None:
        matched_item['total_cost'] = round(new_qty * new_unit, 2)
        changes.append(f"total recomputed → ${matched_item['total_cost']}")

    # After a successful override the uncertainty marker is no longer
    # accurate — the reviewer has explicitly vetted the values.
    if matched_item.get('data_quality'):
        matched_item['data_quality'] = ''

    return changes


def _match_item_by_sku(
    matched_items: List[Dict[str, Any]],
    sku: str,
) -> Optional[Dict[str, Any]]:
    """Return the first matched_item whose supplier_item equals ``sku``."""
    if not sku:
        return None
    for m in matched_items:
        if (m.get('supplier_item') or '').strip() == sku.strip():
            return m
    return None


def apply_fixes_to_result(
    result: Any,
    invoice_fixes: Dict[str, Any],
) -> Dict[str, Any]:
    """Apply one invoice's fixes block to a pipeline result in place.

    Mutates:
      * ``result.matched_items`` — overrides applied, deleted rows removed,
        new rows appended from ``add_items``.
      * ``result.invoice_data`` — data_quality_notes annotated, uncertainty
        flag cleared once residual is zero.

    Returns a dict describing what was changed, suitable for logging and
    for the learned-fixes candidate journal in Phase 4.
    """
    report: Dict[str, Any] = {
        'invoice_num': invoice_fixes.get('invoice_num', ''),
        'items_updated': 0,
        'items_added': 0,
        'items_deleted': 0,
        'changes': [],
    }
    matched = list(getattr(result, 'matched_items', []) or [])
    inv_data = getattr(result, 'invoice_data', {}) or {}

    # 1) Per-item overrides / deletes across recovered_items + other_items
    to_delete: List[int] = []
    for section in ('recovered_items', 'other_items'):
        for entry in invoice_fixes.get(section, []) or []:
            if not isinstance(entry, dict):
                continue
            sku = entry.get('sku', '')
            override = entry.get('override') or {}
            target = _match_item_by_sku(matched, sku)
            if target is None:
                logger.info(f"fixes: sku {sku!r} not found in matched items — skipped")
                continue
            if override.get('delete'):
                idx = matched.index(target)
                to_delete.append(idx)
                report['items_deleted'] += 1
                report['changes'].append(f"delete {sku}")
                continue
            changes = _apply_item_override(target, override)
            if changes:
                report['items_updated'] += 1
                report['changes'].append(f"{sku}: " + "; ".join(changes))

    # Perform deletions from the highest index down so earlier indices stay valid
    for idx in sorted(set(to_delete), reverse=True):
        del matched[idx]

    # 2) New items from add_items
    for entry in invoice_fixes.get('add_items', []) or []:
        if not isinstance(entry, dict):
            continue
        sku = (entry.get('sku') or '').strip()
        desc = (entry.get('description') or '').strip()
        qty = _coerce_number(entry.get('quantity')) or 0
        unit = _coerce_number(entry.get('unit_cost')) or 0
        total = _coerce_number(entry.get('total_cost'))
        if total is None:
            total = round(qty * unit, 2)
        if not sku and not desc:
            continue
        matched.append({
            'supplier_item': sku,
            'supplier_item_desc': desc,
            'quantity': qty,
            'unit_price': unit,
            'total_cost': total,
            'data_quality': '',
            'classification_source': 'fixes_yaml',
            'tariff_code': (entry.get('tariff_code') or '00000000'),
        })
        report['items_added'] += 1
        report['changes'].append(
            f"add {sku or desc[:20]} qty={qty} unit=${unit} total=${total}"
        )

    # Commit the mutated list back onto the result
    try:
        result.matched_items = matched
    except AttributeError:
        pass  # result may be a namespace; assignment is fine in that case too

    # 3) Clear uncertainty once the residual balances
    items_sum = sum(
        float(m.get('total_cost', 0) or 0) for m in matched
    )
    freight = float(inv_data.get('freight', 0) or 0)
    tax = float(inv_data.get('tax', 0) or 0)
    other_cost = float(inv_data.get('other_cost', 0) or 0)
    credits = float(inv_data.get('credits', 0) or 0)
    discount = float(inv_data.get('discount', 0) or 0)
    free_shipping = float(inv_data.get('free_shipping', 0) or 0)
    adjustments = freight - credits + tax + other_cost - discount - free_shipping
    invoice_total = float(inv_data.get('invoice_total', 0) or 0)
    residual = round(invoice_total - (items_sum + adjustments), 2)
    report['residual_after'] = residual

    if abs(residual) <= 0.02:
        inv_data['invoice_total_uncertain'] = False
        inv_data['data_quality_notes'] = [
            f"Reviewer applied fixes: {report['items_updated']} updated, "
            f"{report['items_added']} added, {report['items_deleted']} deleted"
        ]
    else:
        inv_data.setdefault('data_quality_notes', []).append(
            f"Reviewer applied fixes; residual ${residual:.2f} remains"
        )

    return report


def apply_fixes_to_results(
    results: List[Any],
    fixes_map: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Apply a fixes_map across a list of pipeline results.

    Returns a list of per-invoice change reports (only for results that
    had a matching fixes entry).  Mutates results in place.
    """
    reports: List[Dict[str, Any]] = []
    for r in results:
        num = getattr(r, 'invoice_num', '') or ''
        if not num:
            continue
        fixes = fixes_map.get(num)
        if not fixes:
            continue
        report = apply_fixes_to_result(r, fixes)
        reports.append(report)
        logger.info(
            f"fixes applied to {num}: "
            f"{report['items_updated']} updated, "
            f"{report['items_added']} added, "
            f"{report['items_deleted']} deleted"
        )
    return reports


# ─── Candidate journaling (Phase 4) ───────────────────────────────────


def _snapshot_item(m: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'sku': (m.get('supplier_item') or '').strip(),
        'description': (m.get('supplier_item_desc') or '')[:120],
        'quantity': m.get('quantity', 0),
        'unit_cost': m.get('unit_price', 0),
        'total_cost': m.get('total_cost', 0),
    }


def log_fix_candidates(
    reports: List[Dict[str, Any]],
    results: List[Any],
    waybill: str,
    base_dir: str,
) -> Optional[str]:
    """Append one JSONL record per applied fix into the candidate journal.

    Each record captures the minimum a reviewer needs to decide whether
    the fix should be promoted into a format spec: format name, supplier,
    invoice number, residual before/after, and the human-readable change
    list produced by ``apply_fixes_to_result``.

    The journal lives under ``data/learned_fixes/candidates/YYYY-MM/YYYY-MM-DD.jsonl``
    so it is trivially appendable and rotates monthly without coordination.
    Returns the path written to, or None if there is nothing to log.
    """
    if not reports:
        return None

    # Index results by invoice_num for quick format/supplier lookup
    by_num: Dict[str, Any] = {}
    for r in results:
        num = getattr(r, 'invoice_num', '') or ''
        if num:
            by_num[num] = r

    now = datetime.now()
    month = now.strftime('%Y-%m')
    day = now.strftime('%Y-%m-%d')
    journal_dir = os.path.join(base_dir, 'data', 'learned_fixes', 'candidates', month)
    os.makedirs(journal_dir, exist_ok=True)
    journal_path = os.path.join(journal_dir, f"{day}.jsonl")

    with open(journal_path, 'a', encoding='utf-8') as f:
        for rep in reports:
            num = rep.get('invoice_num', '')
            r = by_num.get(num)
            record = {
                'timestamp': now.isoformat(timespec='seconds'),
                'waybill': waybill,
                'invoice_num': num,
                'format_name': getattr(r, 'format_name', '') if r else '',
                'supplier': (
                    (getattr(r, 'supplier_info', {}) or {}).get('name', '')
                    if r else ''
                ),
                'items_updated': rep.get('items_updated', 0),
                'items_added': rep.get('items_added', 0),
                'items_deleted': rep.get('items_deleted', 0),
                'residual_after': rep.get('residual_after', 0),
                'changes': list(rep.get('changes', [])),
                'items_snapshot': [
                    _snapshot_item(m) for m in (
                        getattr(r, 'matched_items', []) or []
                    )
                ] if r else [],
            }
            f.write(json.dumps(record) + "\n")

    logger.info(f"fix candidates journalled: {journal_path} ({len(reports)} entries)")
    return journal_path


# ─── Archival ─────────────────────────────────────────────────────────


def archive_fixes_yaml(
    yaml_path: str,
    waybill: str,
    base_dir: str,
) -> Optional[str]:
    """Move an applied fixes YAML into ``data/learned_fixes/YYYY-MM/``.

    Returns the destination path on success, or None if the source is
    missing.  The subdirectory is created on demand.  A timestamp is
    inserted into the filename so repeat replays of the same waybill do
    not overwrite previous archives.
    """
    if not yaml_path or not os.path.exists(yaml_path):
        return None
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    month = datetime.now().strftime('%Y-%m')
    safe_wb = re.sub(r'[^A-Za-z0-9_\-]', '_', waybill or 'shipment')
    archive_dir = os.path.join(base_dir, 'data', 'learned_fixes', month)
    os.makedirs(archive_dir, exist_ok=True)
    dest = os.path.join(archive_dir, f"{safe_wb}_{ts}.yaml")
    shutil.move(yaml_path, dest)
    logger.info(f"fixes archived: {dest}")
    return dest
