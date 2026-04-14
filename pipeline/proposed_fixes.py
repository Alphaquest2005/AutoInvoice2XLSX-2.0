#!/usr/bin/env python3
"""
Proposed Fixes: reviewer-facing artefacts for uncertain invoices.

When the honest reconciliation pipeline cannot deterministically resolve
an invoice (orphan-price recovery left a residual, OCR dropped a field,
variance had to be absorbed into the ADJUSTMENTS row, etc.), this module
packages the uncertainty into two human-tractable artefacts:

1. A machine-editable YAML patch (``proposed_fixes_<waybill>.yaml``)
   that lists every invoice's current flagged numbers alongside an
   "override" section the reviewer can fill in.  This is what gets
   attached to the email and what the Phase 3 replay module reads back.

2. A plain-text email body that summarises the shipment, lists each
   uncertain invoice with its notes, and explains how to respond.

The module is pure data-shaping: it does not send emails, does not touch
settings.json, and does not write anywhere outside the output_dir passed
in.  ``send_shipment_email.py`` owns the actual SMTP hand-off.
"""

from __future__ import annotations

import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

try:
    import yaml
except ImportError:  # pragma: no cover — yaml is a first-class dep elsewhere
    yaml = None

logger = logging.getLogger(__name__)


@dataclass
class UncertainInvoice:
    """One uncertain invoice within a shipment."""
    invoice_num: str
    supplier: str
    pdf_path: str
    xlsx_path: str
    invoice_total: float
    sub_total: float
    items_sum: float
    residual: float  # invoice_total - (items_sum + adjustments)
    notes: List[str] = field(default_factory=list)
    recovered_items: List[Dict[str, Any]] = field(default_factory=list)
    # Non-recovered items (for context in the YAML patch)
    other_items: List[Dict[str, Any]] = field(default_factory=list)


def detect_uncertain_invoices(results: List[Any]) -> List[UncertainInvoice]:
    """Walk pipeline results and return the invoices that carry uncertainty.

    ``results`` is the list of ``InvoiceResult`` objects returned by
    ``invoice_processor.process_single_invoice``.  We inspect
    ``invoice_data`` for ``data_quality_notes`` / ``invoice_total_uncertain``
    and per-item ``data_quality`` markers.
    """
    uncertain: List[UncertainInvoice] = []
    for r in results:
        inv_data = getattr(r, 'invoice_data', {}) or {}
        notes = list(inv_data.get('data_quality_notes') or [])
        uncertain_flag = bool(inv_data.get('invoice_total_uncertain'))

        items = list(inv_data.get('items') or [])
        # Also consult matched_items for per-item data_quality (propagated by
        # _items_without_po).  Flag any invoice that carries at least one
        # recovered or residually-adjusted value.
        matched = list(getattr(r, 'matched_items', []) or [])
        recovered = [
            m for m in matched
            if (m.get('data_quality') or '').strip() not in ('', 'ok')
        ]

        if not notes and not uncertain_flag and not recovered:
            continue

        items_sum = sum(
            float(m.get('total_cost', 0) or 0) for m in matched
        )
        invoice_total = float(inv_data.get('invoice_total', 0) or 0)
        sub_total = float(inv_data.get('sub_total', 0) or 0)

        # Adjustments same formula as invoice_processor._save_email_params
        freight = float(inv_data.get('freight', 0) or 0)
        tax = float(inv_data.get('tax', 0) or 0)
        other_cost = float(inv_data.get('other_cost', 0) or 0)
        credits = float(inv_data.get('credits', 0) or 0)
        discount = float(inv_data.get('discount', 0) or 0)
        free_shipping = float(inv_data.get('free_shipping', 0) or 0)
        adjustments = freight - credits + tax + other_cost - discount - free_shipping

        residual = round(invoice_total - (items_sum + adjustments), 2)

        uncertain.append(UncertainInvoice(
            invoice_num=getattr(r, 'invoice_num', '') or inv_data.get('invoice_num', ''),
            supplier=(
                getattr(r, 'supplier_info', {}).get('name', '')
                or inv_data.get('supplier_name', '')
                or inv_data.get('supplier', '')
                or ''
            ),
            pdf_path=getattr(r, 'pdf_output_path', '') or '',
            xlsx_path=getattr(r, 'xlsx_path', '') or '',
            invoice_total=round(invoice_total, 2),
            sub_total=round(sub_total, 2),
            items_sum=round(items_sum, 2),
            residual=residual,
            notes=notes,
            recovered_items=[
                {
                    'sku': m.get('supplier_item', ''),
                    'description': (m.get('supplier_item_desc') or '')[:120],
                    'quantity': m.get('quantity', 0),
                    'unit_cost': m.get('unit_price', 0),
                    'total_cost': m.get('total_cost', 0),
                    'data_quality': m.get('data_quality', ''),
                }
                for m in recovered
            ],
            other_items=[
                {
                    'sku': m.get('supplier_item', ''),
                    'description': (m.get('supplier_item_desc') or '')[:120],
                    'quantity': m.get('quantity', 0),
                    'unit_cost': m.get('unit_price', 0),
                    'total_cost': m.get('total_cost', 0),
                }
                for m in matched
                if (m.get('data_quality') or '').strip() in ('', 'ok')
            ],
        ))
    return uncertain


def build_fixes_yaml(
    waybill: str,
    uncertain: List[UncertainInvoice],
) -> str:
    """Build a YAML patch string that the reviewer can edit and send back.

    Shape::

        waybill: HAWBS665535
        replay_mode: true
        instructions: >
          Edit the `override` section for any item you want to correct.
          Leave fields null to keep the current value.  When done, reply
          to this email with the file attached and the pipeline will
          re-process the shipment.
        invoices:
          - invoice_num: INVUS...
            supplier: SHEIN US Services, LLC
            pdf: /path/to/file.pdf
            current:
              sub_total: 33.10
              items_sum: 32.90
              invoice_total: 35.43
              residual: 0.20
            notes: [...]
            recovered_items:
              - sku: SHEIN-4
                description: "..."
                current:
                  quantity: 1
                  unit_cost: 13.70
                  total_cost: 13.70
                override:
                  quantity: null
                  unit_cost: null
                  total_cost: null
                  description: null
                  delete: false
    """
    if yaml is None:
        raise RuntimeError("PyYAML not installed; cannot build proposed fixes")

    invoices_payload: List[Dict[str, Any]] = []
    for u in uncertain:
        rec_payload = []
        for it in u.recovered_items:
            rec_payload.append({
                'sku': it['sku'],
                'description': it['description'],
                'current': {
                    'quantity': it['quantity'],
                    'unit_cost': round(float(it['unit_cost'] or 0), 2),
                    'total_cost': round(float(it['total_cost'] or 0), 2),
                },
                'data_quality': it['data_quality'],
                'override': {
                    'quantity': None,
                    'unit_cost': None,
                    'total_cost': None,
                    'description': None,
                    'delete': False,
                },
            })
        other_payload = []
        for it in u.other_items:
            other_payload.append({
                'sku': it['sku'],
                'description': it['description'],
                'current': {
                    'quantity': it['quantity'],
                    'unit_cost': round(float(it['unit_cost'] or 0), 2),
                    'total_cost': round(float(it['total_cost'] or 0), 2),
                },
                'override': {
                    'quantity': None,
                    'unit_cost': None,
                    'total_cost': None,
                    'description': None,
                    'delete': False,
                },
            })
        invoices_payload.append({
            'invoice_num': u.invoice_num,
            'supplier': u.supplier,
            'pdf': os.path.basename(u.pdf_path) if u.pdf_path else '',
            'current': {
                'sub_total': u.sub_total,
                'items_sum': u.items_sum,
                'invoice_total': u.invoice_total,
                'residual': u.residual,
            },
            'notes': u.notes,
            'recovered_items': rec_payload,
            'other_items': other_payload,
            'add_items': [],  # reviewer can append new items here
        })

    payload = {
        'waybill': waybill,
        'replay_mode': True,
        'instructions': (
            'Edit the override block for any item. Leave fields null to '
            'keep current. Set delete: true to remove an item. Append new '
            'items under add_items. Reply with this file attached to '
            'trigger pipeline replay.'
        ),
        'invoices': invoices_payload,
    }
    return yaml.safe_dump(payload, sort_keys=False, width=100)


def build_fixes_body(
    waybill: str,
    uncertain: List[UncertainInvoice],
    yaml_filename: str,
) -> str:
    """Compose the human-readable email body."""
    lines = [
        f"Proposed Fixes for shipment: {waybill}",
        "",
        f"This shipment has {len(uncertain)} invoice(s) that could not be ",
        "resolved deterministically. Each one was reconciled honestly:",
        "  - orphan price tokens in the OCR text were matched against ",
        "    the subtotal anchor where possible,",
        "  - any remaining variance was absorbed into the ADJUSTMENTS row ",
        "    (visible in the combined manifest), and",
        "  - every recovered value is highlighted in the XLSX with a ",
        "    yellow fill and a cell comment.",
        "",
        "Please review each invoice below and — if anything is wrong — ",
        f"edit the attached file ({yaml_filename}) and reply to this ",
        "message with the edited file attached. The pipeline will detect ",
        "the patch and re-process the shipment with your corrections.",
        "",
        "-" * 70,
    ]
    for idx, u in enumerate(uncertain, 1):
        lines.extend([
            "",
            f"[{idx}] Invoice: {u.invoice_num}   Supplier: {u.supplier}",
            f"    Sub-total:    ${u.sub_total:.2f}",
            f"    Items sum:    ${u.items_sum:.2f}",
            f"    Invoice total:${u.invoice_total:.2f}",
            f"    Residual:     ${u.residual:.2f}",
            "    Notes:",
        ])
        if u.notes:
            for n in u.notes:
                lines.append(f"      - {n}")
        else:
            lines.append("      (none)")
        if u.recovered_items:
            lines.append("    Recovered items (review these first):")
            for it in u.recovered_items:
                lines.append(
                    f"      * {it['sku']}  qty={it['quantity']}  "
                    f"unit=${float(it['unit_cost'] or 0):.2f}  "
                    f"total=${float(it['total_cost'] or 0):.2f}  "
                    f"[{it['data_quality']}]"
                )
                desc = (it.get('description') or '')[:80]
                if desc:
                    lines.append(f"        {desc}")
    lines.extend([
        "",
        "-" * 70,
        "",
        "How to respond:",
        f"  1. Download {yaml_filename}.",
        "  2. For each item you want to correct, fill in the override ",
        "     block. Leave fields as null to keep current values.",
        "  3. Reply to this email with the edited file attached. The ",
        "     subject must start with 'APPLY FIXES:' so the pipeline ",
        "     picks it up automatically.",
        "",
        "If everything in the manifest looks right as-is, no action is ",
        "needed — the shipment email has already been sent for processing.",
    ])
    return "\n".join(lines)


def save_fixes_artifacts(
    waybill: str,
    uncertain: List[UncertainInvoice],
    output_dir: str,
) -> Optional[Dict[str, str]]:
    """Write the YAML patch and a ``_proposed_fixes_params.json`` sidecar.

    Returns a dict with ``{yaml_path, params_path}`` on success, or None
    when there are no uncertain invoices to report.
    """
    if not uncertain:
        return None
    os.makedirs(output_dir, exist_ok=True)

    safe_wb = re.sub(r'[^A-Za-z0-9_\-]', '_', waybill or 'shipment')
    yaml_name = f"proposed_fixes_{safe_wb}.yaml"
    yaml_path = os.path.join(output_dir, yaml_name)
    yaml_text = build_fixes_yaml(waybill, uncertain)
    with open(yaml_path, 'w', encoding='utf-8') as f:
        f.write(yaml_text)

    body = build_fixes_body(waybill, uncertain, yaml_name)
    subject = f"Proposed Fixes for shipment: {waybill}"

    # Attach: the YAML, plus each uncertain invoice's source PDF and
    # the XLSX that shows the reviewer where the yellow fills are.
    attachments: List[str] = [yaml_path]
    for u in uncertain:
        for p in (u.pdf_path, u.xlsx_path):
            if p and os.path.exists(p) and p not in attachments:
                attachments.append(p)

    params = {
        'kind': 'proposed_fixes',
        'waybill': waybill,
        'subject': subject,
        'body': body,
        'yaml_path': yaml_path,
        'uncertain_count': len(uncertain),
        'attachment_paths': attachments,
    }
    params_path = os.path.join(output_dir, '_proposed_fixes_params.json')
    with open(params_path, 'w', encoding='utf-8') as f:
        json.dump(params, f, indent=2)

    logger.info(
        f"Proposed Fixes written: {yaml_path} "
        f"({len(uncertain)} uncertain invoice(s))"
    )
    return {'yaml_path': yaml_path, 'params_path': params_path}
