"""Per-shipment consignee grouping primitive for the auto-split mechanism.

When a shipment folder contains invoices for genuinely different
consignees (the historical incident: one Bernisha invoice landing in a
Budget Marine BL folder), the pipeline must NOT silently merge them
into a single ``_email_params.json`` — that produced wrong customs
paperwork that had to be re-filed by hand.

This module provides ``group_results_by_consignee()``, the grouping
primitive that downstream call sites (``run.py:_save_email_params`` /
``_save_batch_email_params``) invoke after every invoice's per-invoice
consignee resolution is in. Each group becomes one email/customs
declaration; the multi-group case is surfaced as a block-severity
``shipment_split_detected`` checklist finding so a human reviews
before send.

Inputs are duck-typed dicts/objects rather than a strict schema so
callers can pass either ``InvoiceResult`` objects from
``stages.invoice_processor`` or raw dicts from ``workflow.batch``.
"""
from __future__ import annotations

from typing import Any, Iterable

from config_loader import load_document_types, load_pipeline


def _result_consignee_resolution(result: Any) -> dict | None:
    """Return the consignee_resolution dict carried on a result, or None.

    Accepts either an attribute (``result.consignee_resolution``) or a
    dict key (``result["consignee_resolution"]``) so the function works
    against ``InvoiceResult`` instances and plain dicts alike.
    """
    if result is None:
        return None
    if isinstance(result, dict):
        return result.get("consignee_resolution")
    return getattr(result, "consignee_resolution", None)


def _grouping_key(resolution: dict | None) -> tuple[str, str]:
    """Stable group key: (consignee_name, doc_type).

    Empty consignee + default doc_type collapses into one
    ``"unresolved"`` group so callers can surface a single
    consignee_unrecognised finding rather than N copies.
    """
    if not resolution:
        return ("", load_document_types()["default"])
    return (
        resolution.get("consignee_name") or "",
        resolution.get("doc_type") or load_document_types()["default"],
    )


def group_results_by_consignee(results: Iterable[Any]) -> list[dict]:
    """Group invoice results by (consignee_name, doc_type).

    Each input result must carry a ``consignee_resolution`` dict
    produced by ``consignee_resolver.resolve_invoice_consignee``.
    Results with no resolution fall into a sentinel group keyed on
    the empty consignee + the configured default doc_type.

    Returns a list of group dicts ordered by first-seen invoice (so
    snapshots / golden diffs stay stable):

        [
            {
                "consignee_name":  "Budget Marine Grenada",
                "doc_type":        "7400-000",
                "matched_rule":    {...},      # may be None
                "source":          "rule_scan_invoice",
                "results":         [<result>, ...],
            },
            ...
        ]

    The grouping is intentionally simple: tuples of names + doc_types
    are compared as-is, no canonicalisation. Resolver normalisation
    (rule_substring scan + name lookup) is the place to enforce
    "Budget Marine" / "BUDGET MARINE GRENADA" etc. all map to the same
    rule consignee_name.
    """
    sources_cfg = load_pipeline()["consignee_resolution_sources"]
    default_source = sources_cfg["DEFAULT"]

    groups: list[dict] = []
    index_by_key: dict[tuple[str, str], int] = {}

    for r in results:
        resolution = _result_consignee_resolution(r) or {}
        key = _grouping_key(resolution)
        idx = index_by_key.get(key)
        if idx is None:
            groups.append({
                "consignee_name": key[0],
                "doc_type":       key[1],
                "matched_rule":   resolution.get("matched_rule"),
                "source":         resolution.get("source") or default_source,
                "results":        [r],
            })
            index_by_key[key] = len(groups) - 1
        else:
            groups[idx]["results"].append(r)

    return groups


def shipment_was_split(groups: list[dict]) -> bool:
    """True iff there's more than one consignee group — the trigger
    condition for a ``shipment_split_detected`` checklist finding."""
    return len(groups) > 1


def bl_consignee_disagrees(groups: list[dict], bl_consignee: str) -> bool:
    """True iff the BL-level consignee (importer of record) differs
    from EVERY invoice-level consignee group. A common cause is a
    folder that mixes invoices from a different shipper's BL — the
    customer should review before send.

    Returns False when bl_consignee is empty (no BL parsed) or when
    any group's consignee matches the BL consignee (case-insensitive
    substring match — handles "BUDGET MARINE GRENADA" vs
    "Budget Marine Grenada" naming variation between BL OCR and the
    document_types.json rule).
    """
    if not bl_consignee:
        return False
    bl_lower = bl_consignee.strip().lower()
    if not bl_lower:
        return False
    for g in groups:
        name = (g.get("consignee_name") or "").strip().lower()
        if not name:
            continue
        if bl_lower in name or name in bl_lower:
            return False
    return True
