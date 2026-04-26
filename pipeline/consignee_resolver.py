"""Per-invoice consignee + doc_type resolver — SSOT.

This module is the single source of truth for answering "what consignee
is this invoice for, and what document type should the XLSX use?".

It is called by both the legacy orchestrator (``pipeline/run.py``) and
the per-invoice rebuild path (``pipeline/pipeline_runner.py`` via
``pipeline/workflow/batch.py``) so a single invoice resolves the same
way regardless of which path produces its XLSX. The bug it exists to
prevent: regenerating an invoice through the per-invoice path silently
defaults to ``4000-000`` because the orchestrator's once-per-run
``resolve_doc_type`` is bypassed (see TSCW18489131 / 26006159).

Resolution layers (each falls through on miss):
    1. Rule-substring scan against the invoice's own OCR text — answers
       "does this invoice mention a known consignee anywhere?". Catches
       layouts where the consignee is named without a Bill-To/Ship-To
       label (e.g. Victron's ``INVOICE ADDRESS / DELIVERY ADDRESS``).
    2. Label-based extractor result from the upstream parser — when a
       Bill-To/Ship-To pattern already produced a name. Mapped to a
       rule when possible; preserved as free text otherwise.
    3. BL consignee — fallback (BL is shipment-level, not per invoice).
    4. Manifest consignee — last resort before default.
    5. Default — empty consignee + fallback doc_type. Caller surfaces
       this via a ``consignee_unrecognised`` checklist finding.

Returns a dict so callers can branch on ``source`` and ``matched_rule``
without re-implementing the precedence stack.
"""
from __future__ import annotations

import re

from pipeline.config_loader import (
    load_document_types,
    load_patterns,
    load_pipeline,
)


def _match_consignee_name_to_rule(consignee_name: str) -> dict | None:
    """Return the rule whose ``match`` or any ``alias`` is a substring of
    ``consignee_name`` (case-insensitive). None if no rule matches.

    Used when an upstream extractor already produced a candidate name
    and we need to map it to the configured rule.
    """
    needle = (consignee_name or "").strip().lower()
    if not needle:
        return None
    for rule in load_document_types().get("consignee_rules", []) or []:
        match = (rule.get("match") or "").lower()
        if match and match in needle:
            return rule
        for alias in rule.get("aliases") or []:
            a = (alias or "").lower()
            if a and a in needle:
                return rule
    return None


def _scan_text_for_consignee_rule(text: str) -> dict | None:
    """Scan free OCR text for any consignee rule's ``match`` or ``alias``.

    Multi-word patterns use case-insensitive substring (whitespace already
    bounds them). Single-word patterns use strict word-token membership
    via the ``consignee_alias_word_token`` regex from patterns.yaml, with
    a length floor from ``consignee_match.min_single_word_alias_chars`` in
    document_types.json — keeps short aliases from false-matching inside
    longer words (e.g. ``bm`` in ``submarine``).
    Returns the first matching rule (rule list order = priority).
    """
    if not text:
        return None
    word_re = load_patterns()["consignee_alias_word_token"]
    dt = load_document_types()
    min_len = dt.get("consignee_match", {}).get("min_single_word_alias_chars", 0)
    text_lower = text.lower()
    text_words = set(re.findall(word_re, text_lower))
    for rule in dt.get("consignee_rules", []) or []:
        candidates = [rule.get("match") or ""]
        candidates.extend(rule.get("aliases") or [])
        for cand in candidates:
            cand = (cand or "").strip().lower()
            if not cand:
                continue
            if " " in cand:
                if cand in text_lower:
                    return rule
            elif len(cand) >= min_len:
                if cand in text_words:
                    return rule
    return None


def _make_result(consignee_name: str, doc_type: str, source: str,
                 matched_rule: dict | None) -> dict:
    return {
        "consignee_name": consignee_name,
        "doc_type": doc_type,
        "source": source,
        "matched_rule": matched_rule,
    }


def resolve_invoice_consignee(
    invoice_text: str = "",
    bl_consignee: str = "",
    manifest_consignee: str = "",
    label_extracted: str = "",
) -> dict:
    """Per-invoice layered consignee + doc_type resolution.

    Inputs (any combination may be empty):
        invoice_text        : OCR text of the invoice itself.
        bl_consignee        : consignee name from BL parsing (shipment-level).
        manifest_consignee  : consignee name from manifest metadata.
        label_extracted     : name produced by the upstream Ship-To /
                              Bill-To / Sold-To label parsers.

    Returns dict with keys:
        consignee_name : str — resolved name (may be "")
        doc_type       : str — resolved doc_type code
        source         : str — provenance tag (see pipeline.yaml
                         ``consignee_resolution_sources``)
        matched_rule   : dict | None — the consignee rule when one fired

    Splitting downstream uses ``(consignee_name, doc_type)`` as the
    grouping key, so legitimate multi-consignee folders surface as
    separate groups instead of getting silently merged.
    """
    sources = load_pipeline()["consignee_resolution_sources"]
    default_dt = load_document_types()["default"]

    rule = _scan_text_for_consignee_rule(invoice_text)
    if rule:
        name = rule.get("consignee_name") or (rule.get("match") or "").title()
        return _make_result(name, rule["doc_type"],
                            sources["RULE_SCAN_INVOICE"], rule)

    if label_extracted:
        rule = _match_consignee_name_to_rule(label_extracted)
        if rule:
            name = rule.get("consignee_name") or label_extracted
            return _make_result(name, rule["doc_type"],
                                sources["RULE_MATCH_LABEL"], rule)
        return _make_result(label_extracted, default_dt,
                            sources["LABEL_INVOICE"], None)

    if bl_consignee:
        rule = _match_consignee_name_to_rule(bl_consignee)
        if rule:
            name = rule.get("consignee_name") or bl_consignee
            return _make_result(name, rule["doc_type"],
                                sources["BL_RULE_MATCH"], rule)
        return _make_result(bl_consignee, default_dt,
                            sources["BL"], None)

    if manifest_consignee:
        rule = _match_consignee_name_to_rule(manifest_consignee)
        if rule:
            name = rule.get("consignee_name") or manifest_consignee
            return _make_result(name, rule["doc_type"],
                                sources["MANIFEST_RULE_MATCH"], rule)
        return _make_result(manifest_consignee, default_dt,
                            sources["MANIFEST"], None)

    return _make_result("", default_dt, sources["DEFAULT"], None)
