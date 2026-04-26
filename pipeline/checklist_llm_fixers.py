"""LLM / text re-extraction fixers for ``shipment_checklist`` failures.

Phase B of the fix-then-flag-then-send architecture (Joseph 2026-04-25):
when the mechanical fixers in ``checklist_fixer`` cannot resolve a finding,
this module re-derives the missing field from the BL/Declaration PDF — first
via the deterministic ``bl_parser.parse_bl_pdf`` text scrape, then via a
strict-JSON LLM call over the cached OCR text. Failures are swallowed and
the finding is left as residual so it surfaces in the PRE-SEND ISSUES
banner instead of crashing the pipeline.

Module surface
--------------
* ``register_into(registry: dict)`` — plugs each LLM fixer into the same
  ``_FIXERS`` table that the mechanical module uses. ``checklist_fixer``
  invokes this at import time so callers only ever interact with the
  unified ``attempt_fixes`` orchestrator.
* ``fix_waybill``, ``fix_weight``, ``fix_packages``, ``fix_consignee_name``,
  ``fix_consignee_address`` — individual fixer entry points, exposed for
  direct unit testing.

Design notes
------------
* The fixers take ``parse_bl_pdf`` and ``_call_llm`` / ``_extract_pdf_text``
  through module-level indirection so tests can monkeypatch them without
  touching real PDFs or the network.
* Re-extracted strings that would re-trigger the same ``*_garbage`` check
  (eg. consignee name > 60 chars, address > 200 chars) are rejected so the
  fixer doesn't cause an infinite "fix → fail → fix" loop.
* No fabrication: when both the BL parser and the LLM fail, the fixer
  returns ``False`` and the finding stays in the residual list.
"""
from __future__ import annotations

import json
import logging
import os
import re
from typing import Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

# Keep these check-key thresholds in sync with xlsx_validator.py:
# * consignee_name_garbage fires above 60 chars
# * consignee_address_garbage fires above 200 chars
_CONSIGNEE_NAME_MAX = 60   # magic-ok: mirrors xlsx_validator threshold
_CONSIGNEE_ADDR_MAX = 200   # magic-ok: mirrors xlsx_validator threshold


# ── Indirection layer (monkeypatched in tests) ─────────────────────


def parse_bl_pdf(pdf_path: str) -> dict:   # pragma: no cover - thin shim
    """Indirection wrapper so tests can stub the BL parser in this module."""
    from bl_parser import parse_bl_pdf as _parse
    try:
        return _parse(pdf_path) or {}
    except Exception as e:
        logger.warning(f"parse_bl_pdf failed for {pdf_path}: {e}")
        return {}


def _extract_pdf_text(pdf_path: str) -> str:
    """Return cached OCR text for ``pdf_path`` or ``''`` on failure."""
    try:
        from multi_ocr import extract_text
        result = extract_text(pdf_path, quality="standard", use_cache=True)
        return getattr(result, "text", "") or ""
    except Exception as e:
        logger.warning(f"OCR extract_text failed for {pdf_path}: {e}")
        return ""


def _call_llm(system_prompt: str, user_message: str,
              *, cache_key_extra: str = "") -> Optional[str]:
    """Strict-JSON LLM call wrapper — returns the raw response text or None."""
    try:
        from core.llm_client import get_llm_client
        client = get_llm_client()
        return client.call(
            user_message=user_message,
            system_prompt=system_prompt,
            max_tokens=512,
            use_cache=True,
            cache_key_extra=cache_key_extra,
        )
    except Exception as e:
        logger.warning(f"LLM fixer call failed: {e}")
        return None


# ── PDF discovery ──────────────────────────────────────────────────


_HAWB_PREFIX_RE = re.compile(r"^HAWB", re.IGNORECASE)


def _find_bl_pdf(attachment_paths: List[str]) -> Optional[str]:
    """Locate the BL/Declaration PDF in ``attachment_paths``.

    Preference order:
        1. Any ``*-Declaration.pdf`` (the renamed declaration attachment).
        2. The first ``HAWB*-*.pdf`` (legacy BL document layout).
    Returns ``None`` when no PDF candidate exists.
    """
    if not attachment_paths:
        return None
    pdfs = [p for p in attachment_paths
            if isinstance(p, str) and p.lower().endswith(".pdf")]
    if not pdfs:
        return None
    # 1. Declaration suffix wins.
    for p in pdfs:
        if os.path.basename(p).lower().endswith("-declaration.pdf"):
            return p
    # 2. First HAWB-prefixed PDF.
    for p in pdfs:
        if _HAWB_PREFIX_RE.match(os.path.basename(p)):
            return p
    return None


# ── JSON helpers ───────────────────────────────────────────────────


def _extract_json_field(text: str, key: str) -> Optional[str]:
    """Extract ``key`` from a (possibly fenced) JSON blob in ``text``."""
    if not text:
        return None
    # Strip code fences if present.
    cleaned = re.sub(r"^```(?:json)?|```$", "", text.strip(),
                     flags=re.MULTILINE).strip()
    try:
        data = json.loads(cleaned)
    except Exception:
        # Fall back to a non-greedy scan for {...}
        m = re.search(r"\{[^{}]*\}", cleaned, re.DOTALL)
        if not m:
            return None
        try:
            data = json.loads(m.group(0))
        except Exception:
            return None
    if not isinstance(data, dict):
        return None
    val = data.get(key)
    if val is None:
        return None
    return str(val).strip() or None


# ── Fixer entry points ─────────────────────────────────────────────


def fix_waybill(ep: dict, _finding: dict, *, output_dir: str = "",
                **_kw) -> bool:
    """Re-extract waybill from the Declaration / BL PDF."""
    pdf = _find_bl_pdf(ep.get("attachment_paths") or [])
    if not pdf:
        return False
    bl = parse_bl_pdf(pdf)
    candidate = (bl.get("bl_number") or "").strip()
    if not candidate:
        text = _extract_pdf_text(pdf)
        if text:
            resp = _call_llm(
                "You extract structured fields from shipping documents. "
                "Return STRICT JSON only.",
                "Extract the Bill of Lading / waybill number from the text "
                "below. Respond as JSON: {\"waybill\": \"<value or empty>\"}.\n\n"
                f"---\n{text[:4000]}\n---",
                cache_key_extra="waybill",
            )
            candidate = (_extract_json_field(resp or "", "waybill") or "").strip()
    if not candidate or len(candidate) < 4:
        return False
    ep["waybill"] = candidate
    return True


def fix_weight(ep: dict, _finding: dict, *, output_dir: str = "",
               **_kw) -> bool:
    """Re-extract shipment weight (kg) from the BL grand total / OCR."""
    pdf = _find_bl_pdf(ep.get("attachment_paths") or [])
    if not pdf:
        return False
    bl = parse_bl_pdf(pdf)
    weight = (bl.get("grand_total") or {}).get("weight_kg") or 0
    try:
        weight = float(weight)
    except (TypeError, ValueError):
        weight = 0.0
    if weight <= 0:
        text = _extract_pdf_text(pdf)
        if text:
            resp = _call_llm(
                "You extract structured fields from shipping documents. "
                "Return STRICT JSON only.",
                "Extract the total shipment weight in kilograms from the BL "
                "text below. Respond as JSON: {\"weight_kg\": <number>}.\n\n"
                f"---\n{text[:4000]}\n---",
                cache_key_extra="weight_kg",
            )
            try:
                weight = float(_extract_json_field(resp or "", "weight_kg") or 0)
            except (TypeError, ValueError):
                weight = 0.0
    if weight <= 0:
        return False
    ep["weight"] = str(weight)
    return True


def fix_packages(ep: dict, _finding: dict, *, output_dir: str = "",
                 **_kw) -> bool:
    """Re-extract package count from the BL grand total / OCR."""
    pdf = _find_bl_pdf(ep.get("attachment_paths") or [])
    if not pdf:
        return False
    bl = parse_bl_pdf(pdf)
    pkgs = (bl.get("grand_total") or {}).get("packages") or 0
    try:
        pkgs = int(pkgs)
    except (TypeError, ValueError):
        pkgs = 0
    if pkgs <= 0:
        text = _extract_pdf_text(pdf)
        if text:
            resp = _call_llm(
                "You extract structured fields from shipping documents. "
                "Return STRICT JSON only.",
                "Extract the total number of packages / pieces from the BL "
                "text below. Respond as JSON: {\"packages\": <integer>}.\n\n"
                f"---\n{text[:4000]}\n---",
                cache_key_extra="packages",
            )
            try:
                pkgs = int(float(_extract_json_field(resp or "", "packages") or 0))
            except (TypeError, ValueError):
                pkgs = 0
    if pkgs <= 0:
        return False
    ep["packages"] = str(pkgs)
    return True


def fix_consignee_name(ep: dict, _finding: dict, *, output_dir: str = "",
                       **_kw) -> bool:
    """Re-extract consignee name from BL / OCR.

    Rejects re-extracted values that would re-trigger
    ``consignee_name_garbage`` (length > 60).
    """
    pdf = _find_bl_pdf(ep.get("attachment_paths") or [])
    if not pdf:
        return False
    bl = parse_bl_pdf(pdf)
    name = ((bl.get("consignee") or {}).get("name") or "").strip()
    if not name or len(name) > _CONSIGNEE_NAME_MAX:
        text = _extract_pdf_text(pdf)
        if text:
            resp = _call_llm(
                "You extract structured fields from shipping documents. "
                "Return STRICT JSON only.",
                "Extract the consignee name from the BL text below — name "
                "only, no address. Respond as JSON: "
                "{\"consignee_name\": \"<value>\"}.\n\n"
                f"---\n{text[:4000]}\n---",
                cache_key_extra="consignee_name",
            )
            name = (_extract_json_field(resp or "", "consignee_name") or "").strip()
    if not name or len(name) > _CONSIGNEE_NAME_MAX:
        return False
    ep["consignee_name"] = name
    return True


def fix_consignee_address(ep: dict, _finding: dict, *, output_dir: str = "",
                          **_kw) -> bool:
    """Re-extract consignee address from BL / OCR.

    Rejects re-extracted values that would re-trigger
    ``consignee_address_garbage`` (length > 200).
    """
    pdf = _find_bl_pdf(ep.get("attachment_paths") or [])
    if not pdf:
        return False
    bl = parse_bl_pdf(pdf)
    addr = ((bl.get("consignee") or {}).get("address") or "").strip()
    if not addr or len(addr) > _CONSIGNEE_ADDR_MAX:
        text = _extract_pdf_text(pdf)
        if text:
            resp = _call_llm(
                "You extract structured fields from shipping documents. "
                "Return STRICT JSON only.",
                "Extract the consignee address from the BL text below — "
                "address only, no name. Respond as JSON: "
                "{\"consignee_address\": \"<value>\"}.\n\n"
                f"---\n{text[:4000]}\n---",
                cache_key_extra="consignee_address",
            )
            addr = (_extract_json_field(resp or "", "consignee_address") or "").strip()
    if not addr or len(addr) > _CONSIGNEE_ADDR_MAX:
        return False
    ep["consignee_address"] = addr
    return True


# ── Registry plug ──────────────────────────────────────────────────


def register_into(registry: Dict[str, Callable[..., bool]]) -> None:
    """Register every LLM-backed fixer into the shared ``_FIXERS`` table.

    Mechanical entries already in ``registry`` are NOT overwritten — the
    mechanical pass always wins over the LLM pass for kinds that have a
    deterministic fix. New keys (``waybill_missing``, ``weight_zero`` etc.)
    are added.
    """
    new_entries = {
        "waybill_missing": fix_waybill,
        "waybill_short": fix_waybill,
        "weight_zero": fix_weight,
        "packages_zero": fix_packages,
        "consignee_missing": fix_consignee_name,
        "consignee_name_garbage": fix_consignee_name,
        "consignee_address_garbage": fix_consignee_address,
    }
    for key, fn in new_entries.items():
        registry.setdefault(key, fn)
