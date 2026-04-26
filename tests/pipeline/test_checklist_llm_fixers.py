"""LLM/text re-extraction fixers for ``shipment_checklist`` failures.

Phase B of the fix-then-flag-then-send architecture (Joseph 2026-04-25).
Mechanical fixers (Phase A, ``checklist_fixer``) handle deterministic edits.
This module handles the residue: re-derive missing waybill / weight /
packages / consignee fields from the BL/Declaration PDF, and re-extract
mis-parsed item rows from the source invoice PDF when a mechanical fix is
not applicable.

Tests stub out the LLM client and BL parser so they never hit the network.
"""

from __future__ import annotations

import os
import sys

import pytest

_PIPELINE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "pipeline"))
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)


def _finding(check, severity="block"):
    return {
        "check": check,
        "severity": severity,
        "message": f"synthetic {check}",
        "field": "",
        "value": "",
        "fix_hint": "",
    }


def _write_fake_pdf(p):
    p.write_bytes(b"%PDF-1.4 fake")


# ── Registry surface ───────────────────────────────────────────────


def test_llm_fixers_module_exposes_register_into():
    """``register_into(_FIXERS)`` plugs LLM fixers into the mechanical registry."""
    from checklist_llm_fixers import register_into

    fake = {}
    register_into(fake)
    # At minimum the catalogued LLM-recoverable checks are wired.
    expected = {
        "waybill_missing",
        "waybill_short",
        "weight_zero",
        "packages_zero",
        "consignee_missing",
        "consignee_name_garbage",
        "consignee_address_garbage",
    }
    assert expected.issubset(set(fake.keys()))


# ── BL field fixers (mocked parse_bl_pdf, no LLM) ──────────────────


def test_waybill_missing_fixed_from_bl_parser(tmp_path, monkeypatch):
    """parse_bl_pdf returns a BL number → waybill is filled from it."""
    from checklist_llm_fixers import fix_waybill

    decl = tmp_path / "anything-Declaration.pdf"
    _write_fake_pdf(decl)
    ep = {"waybill": "", "attachment_paths": [str(decl)]}

    import checklist_llm_fixers as mod

    monkeypatch.setattr(
        mod,
        "parse_bl_pdf",
        lambda p: {"bl_number": "HAWB9333496", "consignee": {}, "grand_total": {}},
    )
    # LLM should NOT be invoked when the BL parser succeeds.
    monkeypatch.setattr(mod, "_call_llm", lambda *a, **kw: pytest.fail("LLM should not be called"))

    assert fix_waybill(ep, _finding("waybill_missing"), output_dir=str(tmp_path)) is True
    assert ep["waybill"] == "HAWB9333496"


def test_waybill_missing_falls_back_to_llm(tmp_path, monkeypatch):
    """When parse_bl_pdf returns nothing, LLM extracts from OCR text."""
    from checklist_llm_fixers import fix_waybill

    decl = tmp_path / "anything-Declaration.pdf"
    _write_fake_pdf(decl)
    ep = {"waybill": "", "attachment_paths": [str(decl)]}

    import checklist_llm_fixers as mod

    monkeypatch.setattr(
        mod, "parse_bl_pdf", lambda p: {"bl_number": "", "consignee": {}, "grand_total": {}}
    )
    monkeypatch.setattr(mod, "_extract_pdf_text", lambda p: "BL #HAWB777")
    monkeypatch.setattr(mod, "_call_llm", lambda system, user, **kw: '{"waybill": "HAWB777"}')

    assert fix_waybill(ep, _finding("waybill_missing"), output_dir=str(tmp_path)) is True
    assert ep["waybill"] == "HAWB777"


def test_waybill_missing_returns_false_when_no_bl_pdf(tmp_path, monkeypatch):
    """No declaration/BL PDF in attachments → cannot fix → False (skipped)."""
    from checklist_llm_fixers import fix_waybill

    ep = {"waybill": "", "attachment_paths": []}
    import checklist_llm_fixers as mod

    monkeypatch.setattr(mod, "parse_bl_pdf", lambda p: pytest.fail("should not be called"))
    monkeypatch.setattr(mod, "_call_llm", lambda *a, **kw: pytest.fail("should not be called"))

    assert fix_waybill(ep, _finding("waybill_missing"), output_dir=str(tmp_path)) is False
    assert ep["waybill"] == ""


def test_weight_zero_fixed_from_bl_grand_total(tmp_path, monkeypatch):
    from checklist_llm_fixers import fix_weight

    decl = tmp_path / "HAWB-Declaration.pdf"
    _write_fake_pdf(decl)
    ep = {"weight": "0", "attachment_paths": [str(decl)]}

    import checklist_llm_fixers as mod

    monkeypatch.setattr(mod, "parse_bl_pdf", lambda p: {"grand_total": {"weight_kg": 12.5}})
    monkeypatch.setattr(mod, "_call_llm", lambda *a, **kw: pytest.fail("LLM should not be called"))

    assert fix_weight(ep, _finding("weight_zero"), output_dir=str(tmp_path)) is True
    assert float(ep["weight"]) == 12.5


def test_weight_zero_skipped_when_bl_returns_zero(tmp_path, monkeypatch):
    """If BL grand_total has no weight, fixer reports skipped (no fabrication)."""
    from checklist_llm_fixers import fix_weight

    decl = tmp_path / "HAWB-Declaration.pdf"
    _write_fake_pdf(decl)
    ep = {"weight": "0", "attachment_paths": [str(decl)]}

    import checklist_llm_fixers as mod

    monkeypatch.setattr(mod, "parse_bl_pdf", lambda p: {"grand_total": {"weight_kg": 0}})
    monkeypatch.setattr(mod, "_extract_pdf_text", lambda p: "")
    monkeypatch.setattr(mod, "_call_llm", lambda *a, **kw: None)

    assert fix_weight(ep, _finding("weight_zero"), output_dir=str(tmp_path)) is False
    assert ep["weight"] == "0"


def test_packages_zero_fixed_from_bl_grand_total(tmp_path, monkeypatch):
    from checklist_llm_fixers import fix_packages

    decl = tmp_path / "HAWB-Declaration.pdf"
    _write_fake_pdf(decl)
    ep = {"packages": "0", "attachment_paths": [str(decl)]}

    import checklist_llm_fixers as mod

    monkeypatch.setattr(mod, "parse_bl_pdf", lambda p: {"grand_total": {"packages": 7}})

    assert fix_packages(ep, _finding("packages_zero"), output_dir=str(tmp_path)) is True
    assert ep["packages"] == "7"


# ── Consignee fixers ───────────────────────────────────────────────


def test_consignee_missing_fixed_from_bl(tmp_path, monkeypatch):
    from checklist_llm_fixers import fix_consignee_name

    decl = tmp_path / "HAWB-Declaration.pdf"
    _write_fake_pdf(decl)
    ep = {"consignee_name": "", "attachment_paths": [str(decl)]}

    import checklist_llm_fixers as mod

    monkeypatch.setattr(
        mod,
        "parse_bl_pdf",
        lambda p: {
            "consignee": {"name": "ACME GRENADA LIMITED", "address": "St George's, Grenada"}
        },
    )

    assert fix_consignee_name(ep, _finding("consignee_missing"), output_dir=str(tmp_path)) is True
    assert ep["consignee_name"] == "ACME GRENADA LIMITED"


def test_consignee_name_garbage_truncated_when_too_long(tmp_path, monkeypatch):
    """A re-extracted name longer than 60 chars is rejected (would re-trigger)."""
    from checklist_llm_fixers import fix_consignee_name

    decl = tmp_path / "HAWB-Declaration.pdf"
    _write_fake_pdf(decl)
    ep = {"consignee_name": "X" * 80, "attachment_paths": [str(decl)]}

    import checklist_llm_fixers as mod

    # BL parser returns a still-too-long name → fixer must NOT accept it.
    monkeypatch.setattr(
        mod,
        "parse_bl_pdf",
        lambda p: {"consignee": {"name": "Y" * 90, "address": ""}},
    )
    monkeypatch.setattr(mod, "_extract_pdf_text", lambda p: "")
    monkeypatch.setattr(mod, "_call_llm", lambda *a, **kw: None)

    assert (
        fix_consignee_name(ep, _finding("consignee_name_garbage"), output_dir=str(tmp_path))
        is False
    )
    assert ep["consignee_name"] == "X" * 80  # unchanged


def test_consignee_address_garbage_fixed_from_bl(tmp_path, monkeypatch):
    from checklist_llm_fixers import fix_consignee_address

    decl = tmp_path / "HAWB-Declaration.pdf"
    _write_fake_pdf(decl)
    ep = {"consignee_address": "Z" * 250, "attachment_paths": [str(decl)]}

    import checklist_llm_fixers as mod

    monkeypatch.setattr(
        mod,
        "parse_bl_pdf",
        lambda p: {"consignee": {"name": "ACME", "address": "St George's, Grenada"}},
    )

    assert (
        fix_consignee_address(ep, _finding("consignee_address_garbage"), output_dir=str(tmp_path))
        is True
    )
    assert ep["consignee_address"] == "St George's, Grenada"


# ── BL pdf discovery ───────────────────────────────────────────────


def test_find_bl_pdf_prefers_declaration_suffix(tmp_path):
    from checklist_llm_fixers import _find_bl_pdf

    decl = tmp_path / "HAWB9333496-Declaration.pdf"
    other = tmp_path / "HAWB9333496-invoice-7.pdf"
    _write_fake_pdf(decl)
    _write_fake_pdf(other)
    paths = [str(other), str(decl)]  # decl second on purpose

    assert _find_bl_pdf(paths) == str(decl)


def test_find_bl_pdf_falls_back_to_first_hawb_pdf(tmp_path):
    from checklist_llm_fixers import _find_bl_pdf

    p1 = tmp_path / "HAWB9333496-1.pdf"
    p2 = tmp_path / "extra.pdf"
    _write_fake_pdf(p1)
    _write_fake_pdf(p2)

    assert _find_bl_pdf([str(p2), str(p1)]) == str(p1)


def test_find_bl_pdf_returns_none_for_no_pdfs():
    from checklist_llm_fixers import _find_bl_pdf

    assert _find_bl_pdf([]) is None
    assert _find_bl_pdf(["/x/items.xlsx"]) is None


# ── attempt_fixes integration: LLM pass runs after mechanical pass ─


def test_attempt_fixes_invokes_llm_fixers_for_unfixed_kinds(tmp_path, monkeypatch):
    """End-to-end: a waybill_missing finding runs through the LLM fixer."""
    from checklist_fixer import attempt_fixes

    decl = tmp_path / "HAWB-Declaration.pdf"
    _write_fake_pdf(decl)

    import checklist_llm_fixers as mod

    monkeypatch.setattr(
        mod, "parse_bl_pdf", lambda p: {"bl_number": "HAWB-OK", "consignee": {}, "grand_total": {}}
    )

    ep = {"waybill": "", "attachment_paths": [str(decl)]}
    rep = attempt_fixes(ep, [_finding("waybill_missing")], output_dir=str(tmp_path))
    assert ep["waybill"] == "HAWB-OK"
    assert "waybill_missing" in rep["fixed"]
