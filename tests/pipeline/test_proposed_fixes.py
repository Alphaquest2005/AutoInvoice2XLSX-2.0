"""Phase 2 — Proposed Fixes email + YAML patch.

These tests guard the behaviour of ``pipeline/proposed_fixes.py`` and its
sidecar handoff to ``send_shipment_email.py``:

* ``detect_uncertain_invoices`` — only flags invoices that carry
  ``data_quality_notes``, ``invoice_total_uncertain``, or per-item
  ``data_quality`` markers.  Clean invoices must be silently skipped.
* ``build_fixes_yaml`` — round-trips as valid YAML, carries the expected
  shape (waybill, replay_mode, invoices[].override blocks), and lists the
  current values verbatim so the reviewer can see what they're correcting.
* ``build_fixes_body`` — mentions the waybill, invoice number, residual
  and the reply instructions a human needs to respond.
* ``save_fixes_artifacts`` — writes both the YAML file and the
  ``_proposed_fixes_params.json`` sidecar that SHEET sends from.
* ``send_shipment_email.py`` — auto-discovers the sidecar next to
  ``_email_params.json`` and sends a second email via the configured
  ``email_fixes_recipient`` mailbox.
"""

from __future__ import annotations

import json
import os
import sys
from types import SimpleNamespace

import pytest
import yaml

_PIPELINE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "pipeline"))
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)

from proposed_fixes import (  # noqa: E402
    UncertainInvoice,
    build_fixes_body,
    build_fixes_yaml,
    detect_uncertain_invoices,
    save_fixes_artifacts,
)

# ─── Fixtures ──────────────────────────────────────────────────────────


def _mk_result(
    invoice_num: str,
    *,
    invoice_total: float = 33.10,
    sub_total: float = 33.10,
    items: list = None,
    matched: list = None,
    notes: list = None,
    uncertain_flag: bool = False,
    pdf_path: str = "/tmp/fake.pdf",
    xlsx_path: str = "/tmp/fake.xlsx",
    supplier_name: str = "SHEIN US Services, LLC",
) -> SimpleNamespace:
    """Build a fake InvoiceResult-shaped object for detect_uncertain_invoices."""
    return SimpleNamespace(
        pdf_file=f"{invoice_num}.pdf",
        invoice_num=invoice_num,
        invoice_data={
            "invoice_num": invoice_num,
            "invoice_total": invoice_total,
            "sub_total": sub_total,
            "items": items or [],
            "data_quality_notes": list(notes or []),
            "invoice_total_uncertain": uncertain_flag,
            "freight": 0.0,
            "tax": 0.0,
            "other_cost": 0.0,
            "credits": 0.0,
            "discount": 0.0,
            "free_shipping": 0.0,
        },
        matched_items=list(matched or []),
        supplier_info={"name": supplier_name, "code": "SHEIN", "country": "US"},
        xlsx_path=xlsx_path,
        pdf_output_path=pdf_path,
        classified_count=0,
        matched_count=0,
        format_name="shein_us_invoice",
    )


# ─── detect_uncertain_invoices ─────────────────────────────────────────


def test_detect_skips_clean_invoice():
    """An invoice with no notes, no flag, no per-item quality markers is skipped."""
    clean = _mk_result(
        "INV_CLEAN",
        matched=[
            {
                "supplier_item": "SHEIN-1",
                "supplier_item_desc": "Clean item",
                "quantity": 1,
                "unit_price": 33.10,
                "total_cost": 33.10,
                "data_quality": "",
            },
        ],
    )
    assert detect_uncertain_invoices([clean]) == []


def test_detect_flags_invoice_with_quality_notes():
    """An invoice that carries a data_quality_note is included."""
    r = _mk_result(
        "INV_NOTE",
        notes=["orphan_price_recovered: +1 item $13.70"],
        matched=[
            {
                "supplier_item": "SHEIN-1",
                "supplier_item_desc": "Normal",
                "quantity": 1,
                "unit_price": 19.20,
                "total_cost": 19.20,
                "data_quality": "",
            },
            {
                "supplier_item": "SHEIN-4",
                "supplier_item_desc": "Recovered orphan",
                "quantity": 1,
                "unit_price": 13.70,
                "total_cost": 13.70,
                "data_quality": "orphan_price_recovered",
            },
        ],
    )
    uncertain = detect_uncertain_invoices([r])
    assert len(uncertain) == 1
    u = uncertain[0]
    assert u.invoice_num == "INV_NOTE"
    assert u.supplier == "SHEIN US Services, LLC"
    assert u.invoice_total == 33.10
    assert u.sub_total == 33.10
    # items_sum totals both matched rows (recovered + other)
    assert u.items_sum == pytest.approx(32.90, abs=0.01)
    # No freight/tax/etc → residual is invoice_total - items_sum
    assert u.residual == pytest.approx(0.20, abs=0.01)
    # Recovered items carry the marker; clean items land in other_items
    assert len(u.recovered_items) == 1
    assert u.recovered_items[0]["sku"] == "SHEIN-4"
    assert u.recovered_items[0]["data_quality"] == "orphan_price_recovered"
    assert len(u.other_items) == 1
    assert u.other_items[0]["sku"] == "SHEIN-1"


def test_detect_flags_invoice_with_uncertain_flag_only():
    """invoice_total_uncertain=True alone is enough to flag the invoice."""
    r = _mk_result(
        "INV_FLAG",
        uncertain_flag=True,
        matched=[
            {
                "supplier_item": "SKU",
                "supplier_item_desc": "X",
                "quantity": 1,
                "unit_price": 33.10,
                "total_cost": 33.10,
                "data_quality": "",
            },
        ],
    )
    uncertain = detect_uncertain_invoices([r])
    assert len(uncertain) == 1
    assert uncertain[0].invoice_num == "INV_FLAG"


def test_detect_mixed_batch_only_returns_uncertain():
    """In a batch of [clean, flagged, clean], only the flagged one is returned."""
    clean1 = _mk_result(
        "INV_A",
        matched=[
            {
                "supplier_item": "A",
                "supplier_item_desc": "",
                "quantity": 1,
                "unit_price": 33.10,
                "total_cost": 33.10,
                "data_quality": "",
            },
        ],
    )
    flagged = _mk_result(
        "INV_B",
        notes=["residual $0.50 absorbed into ADJUSTMENTS row"],
        matched=[
            {
                "supplier_item": "B",
                "supplier_item_desc": "",
                "quantity": 1,
                "unit_price": 32.60,
                "total_cost": 32.60,
                "data_quality": "",
            },
        ],
    )
    clean2 = _mk_result(
        "INV_C",
        matched=[
            {
                "supplier_item": "C",
                "supplier_item_desc": "",
                "quantity": 1,
                "unit_price": 33.10,
                "total_cost": 33.10,
                "data_quality": "",
            },
        ],
    )
    uncertain = detect_uncertain_invoices([clean1, flagged, clean2])
    assert [u.invoice_num for u in uncertain] == ["INV_B"]


# ─── build_fixes_yaml ──────────────────────────────────────────────────


def _sample_uncertain() -> UncertainInvoice:
    return UncertainInvoice(
        invoice_num="INVUS20240728002522185",
        supplier="SHEIN US Services, LLC",
        pdf_path="/tmp/shein_8.pdf",
        xlsx_path="/tmp/shein_8.xlsx",
        invoice_total=33.10,
        sub_total=33.10,
        items_sum=32.90,
        residual=0.20,
        notes=["orphan_price_recovered: +1 item $13.70 from OCR scan"],
        recovered_items=[
            {
                "sku": "SHEIN-4",
                "description": "Plus Size Round Neck Top",
                "quantity": 1,
                "unit_cost": 13.70,
                "total_cost": 13.70,
                "data_quality": "orphan_price_recovered",
            }
        ],
        other_items=[
            {
                "sku": "SHEIN-1",
                "description": "Other item",
                "quantity": 1,
                "unit_cost": 4.60,
                "total_cost": 4.60,
            }
        ],
    )


def test_build_yaml_is_valid_and_round_trips():
    text = build_fixes_yaml("HAWBS665535", [_sample_uncertain()])
    doc = yaml.safe_load(text)
    assert doc["waybill"] == "HAWBS665535"
    assert doc["replay_mode"] is True
    assert "instructions" in doc
    assert isinstance(doc["invoices"], list)
    assert len(doc["invoices"]) == 1


def test_build_yaml_carries_current_values_and_override_block():
    text = build_fixes_yaml("HAWBS665535", [_sample_uncertain()])
    doc = yaml.safe_load(text)
    inv = doc["invoices"][0]
    assert inv["invoice_num"] == "INVUS20240728002522185"
    assert inv["supplier"] == "SHEIN US Services, LLC"
    # Invoice-level current snapshot for the reviewer
    assert inv["current"]["sub_total"] == 33.10
    assert inv["current"]["items_sum"] == 32.90
    assert inv["current"]["invoice_total"] == 33.10
    assert inv["current"]["residual"] == 0.20
    # Recovered item shows current values + an editable override block
    rec = inv["recovered_items"][0]
    assert rec["sku"] == "SHEIN-4"
    assert rec["data_quality"] == "orphan_price_recovered"
    assert rec["current"] == {"quantity": 1, "unit_cost": 13.70, "total_cost": 13.70}
    assert rec["override"] == {
        "quantity": None,
        "unit_cost": None,
        "total_cost": None,
        "description": None,
        "delete": False,
    }
    # add_items is exposed so the reviewer can append new lines
    assert inv["add_items"] == []


def test_build_yaml_handles_multiple_invoices():
    u1 = _sample_uncertain()
    u2 = UncertainInvoice(
        invoice_num="INV_OTHER",
        supplier="Other",
        pdf_path="",
        xlsx_path="",
        invoice_total=10.00,
        sub_total=10.00,
        items_sum=10.00,
        residual=0.00,
        notes=["variance absorbed"],
    )
    doc = yaml.safe_load(build_fixes_yaml("WB", [u1, u2]))
    assert [i["invoice_num"] for i in doc["invoices"]] == [
        "INVUS20240728002522185",
        "INV_OTHER",
    ]


# ─── build_fixes_body ──────────────────────────────────────────────────


def test_build_body_mentions_key_fields():
    body = build_fixes_body(
        "HAWBS665535",
        [_sample_uncertain()],
        "proposed_fixes_HAWBS665535.yaml",
    )
    assert "HAWBS665535" in body
    assert "INVUS20240728002522185" in body
    assert "SHEIN US Services, LLC" in body
    # Monetary fields rendered with 2 decimal places
    assert "33.10" in body
    assert "32.90" in body
    assert "0.20" in body
    # Reply instructions reference the attached filename + the magic subject
    assert "proposed_fixes_HAWBS665535.yaml" in body
    assert "APPLY FIXES:" in body


# ─── save_fixes_artifacts ──────────────────────────────────────────────


def test_save_artifacts_writes_yaml_and_params_sidecar(tmp_path):
    # Make the referenced PDF/XLSX exist so they end up in attachment_paths
    pdf = tmp_path / "shein_8.pdf"
    pdf.write_bytes(b"%PDF-1.4")
    xlsx = tmp_path / "shein_8.xlsx"
    xlsx.write_bytes(b"PK")
    u = _sample_uncertain()
    u.pdf_path = str(pdf)
    u.xlsx_path = str(xlsx)

    result = save_fixes_artifacts("HAWBS665535", [u], str(tmp_path))
    assert result is not None

    yaml_path = result["yaml_path"]
    params_path = result["params_path"]
    assert os.path.exists(yaml_path)
    assert os.path.exists(params_path)
    assert os.path.basename(yaml_path) == "proposed_fixes_HAWBS665535.yaml"
    assert os.path.basename(params_path) == "_proposed_fixes_params.json"

    # YAML on disk matches what build_fixes_yaml produces
    with open(yaml_path) as f:
        doc = yaml.safe_load(f)
    assert doc["waybill"] == "HAWBS665535"
    assert doc["replay_mode"] is True

    # Sidecar shape matches what send_shipment_email.py reads
    with open(params_path) as f:
        params = json.load(f)
    assert params["kind"] == "proposed_fixes"
    assert params["waybill"] == "HAWBS665535"
    assert params["subject"] == "Proposed Fixes for shipment: HAWBS665535"
    assert params["uncertain_count"] == 1
    # YAML, PDF and XLSX are all attached
    attachments = params["attachment_paths"]
    assert yaml_path in attachments
    assert str(pdf) in attachments
    assert str(xlsx) in attachments


def test_save_artifacts_returns_none_for_empty_list(tmp_path):
    assert save_fixes_artifacts("HAWBS", [], str(tmp_path)) is None
    # Nothing should be written on the empty path
    assert os.listdir(tmp_path) == []


def test_save_artifacts_sanitizes_waybill_in_filename(tmp_path):
    u = _sample_uncertain()
    u.pdf_path = ""
    u.xlsx_path = ""
    # Spaces / slashes in the waybill must not produce a bad filename
    result = save_fixes_artifacts("HAWB S66/55", [u], str(tmp_path))
    assert result is not None
    assert os.path.basename(result["yaml_path"]) == "proposed_fixes_HAWB_S66_55.yaml"


# ─── send_shipment_email.py auto-discovery ─────────────────────────────


def test_run_helper_send_proposed_fixes_sidecar(tmp_path, monkeypatch):
    """The in-process ``--send-email`` legacy path must also deliver the
    Proposed Fixes email — not just the standalone send_shipment_email.py
    script.  We import ``run._send_proposed_fixes_sidecar``, stub SMTP,
    and verify it sends to the configured reviewer mailbox.
    """
    from core import config as core_config
    from workflow import email as workflow_email

    # Sidecar on disk — shape matches what save_fixes_artifacts writes
    fixes_params = {
        "kind": "proposed_fixes",
        "waybill": "HAWBS665535",
        "subject": "Proposed Fixes for shipment: HAWBS665535",
        "body": "Please review the attached patch",
        "yaml_path": str(tmp_path / "proposed_fixes_HAWBS665535.yaml"),
        "uncertain_count": 1,
        "attachment_paths": [],
    }
    (tmp_path / "_proposed_fixes_params.json").write_text(json.dumps(fixes_params))

    calls = []

    def fake_send_email(subject, body, attachments, recipient=None):
        calls.append({"subject": subject, "recipient": recipient})
        return True

    monkeypatch.setattr(workflow_email, "send_email", fake_send_email)
    fake_cfg = SimpleNamespace(
        email_sender="documents.websource@auto-brokerage.com",
        email_fixes_recipient="reviewer@auto-brokerage.com",
    )
    monkeypatch.setattr(core_config, "get_config", lambda *a, **kw: fake_cfg)

    import run as pipeline_run

    ok = pipeline_run._send_proposed_fixes_sidecar(str(tmp_path))

    assert ok is True
    assert len(calls) == 1
    assert calls[0]["subject"] == "Proposed Fixes for shipment: HAWBS665535"
    assert calls[0]["recipient"] == "reviewer@auto-brokerage.com"


def test_run_helper_send_proposed_fixes_sidecar_absent(tmp_path):
    """When no sidecar exists the helper is a no-op returning True."""
    import run as pipeline_run

    assert pipeline_run._send_proposed_fixes_sidecar(str(tmp_path)) is True


def test_send_shipment_email_discovers_proposed_fixes_sidecar(tmp_path, monkeypatch):
    """End-to-end: pipeline writes both files, send_shipment_email sends both.

    We stub ``workflow.email.send_email`` to capture calls instead of touching
    SMTP, then run ``send_shipment_email.main`` with ``--params`` pointing at
    a shipment params file.  Expect two send_email calls: one for the
    shipment, one for the Proposed Fixes sidecar, routed to the configured
    ``email_fixes_recipient``.
    """
    import importlib

    # 1) Shipment params (what _save_email_params would write)
    ship_params = {
        "waybill": "HAWBS665535",
        "consignee_name": "Andrea Lord",
        "consignee_code": "AL01",
        "consignee_address": "Somewhere",
        "total_invoices": 1,
        "packages": "1",
        "weight": "5",
        "country_origin": "US",
        "freight": "100",
        "man_reg": "2024 200",
        "attachment_paths": [],
        "location": "STG01",
        "office": "GDSGO",
        "expected_entries": 1,
    }
    ship_path = tmp_path / "_email_params.json"
    ship_path.write_text(json.dumps(ship_params))

    # 2) Proposed Fixes sidecar (what _maybe_save_proposed_fixes writes)
    fixes_params = {
        "kind": "proposed_fixes",
        "waybill": "HAWBS665535",
        "subject": "Proposed Fixes for shipment: HAWBS665535",
        "body": "body here",
        "yaml_path": str(tmp_path / "proposed_fixes_HAWBS665535.yaml"),
        "uncertain_count": 1,
        "attachment_paths": [],
    }
    fixes_path = tmp_path / "_proposed_fixes_params.json"
    fixes_path.write_text(json.dumps(fixes_params))

    # 3) Stub workflow.email.send_email to capture (subject, recipient) pairs.
    #    compose_email / compose_proposed_fixes_email stay real — we want
    #    their real output flowing through send_shipment_email.
    from workflow import email as workflow_email

    calls = []

    def fake_send_email(subject, body, attachments, recipient=None):
        calls.append(
            {
                "subject": subject,
                "recipient": recipient,
                "attachments": list(attachments),
            }
        )
        return True

    monkeypatch.setattr(workflow_email, "send_email", fake_send_email)

    # 4) Stub core.config.get_config so email_fixes_recipient is deterministic
    from core import config as core_config

    fake_cfg = SimpleNamespace(
        email_sender="documents.websource@auto-brokerage.com",
        email_fixes_recipient="reviewer@auto-brokerage.com",
    )
    monkeypatch.setattr(core_config, "get_config", lambda *a, **kw: fake_cfg)

    # 5) Run send_shipment_email.main with the stubbed deps
    send_shipment_email = importlib.import_module("send_shipment_email")
    monkeypatch.setattr(
        sys,
        "argv",
        ["send_shipment_email.py", "--params", str(ship_path), "--json-output"],
    )
    with pytest.raises(SystemExit) as exc_info:
        send_shipment_email.main()
    assert exc_info.value.code == 0

    # Two sends: shipment (default recipient) + fixes (reviewer mailbox)
    assert len(calls) == 2
    ship_call, fixes_call = calls
    assert ship_call["subject"] == "Shipment: HAWBS665535"
    assert ship_call["recipient"] is None  # default recipient
    assert fixes_call["subject"] == "Proposed Fixes for shipment: HAWBS665535"
    assert fixes_call["recipient"] == "reviewer@auto-brokerage.com"
