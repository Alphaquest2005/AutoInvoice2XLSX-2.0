"""Coverage for the PRE-SEND ISSUES rendering path in compose_email.

The shipment checklist auto-fixer feeds residual (un-fixable) findings into
``compose_email`` via the existing ``notes`` parameter, prefixed with the
sentinel ``PRE_SEND_SENTINEL``. When that sentinel is present the body must
render the warnings as a banner at the TOP of the body so Joseph spots them
the moment he opens the email. Legacy callers that pass plain ``notes`` keep
the historical bottom placement.
"""

from __future__ import annotations

import os
import sys

# Tests under tests/pipeline have pipeline/ on sys.path via conftest, but
# workflow/ lives under pipeline/workflow so the import path matches what
# pipeline/run.py uses ("from workflow.email import compose_email").
_PIPELINE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "pipeline"))
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)

from workflow.email import PRE_SEND_SENTINEL, compose_email  # noqa: E402


def _base_kwargs():
    return dict(  # noqa: C408
        waybill="HAWB123456",
        consignee_name="ACME GRENADA",
        consignee_code="C-001",
        consignee_address="St George's, Grenada",
        total_invoices=1,
        packages="2",
        weight="15.5",
        country_origin="US",
        freight="40.00",
        man_reg="2026 28",
        attachment_paths=[],
        location="WebSource",
        office="GDWBS",
        expected_entries=1,
    )


def test_no_notes_renders_no_banner_and_no_notes_line():
    out = compose_email(**_base_kwargs())
    body = out["body"]
    assert "PRE-SEND ISSUES" not in body
    assert "Notes:" not in body
    # Sanity: standard fields still present.
    assert "BL: HAWB123456" in body


def test_legacy_notes_appear_at_bottom_of_body():
    out = compose_email(notes="legacy reviewer note", **_base_kwargs())
    body = out["body"]
    assert body.rstrip().endswith("Notes: legacy reviewer note")
    assert "PRE-SEND ISSUES" not in body


def test_pre_send_sentinel_renders_banner_at_top_of_body():
    rendered = (
        f"{PRE_SEND_SENTINEL}\n"
        "  - [block] waybill_missing: Waybill/BL number is empty.\n"
        "        Hint: Read the BL PDF to extract the BL number.\n"
        "  - [warn]  freight_zero: Freight is $0.\n"
    )
    out = compose_email(notes=rendered, **_base_kwargs())
    body = out["body"]
    # Banner heading + at least one rendered finding line are at the TOP of
    # the body, before the standard "Expected Entries" line.
    expected_idx = body.index("Expected Entries:")
    banner_idx = body.index("PRE-SEND ISSUES")
    assert banner_idx < expected_idx, "PRE-SEND ISSUES banner must render above the standard fields"
    assert "waybill_missing" in body[:expected_idx]
    assert "freight_zero" in body[:expected_idx]
    # Sentinel itself is an internal marker — must NOT leak into the email.
    assert PRE_SEND_SENTINEL not in body
    # Standard fields remain intact below the banner.
    assert "BL: HAWB123456" in body
    # Notes label ("Notes: ...") at the bottom must not duplicate the banner
    # body, otherwise the broker sees the same warnings twice.
    assert body.count("waybill_missing") == 1
