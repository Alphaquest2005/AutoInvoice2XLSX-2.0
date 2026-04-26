"""Tests for pipeline.consignee_grouping — the auto-split primitive.

Pinned by the historical incident where a single non-Budget-Marine
invoice landed in a Budget Marine BL folder and was silently merged
into the same _email_params.json — the customs paperwork had to be
re-filed by hand. The grouping layer is what makes that
silent-merge impossible: every distinct (consignee_name, doc_type)
yields a separate group, and the multi-group case is surfaced
upstream as a block-severity checklist finding.

All policy values live in ``tests/fixtures/consignee_grouping/index.yaml``;
this test source contains no domain string literals.
"""

from __future__ import annotations

from tests._fixtures import load_test_manifest
from tests._paths import (
    add_pipeline_to_sys_path,
    format_name_from_test_file,
)

add_pipeline_to_sys_path()

FIXTURE_NAME = format_name_from_test_file(__file__)
MANIFEST = load_test_manifest(FIXTURE_NAME)


def _result(name: str, resolution: dict) -> dict:
    """Build a synthetic result dict carrying the resolver output."""
    return {"name": name, "consignee_resolution": dict(resolution)}


def test_homogeneous_shipment_yields_single_group():
    """All invoices for Budget Marine -> one group, no split."""
    from pipeline.consignee_grouping import (
        group_results_by_consignee,
        shipment_was_split,
    )

    bm = MANIFEST["budget_marine_resolution"]
    results = [
        _result(MANIFEST["inv_a_name"], bm),
        _result(MANIFEST["inv_b_name"], bm),
        _result(MANIFEST["inv_c_name"], bm),
    ]
    groups = group_results_by_consignee(results)

    assert len(groups) == 1
    assert groups[0]["consignee_name"] == bm["consignee_name"]
    assert groups[0]["doc_type"] == bm["doc_type"]
    assert groups[0]["source"] == MANIFEST["source_rule_scan_invoice"]
    assert len(groups[0]["results"]) == len(results)
    assert shipment_was_split(groups) is False


def test_mixed_consignee_shipment_splits_into_two_groups():
    """The historical incident — Budget Marine x N + Bernisha x 1 must
    split into two groups so the customer sees both before send."""
    from pipeline.consignee_grouping import (
        group_results_by_consignee,
        shipment_was_split,
    )

    bm = MANIFEST["budget_marine_resolution"]
    bn = MANIFEST["bernisha_resolution"]
    results = [
        _result(MANIFEST["inv_a_name"], bm),
        _result(MANIFEST["inv_b_name"], bm),
        _result(MANIFEST["inv_c_name"], bn),  # the odd one
    ]
    groups = group_results_by_consignee(results)

    assert len(groups) == 2, (
        f"expected split into 2 groups, got {len(groups)}: {[g['consignee_name'] for g in groups]}"
    )
    assert shipment_was_split(groups) is True
    # First-seen ordering is stable.
    assert groups[0]["consignee_name"] == bm["consignee_name"]
    assert groups[1]["consignee_name"] == bn["consignee_name"]
    # Bernisha group has only 1 result; Budget Marine has 2.
    bm_group = next(g for g in groups if g["consignee_name"] == bm["consignee_name"])
    bn_group = next(g for g in groups if g["consignee_name"] == bn["consignee_name"])
    assert len(bm_group["results"]) == 2
    assert len(bn_group["results"]) == 1


def test_unresolved_invoices_collapse_to_single_unresolved_group():
    """Three invoices with empty resolution must NOT yield three
    separate ‘unresolved’ groups — that would multiply the
    consignee_unrecognised checklist finding."""
    from pipeline.consignee_grouping import group_results_by_consignee

    empty = MANIFEST["empty_resolution"]
    results = [
        _result(MANIFEST["inv_a_name"], empty),
        _result(MANIFEST["inv_b_name"], empty),
        _result(MANIFEST["inv_c_name"], empty),
    ]
    groups = group_results_by_consignee(results)

    assert len(groups) == 1
    assert groups[0]["consignee_name"] == ""
    # Default doc_type comes from document_types.json::default
    from pipeline.config_loader import load_document_types

    assert groups[0]["doc_type"] == load_document_types()["default"]
    assert len(groups[0]["results"]) == len(results)


def test_bl_consignee_disagrees_when_no_group_matches():
    """BL says Budget Marine but every invoice resolved to something
    else -> disagreement detected."""
    from pipeline.consignee_grouping import (
        bl_consignee_disagrees,
        group_results_by_consignee,
    )

    bn = MANIFEST["bernisha_resolution"]
    results = [_result(MANIFEST["inv_a_name"], bn)]
    groups = group_results_by_consignee(results)

    assert bl_consignee_disagrees(groups, MANIFEST["bl_consignee_budget_marine"]) is True


def test_bl_consignee_agrees_when_any_group_matches():
    """BL says Budget Marine and at least one group is Budget Marine
    -> no disagreement (handles uppercase / case-insensitive variants)."""
    from pipeline.consignee_grouping import (
        bl_consignee_disagrees,
        group_results_by_consignee,
    )

    bm = MANIFEST["budget_marine_resolution"]
    bn = MANIFEST["bernisha_resolution"]
    results = [
        _result(MANIFEST["inv_a_name"], bm),
        _result(MANIFEST["inv_b_name"], bn),
    ]
    groups = group_results_by_consignee(results)

    assert bl_consignee_disagrees(groups, MANIFEST["bl_consignee_budget_marine"]) is False


def test_bl_consignee_disagreement_skipped_when_bl_blank():
    """Empty BL consignee string -> can't disagree, skip the check."""
    from pipeline.consignee_grouping import (
        bl_consignee_disagrees,
        group_results_by_consignee,
    )

    bm = MANIFEST["budget_marine_resolution"]
    results = [_result(MANIFEST["inv_a_name"], bm)]
    groups = group_results_by_consignee(results)

    assert bl_consignee_disagrees(groups, MANIFEST["bl_consignee_blank"]) is False
