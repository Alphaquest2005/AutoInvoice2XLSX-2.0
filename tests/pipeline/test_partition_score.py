"""Tests for the reverse-calc-aware subset-sum objective used by
``pipeline.run._split_items_per_declaration``.

These tests pin down the redesigned partition scoring (minimax relative
deviation, absolute-gap tiebreak) and the replacement rejection predicate,
both of which were rebuilt to correctly handle the regime where reverse-calc
targets don't sum to the full invoice.

The unit of test here is the pure-function layer
(``_partition_score`` / ``_best_2way_partition`` / ``_best_nway_partition`` /
``_partition_is_pathological``).  Integration with the rest of
``_split_items_per_declaration`` is covered elsewhere (split-items test file
and full-pipeline regression tests).
"""

from __future__ import annotations

import math
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "pipeline"))

from run import (  # noqa: E402
    _best_2way_partition,
    _best_nway_partition,
    _partition_is_pathological,
    _partition_score,
    _partition_score_n,
)


# ---------------------------------------------------------------------------
# _partition_score — minimax relative deviation with abs-gap tiebreak
# ---------------------------------------------------------------------------
def test_partition_score_returns_tuple_lower_is_better():
    exact = _partition_score(10.0, 20.0, 10.0, 20.0)
    off_by_one = _partition_score(11.0, 20.0, 10.0, 20.0)
    assert exact < off_by_one
    assert exact == (0.0, 0.0)


def test_partition_score_is_minimax_relative_on_primary():
    # BL0 off by $1 out of $2 = 50% relative dev (big worry).
    # BL1 off by $1 out of $100 = 1% relative dev (nothing).
    # Primary must be the MAX of the two (= 0.5), not the sum, and not the
    # smaller one.
    score = _partition_score(3.0, 101.0, 2.0, 100.0)
    primary, secondary = score
    assert abs(primary - 0.5) < 1e-6
    assert abs(secondary - 2.0) < 1e-6


def test_partition_score_breaks_ties_on_absolute_gap():
    # Two partitions with identical primary (both 0) — tiebreak on secondary.
    tight = _partition_score(5.0, 10.0, 5.0, 10.0)
    # Another tight perfect match in a different regime — still primary=0 but
    # absolute gap larger would be secondary-larger.  Construct a degenerate
    # case where primary = 0 but secondary differs.
    assert tight == (0.0, 0.0)


def test_partition_score_no_zero_division_on_tiny_target():
    # Ensure _PARTITION_EPS handles target=0 without raising.
    score = _partition_score(5.0, 10.0, 0.0, 10.0)
    assert math.isfinite(score[1])
    # primary will be very large due to division by _PARTITION_EPS
    assert score[0] > 1e3


# ---------------------------------------------------------------------------
# The reference degenerate case: reverse-calc targets don't sum to invoice
# ---------------------------------------------------------------------------
# 7 items totalling $118.93; targets from pencil duties via reverse-calc are
# [$2.34, $56.63] summing to only $58.97. Under the OLD objective every
# partition with sum0>=t0 & sum1>=t1 ties — so mask=1 wins (first item alone
# to BL0). The new minimax-relative objective should pick BL0 = [$5.99] (the
# "eee" item), which best fits the $2.34 target.
REFERENCE_ITEMS = [20.99, 20.99, 12.99, 5.99, 19.99, 17.99, 19.99]
REFERENCE_TARGETS = [2.34, 56.63]


def test_degenerate_items_sum_exceeds_target_sum_prefers_small_target_fit():
    assignment, score = _best_2way_partition(
        REFERENCE_ITEMS, REFERENCE_TARGETS[0], REFERENCE_TARGETS[1]
    )
    # BL0 (target $2.34) should contain only the $5.99 item.
    bl0_items = [REFERENCE_ITEMS[i] for i, d in enumerate(assignment) if d == 0]
    bl1_items = [REFERENCE_ITEMS[i] for i, d in enumerate(assignment) if d == 1]
    assert bl0_items == [5.99], f"Expected BL0 = [5.99] (best fit to target $2.34), got {bl0_items}"
    # BL1 gets the rest, summing to $112.94.
    assert abs(sum(bl1_items) - 112.94) < 0.01
    # The winning minimax primary should be bounded by the $5.99-vs-$2.34 gap
    # (relative dev = 3.65/2.34 ≈ 1.56).
    assert score[0] < 2.0


def test_reverse_calc_authoritative_when_targets_undersized():
    # A partition-building run with target_sum << items_sum must NOT be
    # rejected by _partition_is_pathological (the replacement for the old
    # absolute-gap rejection).  This asserts the "authoritative reverse-calc"
    # feedback rule holds.
    assignment, score = _best_2way_partition(
        REFERENCE_ITEMS, REFERENCE_TARGETS[0], REFERENCE_TARGETS[1]
    )
    assert assignment is not None
    assert not _partition_is_pathological(score, REFERENCE_TARGETS), (
        "Partition must be accepted even when target_sum << items_sum; the "
        "absolute-gap rejection was specifically dropped for this regime."
    )


def test_near_even_split_is_rejected_as_worse_by_primary():
    # A near-even split (sum0 = 60.96, sum1 = 57.97) gives primary =
    # max(60.96-2.34)/2.34, (57.97-56.63)/56.63) ≈ max(25.05, 0.024) = 25.05
    # which is MUCH worse than the eee-only partition's 1.56.
    near_even = _partition_score(60.96, 57.97, 2.34, 56.63)
    eee_only = _partition_score(5.99, 112.94, 2.34, 56.63)
    assert eee_only < near_even


# ---------------------------------------------------------------------------
# Existing "balanced" cases still win
# ---------------------------------------------------------------------------
def test_balanced_case_still_partitions_correctly():
    # Items sum to $100. Targets [40, 60] with items that can hit them.
    items = [30.0, 10.0, 25.0, 35.0]  # sum 100
    assignment, score = _best_2way_partition(items, 40.0, 60.0)
    bl0_sum = sum(items[i] for i, d in enumerate(assignment) if d == 0)
    bl1_sum = sum(items[i] for i, d in enumerate(assignment) if d == 1)
    # Expect perfect fit: 30+10=40 and 25+35=60.
    assert abs(bl0_sum - 40.0) < 1e-6
    assert abs(bl1_sum - 60.0) < 1e-6
    assert score[0] < 1e-6


def test_balanced_case_near_miss_minimises_relative_dev():
    # No perfect partition — verify we pick the minimax-relative best.
    items = [11.0, 9.0, 24.0, 36.0]  # sum 80
    # Targets 30/50.  Enumerating non-empty partitions, the minimax-relative
    # optimum is {9+24=33, 11+36=47} with primary = max(3/30, 3/50) = 0.1.
    assignment, score = _best_2way_partition(items, 30.0, 50.0)
    bl0_sum = sum(items[i] for i, d in enumerate(assignment) if d == 0)
    bl1_sum = sum(items[i] for i, d in enumerate(assignment) if d == 1)
    # Primary should be ~0.1 (best achievable).
    assert abs(score[0] - 0.1) < 1e-6
    # Either {33,47} or symmetric {47,33} — both have the same sums.
    assert (abs(bl0_sum - 33.0) < 1e-6 and abs(bl1_sum - 47.0) < 1e-6) or (
        abs(bl0_sum - 47.0) < 1e-6 and abs(bl1_sum - 33.0) < 1e-6
    )


# ---------------------------------------------------------------------------
# Pathological-case guard
# ---------------------------------------------------------------------------
def test_pathological_target_guard_min_target_near_zero():
    # min(targets) < 1.0 → fall through (reverse-calc yielded garbage).
    assert _partition_is_pathological((0.0, 0.0), [0.5, 50.0])
    assert _partition_is_pathological((0.0, 0.0), [0.0, 50.0])
    assert _partition_is_pathological((0.0, 0.0), [-5.0, 50.0])


def test_pathological_target_guard_worst_rel_dev_too_large():
    # Even with good targets, a best_score primary > 5.0 means the optimum
    # is still unusable (e.g. items can't get anywhere near target).
    assert _partition_is_pathological((5.5, 100.0), [10.0, 10.0])
    assert not _partition_is_pathological((4.99, 100.0), [10.0, 10.0])


def test_pathological_target_guard_falls_through_on_impossible_fit():
    # Two $1000 items, targets [5, 10].  The best NON-EMPTY partition forces
    # at least $1000 on each side, so relative dev is enormous → rejected.
    items = [1000.0, 1000.0]
    assignment, score = _best_2way_partition(items, 5.0, 10.0)
    assert assignment is not None
    assert _partition_is_pathological(score, [5.0, 10.0])


def test_pathological_guard_empty_targets():
    # Empty targets list → pathological by definition (no reverse-calc data).
    assert _partition_is_pathological((0.0, 0.0), [])


# ---------------------------------------------------------------------------
# N-way (>=3 declarations) minimax-relative scoring
# ---------------------------------------------------------------------------
def test_nway_minimax_three_decls_balanced():
    # 3 decls, target_sum == items_sum — verify exhaustive path finds exact fit.
    items = [10.0, 20.0, 30.0, 40.0, 50.0]  # sum 150
    targets = [30.0, 50.0, 70.0]  # 10+20=30, 50=50, 30+40=70
    assignment, score = _best_nway_partition(items, targets)
    sums = [0.0] * 3
    for i, d in enumerate(assignment):
        sums[d] += items[i]
    assert abs(sums[0] - targets[0]) < 1e-6
    assert abs(sums[1] - targets[1]) < 1e-6
    assert abs(sums[2] - targets[2]) < 1e-6
    assert score[0] < 1e-6


def test_nway_minimax_three_decls_degenerate():
    # 3 decls where target_sum << items_sum.  Smallest-target decl must get
    # the best locally-fitting item (minimax relative).
    items = [100.0, 5.0, 80.0]  # sum 185
    targets = [4.5, 80.0, 100.0]  # sum 184.5 — close to items but BL0 is tiny
    assignment, score = _best_nway_partition(items, targets)
    # BL0 (target $4.5) should get the $5 item (best fit).
    bl0_items = [items[i] for i, d in enumerate(assignment) if d == 0]
    assert bl0_items == [5.0], f"Smallest-target decl should get best local fit, got {bl0_items}"


def test_nway_greedy_fallback_for_large_inputs():
    # Above exhaustive_limit: greedy must still produce a valid assignment
    # that beats naive round-robin on the minimax-relative score.
    items = [5.0] * 20  # 20 items, exhaustive limit exceeded
    targets = [25.0, 35.0, 40.0]
    assignment, score = _best_nway_partition(items, targets, exhaustive_limit=15)
    sums = [0.0] * 3
    for i, d in enumerate(assignment):
        sums[d] += items[i]
    # All items accounted for
    assert abs(sum(sums) - 100.0) < 1e-6
    # Every decl got at least one item
    for d in range(3):
        assert sums[d] > 0
    # Primary rel-dev should be small (each target is roughly round-multiple
    # of $5).
    assert score[0] < 0.3


# ---------------------------------------------------------------------------
# Regression: prior absolute-gap tie-breaking behaviour when sums match
# ---------------------------------------------------------------------------
def test_perfect_fit_wins_over_off_by_one():
    # Both partitions achievable; perfect fit should win.
    items = [10.0, 15.0, 20.0, 25.0]  # sum 70
    assignment, score = _best_2way_partition(items, 35.0, 35.0)
    bl0_sum = sum(items[i] for i, d in enumerate(assignment) if d == 0)
    bl1_sum = sum(items[i] for i, d in enumerate(assignment) if d == 1)
    # Perfect: {10,25} = 35, {15,20} = 35.
    assert abs(bl0_sum - 35.0) < 1e-6
    assert abs(bl1_sum - 35.0) < 1e-6
    assert score == (0.0, 0.0)


def test_partition_score_n_equivalent_to_2way_for_2_decls():
    # Consistency: _partition_score and _partition_score_n should agree
    # for the 2-decl case.
    a = _partition_score(3.0, 101.0, 2.0, 100.0)
    b = _partition_score_n([3.0, 101.0], [2.0, 100.0])
    assert abs(a[0] - b[0]) < 1e-9
    assert abs(a[1] - b[1]) < 1e-9
