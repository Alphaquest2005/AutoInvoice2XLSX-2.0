"""Pytest fixtures for pipeline/ regression tests.

The legacy pipeline package lives outside ``src/`` and is imported via a
plain ``sys.path`` insertion.  These fixtures construct minimal synthetic
workbooks that mimic what ``bl_xlsx_generator`` / ``xlsx_generator`` produce,
without the overhead of running the full pipeline.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass

import pytest

# Put pipeline/ on sys.path so ``import xlsx_validator`` works
_PIPELINE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "pipeline"))
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)


@dataclass
class FakeCfg:
    """Minimal stand-in for ``core.config.PipelineConfig``.

    Only the attributes touched by ``variance_fixer`` helpers are defined;
    keeping this local to the tests avoids dragging in the real config loader
    (which reads ``data/settings.json``).
    """

    col_quantity: int = 11
    col_unit_cost: int = 15
    col_total_cost: int = 16
    variance_threshold: float = 0.50


@pytest.fixture
def fake_cfg() -> FakeCfg:
    return FakeCfg()


@pytest.fixture
def ungrouped_workbook():
    """Factory for a minimal ungrouped-mode workbook.

    Returns a callable ``make(inv_total, items, freight=0, insurance=0,
    tax=0, deduction=0)`` that builds an openpyxl workbook matching the
    ungrouped XLSX layout produced by ``bl_xlsx_generator`` + the labels
    defined in ``config/grouping.yaml`` (``ungrouped_totals_section``).
    """
    from tests.pipeline.xlsx_factory import build_ungrouped_workbook

    return build_ungrouped_workbook


@pytest.fixture
def grouped_workbook():
    """Factory for a minimal grouped-mode workbook.

    Returns a callable ``make(inv_total, groups, freight=0, ...)`` where
    ``groups`` is a list of ``(description, [(desc, qty, price), ...])``
    tuples.  The resulting workbook uses "SUBTOTAL (GROUPED)" labels,
    matching ``totals_section`` in ``config/grouping.yaml``.
    """
    from tests.pipeline.xlsx_factory import build_grouped_workbook

    return build_grouped_workbook
