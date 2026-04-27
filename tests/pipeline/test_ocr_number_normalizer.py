"""Tests for pipeline.ocr_number_normalizer.

All test data lives in ``tests/fixtures/ocr_number_normalizer/index.yaml``.
The driver loops the manifest once; per-case ``why`` strings make
failures self-documenting without per-case parametrize boilerplate.
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


def test_normalize_ocr_number_recovery_cases():
    from ocr_number_normalizer import normalize_ocr_number

    tol = MANIFEST["test_tolerance"]
    failures = []
    for case in MANIFEST["recovery_cases"]:
        raw = case["input"]
        expected = case["expected"]
        got = normalize_ocr_number(raw)

        if expected is None:
            if got is not None:
                failures.append(case | {"got": got})
        else:
            if got is None or abs(got - expected) >= tol:
                failures.append(case | {"got": got})

    assert not failures, failures
