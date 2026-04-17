"""SSOT contract — the v2.0 migration's score-keeping test.

Every assertion below is a line from ``docs/V2_REQUIREMENTS.md`` translated
into Python. They start *failing* (xfail) because the current codebase has
parallel implementations under ``pipeline/`` and ``src/autoinvoice/``. As each
bounded context is migrated per ``docs/V2_MIGRATION_PLAN.md``, the
corresponding xfail is promoted to a real assertion (drop the decorator).
When every assertion is green the migration is over and ``pipeline/`` is
empty.

Rules of engagement:

* Never edit this test to accommodate new duplicate code paths — that is
  what the xfail is measuring. Add code; do not relax the contract.
* When promoting an xfail to an assertion, delete the legacy file(s) in the
  **same commit** (R4.2.2).
* This file is intentionally simple: static scans, not runtime behaviour.
  Behaviour belongs in end-to-end tests.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
PIPELINE = REPO_ROOT / "pipeline"
SRC = REPO_ROOT / "src" / "autoinvoice"
TESTS = REPO_ROOT / "tests"

# Scope scans tightly — repo root contains node_modules, .venv, data caches,
# etc. that would make rglob catastrophically slow.
SEARCH_ROOTS: tuple[Path, ...] = (PIPELINE, SRC)


def _py_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return [p for p in root.rglob("*.py") if "__pycache__" not in p.parts]


def _all_production_py_files() -> list[Path]:
    files: list[Path] = []
    for root in SEARCH_ROOTS:
        files.extend(_py_files(root))
    return files


def _files_matching(roots: tuple[Path, ...], pattern: str) -> list[Path]:
    rx = re.compile(pattern)
    hits: list[Path] = []
    for root in roots:
        for p in _py_files(root):
            try:
                if rx.search(p.read_text(encoding="utf-8", errors="ignore")):
                    hits.append(p)
            except OSError:
                continue
    return hits


# ---------------------------------------------------------------------------
# Presence checks — the v2 hex home must exist. These are green today.
# ---------------------------------------------------------------------------


def test_hex_target_directory_exists() -> None:
    """R4.1 — src/autoinvoice/ is the migration target; must be a real package."""
    assert (SRC / "__init__.py").exists(), (
        "src/autoinvoice/ is the v2 target per docs/V2_MIGRATION_PLAN.md §3"
    )


def test_requirements_and_plan_docs_exist() -> None:
    """L10 — requirements live in docs, not only in memory or chat history."""
    assert (REPO_ROOT / "docs" / "V2_REQUIREMENTS.md").exists()
    assert (REPO_ROOT / "docs" / "V2_MIGRATION_PLAN.md").exists()


# ---------------------------------------------------------------------------
# SSOT assertions — currently RED. Each turns green in its migration step.
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    reason="R4.2 — XLSX writing is spread across >1 file in pipeline/. "
    "Target: exactly src/autoinvoice/adapters/xlsx/openpyxl_writer.py. "
    "Migration step 7 in docs/V2_MIGRATION_PLAN.md.",
    strict=True,
)
def test_single_xlsx_writer() -> None:
    """Exactly one module writes XLSX output."""
    # Heuristic: files that both import openpyxl and call a save/write path.
    all_writers = _files_matching(SEARCH_ROOTS, r"openpyxl|Workbook\(|\.save\(|wb\.save")
    assert len(all_writers) == 1, (
        f"Expected 1 XLSX writer, found {len(all_writers)}: "
        f"{[str(p.relative_to(REPO_ROOT)) for p in all_writers]}"
    )


@pytest.mark.xfail(
    reason="R3.2.3 — SMTP code exists in both pipeline/workflow/email.py and "
    "src/autoinvoice/adapters/email/smtp_sender.py. "
    "Migration step 8 in docs/V2_MIGRATION_PLAN.md.",
    strict=True,
)
def test_single_email_sender() -> None:
    """Exactly one module sends email."""
    production = _files_matching(
        SEARCH_ROOTS,
        r"smtplib\.|SMTP\(|\.send_message\(|\.sendmail\(",
    )
    assert len(production) == 1, (
        f"Expected 1 production SMTP sender, found {len(production)}: "
        f"{[str(p.relative_to(REPO_ROOT)) for p in production]}"
    )


@pytest.mark.xfail(
    reason="R1.1.2 — classification rules are loaded from multiple sites "
    "(pipeline/classifier.py, pipeline/run.py, src/autoinvoice/...). "
    "Migration step 2 in docs/V2_MIGRATION_PLAN.md.",
    strict=True,
)
def test_single_classification_rules_loader() -> None:
    """Exactly one module loads classification_rules.json."""
    production = _files_matching(
        SEARCH_ROOTS,
        r"classification_rules\.json|load_classification_rules",
    )
    assert len(production) == 1, (
        f"Expected 1 classification-rules loader, found {len(production)}: "
        f"{[str(p.relative_to(REPO_ROOT)) for p in production]}"
    )


@pytest.mark.xfail(
    reason="L2/R1.2.1 — VARIANCE_CHECK / variance-writing logic is currently "
    "spread across pipeline/xlsx_validator.py, pipeline/variance_checker.py, "
    "pipeline/workflow/variance_fixer.py, pipeline/run.py. "
    "Migration step 3 in docs/V2_MIGRATION_PLAN.md.",
    strict=True,
)
def test_single_variance_writer() -> None:
    """Exactly one module writes the VARIANCE_CHECK formula."""
    production = _files_matching(
        SEARCH_ROOTS,
        r"VARIANCE[_ ]CHECK|variance_check|variance_fixer",
    )
    # Allow the domain model/port + its single adapter; flag everything else.
    assert len(production) <= 2, (
        f"Expected ≤2 variance-writing sites (domain service + adapter), "
        f"found {len(production)}: "
        f"{[str(p.relative_to(REPO_ROOT)) for p in production]}"
    )


@pytest.mark.xfail(
    reason="R4.3.1 — pipeline/run.py is 4400+ LOC; target is deletion "
    "(migration step 10). This gate trips when run.py drops below 500 LOC "
    "so we know the end is near.",
    strict=True,
)
def test_run_py_under_500_loc() -> None:
    """pipeline/run.py should shrink monotonically toward deletion."""
    run_py = PIPELINE / "run.py"
    if not run_py.exists():
        return  # deleted — migration complete for this context
    loc = sum(1 for _ in run_py.open(encoding="utf-8", errors="ignore"))
    assert loc < 500, f"pipeline/run.py is {loc} LOC; target <500 then deleted"


@pytest.mark.xfail(
    reason="R5.3.2 — end state is pipeline/ empty. This is the final gate.",
    strict=True,
)
def test_pipeline_directory_retired() -> None:
    """When pipeline/ has no .py files, the migration is structurally done."""
    remaining = _py_files(PIPELINE) if PIPELINE.exists() else []
    assert remaining == [], (
        f"{len(remaining)} Python files still live under pipeline/; "
        "each must be migrated into src/autoinvoice/ per "
        "docs/V2_MIGRATION_PLAN.md and then deleted."
    )


# ---------------------------------------------------------------------------
# Hygiene — the domain must stay pure. This one is enforced immediately.
# ---------------------------------------------------------------------------


def test_domain_has_no_adapter_imports() -> None:
    """R4.1.1 — domain imports nothing from adapters or composition."""
    domain_dir = SRC / "domain"
    if not domain_dir.exists():
        pytest.skip("domain package not yet present")
    violations: list[tuple[Path, str]] = []
    bad_prefixes = (
        "autoinvoice.adapters",
        "autoinvoice.composition",
        "autoinvoice.application",
    )
    for path in _py_files(domain_dir):
        text = path.read_text(encoding="utf-8", errors="ignore")
        for line in text.splitlines():
            s = line.strip()
            if not (s.startswith("import ") or s.startswith("from ")):
                continue
            if any(p in s for p in bad_prefixes):
                violations.append((path.relative_to(REPO_ROOT), s))
    assert not violations, (
        "Domain layer must not depend on adapters/composition/application:\n"
        + "\n".join(f"  {p}: {line}" for p, line in violations)
    )
