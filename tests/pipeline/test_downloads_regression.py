"""Critical verification test over the Downloads folder corpus.

This is the PRIMARY regression test for the restructured pipeline.  It
feeds every top-level PDF from the user's Downloads folder through
``pipeline/run.py`` one at a time (never all at once), categorises the
result, runs the full invariant checklist on any XLSX produced, and
emits a session-wide summary.

Path resolution order (matches the TS SSOT at
``app/main/services/file-watcher.ts:56``):

    1. $AUTOINVOICE_DOWNLOADS_DIR     (opt-in override)
    2. workspace/Downloads            (junction created by file-watcher)
    3. /mnt/d/OneDrive/Clients/WebSource/Downloads   (absolute fallback)

The entire module is skipped at collection time if none of the above
resolve to an accessible directory.

Per-file categorisation:

    processed_pass      — pipeline emitted XLSX and ALL invariants passed
    processed_fail      — pipeline emitted XLSX but at least one invariant failed
    skipped_non_invoice — pipeline correctly classified as non-invoice, no XLSX
    error_crash         — pipeline exited non-zero, or REPORT:JSON missing

Usage:

    # Full corpus (123 PDFs, takes a long time)
    source .venv/bin/activate
    pytest tests/pipeline/test_downloads_regression.py -v -m integration

    # Subset for development — edit SLICE below or use -k
    pytest tests/pipeline/test_downloads_regression.py -v -m integration \
        -k "test_downloads_pdf[<filename>]"
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from openpyxl import load_workbook

from tests.pipeline.invariants import run_all_invariants

# ── Paths ──────────────────────────────────────────────────────────────
_REPO_ROOT = Path(__file__).resolve().parents[2]
_PIPELINE_DIR = _REPO_ROOT / "pipeline"
_RUN_PY = _PIPELINE_DIR / "run.py"
_CET_DB = _REPO_ROOT / "data" / "cet.db"

_TIMEOUT_SECONDS = 600  # per-PDF timeout


def _resolve_downloads_dir() -> Path | None:
    """Locate the Downloads folder (matches file-watcher.ts:56 SSOT).

    Tries the env override, then the repo junction, then the WSL mount
    fallback.  Returns ``None`` if none of them exist.
    """
    env = os.environ.get("AUTOINVOICE_DOWNLOADS_DIR")
    if env and Path(env).is_dir():
        return Path(env)
    junction = _REPO_ROOT / "workspace" / "Downloads"
    if junction.is_dir():
        return junction
    fallback = Path("/mnt/d/OneDrive/Clients/WebSource/Downloads")
    if fallback.is_dir():
        return fallback
    return None


_DOWNLOADS_DIR = _resolve_downloads_dir()

if _DOWNLOADS_DIR is None:
    pytest.skip(
        "Downloads folder not resolvable (AUTOINVOICE_DOWNLOADS_DIR unset, "
        "workspace/Downloads junction missing, /mnt/d/... unavailable)",
        allow_module_level=True,
    )


def _discover_top_level_pdfs(root: Path) -> list[Path]:
    """Return all top-level PDFs (no recursion, no subdirs).

    Recursion would descend into ``Auto Brokerage - WebSource/``, which
    is owned by the golden-comparison test (``test_auto_brokerage_golden``).
    """
    pdfs: list[Path] = []
    for entry in sorted(root.iterdir()):
        if entry.is_file() and entry.suffix.lower() == ".pdf":
            pdfs.append(entry)
    return pdfs


_ALL_PDFS = _discover_top_level_pdfs(_DOWNLOADS_DIR) if _DOWNLOADS_DIR else []

# ── Dev slice — uncomment to restrict the corpus during iteration ──────
# During development, uncomment the SLICE line below to run on only a
# handful of PDFs; comment it out again before checking in so the full
# corpus runs under ``pytest -m integration``.
# SLICE = slice(0, 3)
# _ALL_PDFS = _ALL_PDFS[SLICE]

if not _ALL_PDFS:
    pytest.skip(
        f"no top-level PDFs found in {_DOWNLOADS_DIR}",
        allow_module_level=True,
    )


def _sanitize_filename(name: str) -> str:
    """Sanitise ``name`` to ASCII-only so the pipeline + shell won't choke.

    Downloads PDFs come from email auto-saves and often contain
    smart-quotes, curly apostrophes, commas, and stray Unicode.  We
    strip to ``[A-Za-z0-9._-]`` with underscore replacements.
    """
    base = re.sub(r"[^A-Za-z0-9._\- ]", "_", name)
    base = re.sub(r"\s+", "_", base)
    base = re.sub(r"_+", "_", base)
    return base.strip("_")


# Session-scoped results collector — consumed by the summary fixture.
_RESULTS: list[dict] = []


@pytest.fixture(scope="session", autouse=True)
def _summary_reporter(tmp_path_factory):
    """Print a session-end summary of per-file results.

    Writes a JSON report to ``tmp_path_factory.mktemp("summary")`` and
    echoes a human-readable table to stdout so the broker running the
    suite can see pass/fail counts at a glance.
    """
    yield
    if not _RESULTS:
        return

    summary_dir = tmp_path_factory.mktemp("downloads_summary")
    summary_path = summary_dir / "downloads_summary.json"
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(_RESULTS, f, indent=2, default=str)

    buckets: dict[str, list[str]] = {
        "processed_pass": [],
        "processed_fail": [],
        "skipped_non_invoice": [],
        "error_crash": [],
    }
    for r in _RESULTS:
        buckets.setdefault(r["category"], []).append(r["filename"])

    print("\n" + "=" * 72)
    print(f"Downloads regression summary  ({len(_RESULTS)} PDFs)")
    print("=" * 72)
    for cat in ("processed_pass", "processed_fail", "skipped_non_invoice", "error_crash"):
        names = buckets.get(cat, [])
        print(f"  {cat:<22} {len(names)}")
    print(f"\nFull report: {summary_path}")
    print("=" * 72)


@pytest.mark.integration
@pytest.mark.requires_downloads
@pytest.mark.parametrize(
    "pdf_path",
    _ALL_PDFS,
    ids=[p.name for p in _ALL_PDFS],
)
def test_downloads_pdf(pdf_path: Path, tmp_path: Path) -> None:
    """Process a single Downloads PDF and categorise the result.

    Staging: copy the PDF into ``tmp_path/stage`` with a sanitised
    filename, run ``pipeline/run.py --input-dir <stage> --output-dir
    <tmp_path/out> --json-output``, and interpret the outcome.

    The test PASSES when the category is ``processed_pass`` OR
    ``skipped_non_invoice``.  It FAILS on ``processed_fail`` (pipeline
    produced an XLSX but at least one invariant failed — real
    regression) and on ``error_crash`` (pipeline raised / exited
    nonzero — unclassified error).
    """
    stage_dir = tmp_path / "stage"
    stage_dir.mkdir()
    output_dir = tmp_path / "out"
    output_dir.mkdir()

    safe_name = _sanitize_filename(pdf_path.name) or "input.pdf"
    if not safe_name.lower().endswith(".pdf"):
        safe_name += ".pdf"
    staged = stage_dir / safe_name
    shutil.copy2(pdf_path, staged)

    result: dict = {
        "filename": pdf_path.name,
        "safe_name": safe_name,
        "category": "error_crash",
        "failures": [],
        "returncode": None,
        "report": None,
    }

    try:
        proc = subprocess.run(  # noqa: S603 - trusted local path
            [
                sys.executable,
                "-u",
                str(_RUN_PY),
                "--input-dir",
                str(stage_dir),
                "--output-dir",
                str(output_dir),
                "--json-output",
            ],
            cwd=str(_PIPELINE_DIR),
            capture_output=True,
            text=True,
            timeout=_TIMEOUT_SECONDS,
        )
        result["returncode"] = proc.returncode
    except subprocess.TimeoutExpired as e:
        result["category"] = "error_crash"
        result["failures"] = [("timeout", f"subprocess timed out after {_TIMEOUT_SECONDS}s")]
        _RESULTS.append(result)
        pytest.fail(f"[{pdf_path.name}] pipeline timed out: {e}")
    except Exception as e:  # noqa: BLE001
        result["category"] = "error_crash"
        result["failures"] = [("subprocess_error", str(e))]
        _RESULTS.append(result)
        pytest.fail(f"[{pdf_path.name}] pipeline subprocess error: {e}")

    if proc.returncode != 0:
        result["category"] = "error_crash"
        result["failures"] = [
            ("nonzero_exit", f"returncode={proc.returncode}"),
            ("stderr_tail", proc.stderr[-500:]),
        ]
        _RESULTS.append(result)
        pytest.fail(
            f"[{pdf_path.name}] pipeline exited with {proc.returncode}.\n"
            f"stderr (last 500 chars): {proc.stderr[-500:]}"
        )

    # Parse REPORT:JSON line from stdout.
    report = None
    for line in proc.stdout.splitlines():
        stripped = line.strip()
        if stripped.startswith("REPORT:JSON:"):
            try:
                report = json.loads(stripped[len("REPORT:JSON:") :])
            except json.JSONDecodeError:
                report = None
            break
    result["report"] = report

    if report is None:
        result["category"] = "error_crash"
        result["failures"] = [("missing_report", "no REPORT:JSON line in stdout")]
        _RESULTS.append(result)
        pytest.fail(f"[{pdf_path.name}] pipeline did not emit REPORT:JSON")

    status = report.get("status", "")
    invoice_count = report.get("invoice_count", 0)

    # Collect any XLSX produced.
    produced: list[Path] = []
    for d in (output_dir, stage_dir):
        for xlsx in d.rglob("*.xlsx"):
            produced.append(xlsx)

    if not produced:
        # No XLSX — either non-invoice skip (OK) or an error we missed.
        if status == "success" and invoice_count == 0:
            result["category"] = "skipped_non_invoice"
            _RESULTS.append(result)
            return  # PASS
        result["category"] = "error_crash"
        result["failures"] = [
            ("no_xlsx_no_skip", f"status={status!r} invoice_count={invoice_count}"),
        ]
        _RESULTS.append(result)
        pytest.fail(
            f"[{pdf_path.name}] no XLSX produced and not a clean skip: "
            f"status={status!r} invoice_count={invoice_count}"
        )

    # XLSX produced — run the full invariant checklist on each sheet.
    per_file_failures: list[tuple[str, str]] = []
    for xlsx_path in produced:
        try:
            wb = load_workbook(str(xlsx_path))
        except Exception as e:  # noqa: BLE001
            per_file_failures.append((f"load::{xlsx_path.name}", str(e)))
            continue
        for sheet in wb.worksheets:
            failures = run_all_invariants(
                sheet,
                mode="minimal",
                cet_db_path=str(_CET_DB) if _CET_DB.exists() else None,
                xlsx_path=str(xlsx_path),
            )
            for name, msg in failures:
                per_file_failures.append((f"{xlsx_path.name}::{sheet.title}::{name}", msg))
        wb.close()

    if per_file_failures:
        result["category"] = "processed_fail"
        result["failures"] = per_file_failures
        _RESULTS.append(result)
        lines = [f"[{pdf_path.name}] invariant failures:"]
        for name, msg in per_file_failures:
            lines.append(f"  - {name}: {msg}")
        pytest.fail("\n".join(lines))

    result["category"] = "processed_pass"
    _RESULTS.append(result)
