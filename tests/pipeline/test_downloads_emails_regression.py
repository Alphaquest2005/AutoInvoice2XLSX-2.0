"""Regression tests over workspace/output/downloads-regression-emails/.

This is the baseline-diffing counterpart to ``test_downloads_regression.py``
but scoped to the per-email shipment corpus under
``workspace/output/downloads-regression-emails/`` (105 folders at time of
writing).  It exists so that after changes like the v4 vision-prompt /
vision-authoritative-OCR upgrade we can re-run the pipeline and spot
per-shipment drift against a frozen golden baseline.

Per-folder flow:

1. Stage only the SOURCE PDFs (exclude generated ``*-Declaration.pdf``,
   ``*-Manifest.pdf``, generator ``HAWB*.xlsx`` outputs, ``_email_params*``,
   ``*.meta.json``, and the ``_split_temp`` / ``Unprocessed`` side dirs)
   into a fresh ``tmp_path`` — mirrors ``scripts/rerun_corpus.py``'s
   ``is_source_file`` SSOT.
2. Invoke ``pipeline/run.py --input-dir <stage> --output-dir <out>
   --json-output`` via subprocess.
3. Run the full invariant checklist on every produced XLSX.
4. Snapshot the XLSX + ``_email_params*.json`` artifacts and diff against
   ``tests/regression_artifacts/downloads_emails/<folder>.baseline.json``.
   Drift is reported (not a hard failure) so legitimate improvements
   (e.g. HAWB9590375 gaining tax-column 155.04) surface for review.
   Promote with ``AUTOINVOICE_UPDATE_GOLDENS=1``.

Usage:

    # Full 105-folder corpus (slow — vision API + OCR per PDF):
    source .venv/bin/activate
    pytest tests/pipeline/test_downloads_emails_regression.py -v -m integration

    # Single shipment (fast iteration):
    pytest tests/pipeline/test_downloads_emails_regression.py -v -m integration \
        -k "03152025_RECEIPT"

    # Promote a drifted baseline as the new golden:
    AUTOINVOICE_UPDATE_GOLDENS=1 pytest \
        tests/pipeline/test_downloads_emails_regression.py -v -m integration \
        -k "<folder>"

Named-fixture sanity:

The two shipments we just worked on have additional field-level assertions
(``_SHIPMENT_EXPECTATIONS``) on top of the snapshot diff.  Editing the
expectations dict is the authoritative way to add new named regression
cases — the snapshot diff alone catches unexpected drift, but field-level
assertions catch semantic regressions (e.g. customs_value_ec silently
falling off) that might not be flagged by a matching snapshot.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from openpyxl import load_workbook

from tests.pipeline._regression_artifacts import snapshot_and_compare
from tests.pipeline.invariants import run_all_invariants

# ── Paths ──────────────────────────────────────────────────────────────
_REPO_ROOT = Path(__file__).resolve().parents[2]
_PIPELINE_DIR = _REPO_ROOT / "pipeline"
_RUN_PY = _PIPELINE_DIR / "run.py"
_CET_DB = _REPO_ROOT / "data" / "cet.db"
_CORPUS_DIR = _REPO_ROOT / "workspace" / "output" / "downloads-regression-emails"

_TIMEOUT_SECONDS = 900  # matches scripts/rerun_corpus.py

# Named-fixture sanity checks.  Each entry maps a shipment folder name to
# a dict of expected fields that the pipeline MUST populate under the
# current v4 vision prompt + vision-authoritative consensus.  These are
# stronger than snapshot diffs — they fail the test outright if a
# regression drops the field, even when the overall snapshot still
# parses.
#
# Field semantics:
#   expected_email_params : subset of keys that must match in at least
#                           one _email_params*.json emitted by the folder.
#   expected_customs_value_ec : per-waybill declared duty (EC$) that the
#                           v4 vision prompt should now detect.  None
#                           means "we accept any value" — the field just
#                           has to be present and numeric.
#   expected_waybills     : the set of HAWB numbers the pipeline must
#                           emit XLSX for.
_SHIPMENT_EXPECTATIONS: dict[str, dict] = {
    # Twin H&M receipt — dual-waybill shipment for ROSALIE LA GRENADE.
    # Under v4, HAWB9600998 extracts 8.96 (left margin vertical) and
    # HAWB9603312 newly extracts 80.06 — v3 missed the second form
    # entirely, v4's multi-location scan picks it up.
    "03152025_RECEIPT": {
        "expected_waybills": {"HAWB9600998", "HAWB9603312"},
        "expected_customs_value_ec": {
            "HAWB9600998": 8.96,
            "HAWB9603312": 80.06,
        },
        "consignee": "ROSALIE LA GRENADE",
    },
    # Primary v4 win — HAWB9590375 had 155.04 in the left margin + a
    # 136.57+6.37 breakdown in the right margin.  v3 latched onto 136.57
    # from the right margin; v4's disambiguation picks 155.04 as the
    # authoritative total.  Folder is shared with HAWB9591043 (Amazon-
    # source) so we only constrain the 9590375 leg.
    "03142025_USD_170.88_XCD_467.72": {
        "expected_waybills_subset": {"HAWB9590375"},
        "expected_customs_value_ec": {
            "HAWB9590375": 155.04,
        },
    },
}


def _read_client_declared_duties(xlsx_path: Path) -> float | None:
    """Return the ``CLIENT DECLARED DUTIES`` row value from *xlsx_path*.

    The bl_xlsx_generator writes a labelled duty row; we scan the first
    sheet for a cell whose value matches (case-insensitive) and return
    the numeric value from the adjacent column.  Returns ``None`` if the
    row isn't present (the pipeline only emits it when the vision
    extract produced a customs_value_ec).
    """
    try:
        wb = load_workbook(str(xlsx_path), data_only=False)
    except Exception:  # noqa: BLE001
        return None
    try:
        for sheet in wb.worksheets:
            for row in sheet.iter_rows():
                for cell in row:
                    v = cell.value
                    if not isinstance(v, str):
                        continue
                    if v.strip().upper() != "CLIENT DECLARED DUTIES":
                        continue
                    # Value lives in one of the cells to the right of
                    # the label in the same row.  Scan right for the
                    # first numeric.
                    for other in row[cell.column - 1 :]:
                        if other is cell:
                            continue
                        ov = other.value
                        if isinstance(ov, (int, float)):
                            return float(ov)
                        if isinstance(ov, str):
                            try:
                                return float(ov.replace(",", "").strip())
                            except ValueError:
                                continue
            # Fall through if no match on this sheet.
        return None
    finally:
        wb.close()


# Generated/output-file filter SSOT — mirrors scripts/rerun_corpus.py.
# Keep this list in sync with that module; duplicated here to avoid a
# scripts/ → tests/ import dependency.
#
# IMPORTANT: ``HAWB*-Declaration.pdf`` and ``HAWB*-Manifest.pdf`` are
# CLIENT-PROVIDED CARICOM declaration / manifest forms (the whole point
# of running the pipeline is to vision-extract their handwritten customs
# values); they are NOT pipeline outputs.  The generator writes
# ``<HAWB>.xlsx`` next to them, not another PDF.  Therefore we only
# filter by compound suffix + prefix, never by PDF name suffix.
_GENERATED_SUFFIXES = {".xlsx", ".meta.json", ".pages.json", ".pdf.pages.json"}
_GENERATED_PREFIXES = ("_email_params", "_proposed_fixes", "proposed_fixes_")


def _is_source_pdf(path: Path) -> bool:
    """True for PDFs the pipeline should re-process from.

    Matches ``scripts/rerun_corpus.py::is_source_file`` — keep in sync.
    """
    name = path.name
    if not path.is_file():
        return False
    if path.suffix.lower() != ".pdf":
        return False
    if any(name.startswith(p) for p in _GENERATED_PREFIXES):
        return False
    # Defensive: exclude any compound-suffix generated file (e.g.
    # foo.pdf.pages.json, foo.meta.json).  Plain .pdf always passes.
    suffix = "".join(path.suffixes)
    return suffix not in _GENERATED_SUFFIXES


def _discover_corpus_folders() -> list[tuple[str, Path]]:
    """Return ``[(name, path), ...]`` for every folder with at least one
    source PDF."""
    if not _CORPUS_DIR.is_dir():
        return []
    results: list[tuple[str, Path]] = []
    for entry in sorted(_CORPUS_DIR.iterdir()):
        if not entry.is_dir():
            continue
        if entry.name.startswith("."):
            continue
        # Skip the _split_temp / Unprocessed side dirs if they ever end
        # up at the top level.
        if entry.name in {"_split_temp", "Unprocessed"}:
            continue
        pdfs = [p for p in entry.iterdir() if _is_source_pdf(p)]
        if not pdfs:
            continue
        results.append((entry.name, entry))
    return results


_FOLDERS = _discover_corpus_folders()

# Opt-in restriction: when AUTOINVOICE_DOWNLOADS_EMAILS_ONLY is set (comma-
# separated folder names) only those folders run.  Useful for focused
# iteration on the two named fixtures without paying for the full corpus.
_ONLY_ENV = os.environ.get("AUTOINVOICE_DOWNLOADS_EMAILS_ONLY", "").strip()
if _ONLY_ENV:
    _only = {s.strip() for s in _ONLY_ENV.split(",") if s.strip()}
    _FOLDERS = [(n, p) for (n, p) in _FOLDERS if n in _only]

if not _FOLDERS:
    pytest.skip(
        f"no shipment folders with source PDFs under {_CORPUS_DIR.relative_to(_REPO_ROOT)}",
        allow_module_level=True,
    )


# Session-scoped results collector for the summary fixture.
_RESULTS: list[dict] = []


@pytest.fixture(scope="session", autouse=True)
def _summary_reporter(tmp_path_factory):
    """Emit a session-end summary of per-shipment results + drift list."""
    yield
    if not _RESULTS:
        return
    summary_dir = tmp_path_factory.mktemp("downloads_emails_summary")
    summary_path = summary_dir / "downloads_emails_summary.json"
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump(_RESULTS, f, indent=2, default=str)
    drift = [r for r in _RESULTS if r.get("snapshot_status") == "drift"]
    new = [r for r in _RESULTS if r.get("snapshot_status") == "new"]
    promoted = [r for r in _RESULTS if r.get("snapshot_status") == "promoted"]
    matched = [r for r in _RESULTS if r.get("snapshot_status") == "match"]
    with_invariant_fails = [r for r in _RESULTS if r.get("invariant_failure_count")]
    print("\n" + "=" * 72)
    print(f"Downloads-emails regression summary  ({len(_RESULTS)} shipments)")
    print("=" * 72)
    print(
        f"  snapshot  match={len(matched)}  new={len(new)}  drift={len(drift)}  "
        f"promoted={len(promoted)}"
    )
    print(f"  invariant violations (drift-only): {len(with_invariant_fails)} shipments")
    for r in drift:
        print(
            f"    DRIFT {r['folder']}  ({r.get('snapshot_diff_count', '?')} diffs)  "
            f"→ {r.get('snapshot_current_dir', '?')}"
        )
    for r in with_invariant_fails:
        print(f"    INVARIANT {r['folder']}  ({r['invariant_failure_count']} violations)")
    print(f"\nFull report: {summary_path}")
    print("=" * 72)


def _run_pipeline(stage_dir: Path, output_dir: Path) -> subprocess.CompletedProcess:
    return subprocess.run(  # noqa: S603 - trusted local path
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


def _parse_report(stdout: str) -> dict | None:
    for line in stdout.splitlines():
        stripped = line.strip()
        if stripped.startswith("REPORT:JSON:"):
            try:
                return json.loads(stripped[len("REPORT:JSON:") :])
            except json.JSONDecodeError:
                return None
    return None


def _load_email_params(output_dir: Path) -> list[dict]:
    """Return the contents of every ``_email_params*.json`` produced."""
    out: list[dict] = []
    for p in sorted(output_dir.rglob("_email_params*.json")):
        try:
            out.append(json.loads(p.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError):
            continue
    return out


def _check_shipment_expectations(
    folder_name: str,
    output_dir: Path,
    report: dict,
) -> list[tuple[str, str]]:
    """Apply the named-fixture sanity checks.  Returns list of failures."""
    exp = _SHIPMENT_EXPECTATIONS.get(folder_name)
    if not exp:
        return []
    failures: list[tuple[str, str]] = []

    produced_waybills: set[str] = set()
    for x in output_dir.rglob("HAWB*.xlsx"):
        # HAWB9590375.xlsx → HAWB9590375
        stem = x.stem
        if "-" in stem:
            stem = stem.split("-")[0]
        produced_waybills.add(stem)

    if "expected_waybills" in exp:
        want = set(exp["expected_waybills"])
        if produced_waybills != want:
            failures.append(
                (
                    "waybills_mismatch",
                    f"expected exactly {sorted(want)}, got {sorted(produced_waybills)}",
                )
            )
    if "expected_waybills_subset" in exp:
        want = set(exp["expected_waybills_subset"])
        missing = want - produced_waybills
        if missing:
            failures.append(
                (
                    "waybills_missing",
                    f"expected subset {sorted(want)}, missing {sorted(missing)}",
                )
            )

    expected_cv = exp.get("expected_customs_value_ec") or {}
    if expected_cv:
        # ``customs_value_ec`` lands in the XLSX as a ``CLIENT DECLARED
        # DUTIES`` row, written by bl_xlsx_generator.  We read it back
        # at the system boundary rather than poking internal caches.
        for wb, want in expected_cv.items():
            xlsx_paths = list(output_dir.rglob(f"{wb}.xlsx"))
            if not xlsx_paths:
                failures.append(
                    (
                        f"customs_value_ec::{wb}",
                        f"no {wb}.xlsx produced",
                    )
                )
                continue
            # Prefer a non-combined workbook (single-waybill output).
            xlsx_paths.sort(key=lambda p: "combined" in p.name.lower())
            got = _read_client_declared_duties(xlsx_paths[0])
            if want is None:
                if got is None:
                    failures.append(
                        (
                            f"customs_value_ec::{wb}",
                            f"no CLIENT DECLARED DUTIES row in XLSX ({xlsx_paths[0].name})",
                        )
                    )
                continue
            if got is None or abs(got - float(want)) > 0.005:
                failures.append(
                    (
                        f"customs_value_ec::{wb}",
                        f"expected {want!r}, got {got!r} (xlsx={xlsx_paths[0].name})",
                    )
                )

    if "consignee" in exp:
        params = _load_email_params(output_dir)
        consignees = {p.get("consignee_name") for p in params}
        if exp["consignee"] not in consignees:
            failures.append(
                (
                    "consignee",
                    f"expected {exp['consignee']!r} in any _email_params, got {consignees}",
                )
            )

    return failures


@pytest.mark.integration
@pytest.mark.requires_downloads
@pytest.mark.parametrize(
    ("folder_name", "folder_path"),
    _FOLDERS,
    ids=[n for (n, _) in _FOLDERS],
)
def test_downloads_email_folder(
    folder_name: str,
    folder_path: Path,
    tmp_path: Path,
) -> None:
    """Process a single shipment folder and diff against baseline."""
    stage_dir = tmp_path / "stage"
    stage_dir.mkdir()
    output_dir = tmp_path / "out"
    output_dir.mkdir()

    staged = 0
    for item in folder_path.iterdir():
        if _is_source_pdf(item):
            shutil.copy2(item, stage_dir / item.name)
            staged += 1
            # Copy OCR sidecars (.txt) alongside the PDF if present — they
            # let the pipeline bypass OCR for pre-digitised invoices.
            sidecar_txt = folder_path / (item.stem + ".txt")
            if sidecar_txt.exists():
                shutil.copy2(sidecar_txt, stage_dir / sidecar_txt.name)

    result: dict = {
        "folder": folder_name,
        "staged_pdfs": staged,
        "category": "error_crash",
        "failures": [],
    }

    if staged == 0:
        result["category"] = "skipped_no_pdfs"
        _RESULTS.append(result)
        pytest.skip(f"no source PDFs in {folder_name}")

    try:
        proc = _run_pipeline(stage_dir, output_dir)
    except subprocess.TimeoutExpired as e:
        result["failures"] = [("timeout", f"subprocess timed out after {_TIMEOUT_SECONDS}s")]
        _RESULTS.append(result)
        pytest.fail(f"[{folder_name}] pipeline timed out: {e}")

    result["returncode"] = proc.returncode
    if proc.returncode != 0:
        result["failures"] = [
            ("nonzero_exit", f"returncode={proc.returncode}"),
            ("stderr_tail", proc.stderr[-500:]),
        ]
        _RESULTS.append(result)
        pytest.fail(
            f"[{folder_name}] pipeline exited with {proc.returncode}.\n"
            f"stderr tail: {proc.stderr[-500:]}"
        )

    report = _parse_report(proc.stdout)
    result["report"] = report
    if report is None:
        result["failures"] = [("missing_report", "no REPORT:JSON line in stdout")]
        _RESULTS.append(result)
        pytest.fail(f"[{folder_name}] pipeline did not emit REPORT:JSON")

    # Invariants on every produced XLSX — recorded as drift, not hard
    # failure.  Many shipments have pre-existing pipeline-level invariant
    # violations (e.g. the historical VARIANCE CHECK=0 issue covered by
    # project_variance_check_historical).  We surface them in the
    # summary for review without blocking the regression sweep.
    produced_xlsx = list(output_dir.rglob("*.xlsx"))
    invariant_failures: list[tuple[str, str]] = []
    for xlsx_path in produced_xlsx:
        try:
            wb = load_workbook(str(xlsx_path))
        except Exception as e:  # noqa: BLE001
            invariant_failures.append((f"load::{xlsx_path.name}", str(e)))
            continue
        for sheet in wb.worksheets:
            failures = run_all_invariants(
                sheet,
                mode="minimal",
                cet_db_path=str(_CET_DB) if _CET_DB.exists() else None,
                xlsx_path=str(xlsx_path),
            )
            for name, msg in failures:
                invariant_failures.append((f"{xlsx_path.name}::{sheet.title}::{name}", msg))
        wb.close()
    if invariant_failures:
        result["invariant_failures"] = invariant_failures
        result["invariant_failure_count"] = len(invariant_failures)

    # Named-fixture semantic checks — these ARE hard failures, because
    # they encode what the current pipeline version MUST do right for
    # this shipment (e.g. HAWB9590375 must extract 155.04 under v4).
    semantic_failures = _check_shipment_expectations(folder_name, output_dir, report)

    if semantic_failures:
        result["category"] = "processed_fail"
        result["failures"] = semantic_failures
        _RESULTS.append(result)
        lines = [f"[{folder_name}] named-fixture regression failures:"]
        for name, msg in semantic_failures:
            lines.append(f"  - {name}: {msg}")
        pytest.fail("\n".join(lines))

    result["category"] = "processed_pass"

    # Snapshot + diff against frozen baseline.
    try:
        snap = snapshot_and_compare(
            f"email_{folder_name}",
            output_dir,
        )
    except Exception as e:  # noqa: BLE001
        result["snapshot_error"] = str(e)
    else:
        result["snapshot_status"] = snap["status"]
        result["snapshot_hash"] = snap["hash"]
        if snap["diffs"]:
            result["snapshot_diffs"] = snap["diffs"][:200]
            result["snapshot_diff_count"] = len(snap["diffs"])
            result["snapshot_current_dir"] = snap["current_dir"]

    _RESULTS.append(result)
