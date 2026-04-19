"""Rerun every shipment under workspace/output/downloads-regression-emails/.

Orchestration:
    1. Back up the existing corpus to downloads-regression-emails.backup-<ts>/
       (non-destructive — preserves the old XLSX/email_params for diffing).
    2. For each top-level folder (one "email" / "shipment" each):
       a. Copy only the PDFs into a fresh tmp stage dir.
       b. Invoke pipeline/run.py --input-dir <stage> --output-dir <out>.
       c. Capture stdout (REPORT:JSON line) + return code.
       d. Move the new outputs back next to the source PDFs in-place.
    3. Emit a per-folder result row + session summary to a JSON report.

Safety:
    * Backup is taken before any deletion.
    * On any unhandled error mid-folder the tmp stage is torn down but the
      backup remains intact; rerun can resume by passing --skip-existing.

Usage:
    .venv/bin/python scripts/rerun_corpus.py [--folder NAME] [--limit N] \
        [--skip-existing] [--report PATH]
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CORPUS = REPO_ROOT / "workspace" / "output" / "downloads-regression-emails"
RUN_PY = REPO_ROOT / "pipeline" / "run.py"
PYTHON = REPO_ROOT / ".venv" / "bin" / "python"

# Generated/output files to exclude when staging a folder's PDFs.
GENERATED_SUFFIXES = {
    ".xlsx",
    ".meta.json",
    ".pages.json",
    ".pdf.pages.json",
}
GENERATED_PREFIXES = ("_email_params",)


def is_source_file(path: Path) -> bool:
    """Return True for files the pipeline should re-process from."""
    name = path.name
    # Exclude generated files.
    if name.startswith(GENERATED_PREFIXES):
        return False
    suffix = "".join(path.suffixes)
    if suffix in GENERATED_SUFFIXES:
        return False
    # Keep .pdf (and any .PDF).
    return path.suffix.lower() == ".pdf"


def stage_folder(src: Path, stage: Path) -> int:
    """Copy only source PDFs from src into stage. Returns number staged."""
    stage.mkdir(parents=True, exist_ok=True)
    n = 0
    for item in src.iterdir():
        if item.is_file() and is_source_file(item):
            shutil.copy2(item, stage / item.name)
            n += 1
            # Also copy OCR sidecars (.txt, .pdf.pages.json) if they exist
            for sidecar_suffix in (".txt", ".pdf.pages.json"):
                if sidecar_suffix == ".txt":
                    sidecar = src / (item.stem + sidecar_suffix)
                else:
                    sidecar = src / (item.name + ".pages.json")
                if sidecar.exists():
                    shutil.copy2(sidecar, stage / sidecar.name)
    return n


def is_ocr_sidecar(path: Path) -> bool:
    """Return True for OCR sidecar files paired with a source PDF."""
    name = path.name
    # .txt sidecar: foo.txt → paired with foo.pdf
    if path.suffix == ".txt":
        pdf = path.with_suffix(".pdf")
        return pdf.exists() and is_source_file(pdf)
    # .pages.json sidecar: foo.pdf.pages.json → paired with foo.pdf
    if name.endswith(".pdf.pages.json"):
        pdf = path.parent / name[: -len(".pages.json")]
        return pdf.exists() and is_source_file(pdf)
    return False


def clear_generated(dst: Path) -> int:
    """Remove generated files in-place from dst. Returns count removed."""
    n = 0
    for item in dst.iterdir():
        if item.is_file() and not is_source_file(item) and not is_ocr_sidecar(item):
            item.unlink()
            n += 1
        elif item.is_dir() and item.name == "_split_temp":
            shutil.rmtree(item)
            n += 1
    return n


def run_pipeline(stage: Path, out: Path, timeout: int) -> tuple[int, str, str]:
    """Invoke pipeline/run.py on stage → out. Returns (rc, stdout, stderr)."""
    proc = subprocess.run(
        [
            str(PYTHON),
            str(RUN_PY),
            "--input-dir",
            str(stage),
            "--output-dir",
            str(out),
            "--json-output",
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=str(REPO_ROOT),
        check=False,
    )
    return proc.returncode, proc.stdout, proc.stderr


def parse_report(stdout: str) -> dict | None:
    """Extract REPORT:JSON:{...} line from pipeline stdout."""
    for line in stdout.splitlines():
        if line.startswith("REPORT:JSON:"):
            try:
                return json.loads(line[len("REPORT:JSON:") :])
            except json.JSONDecodeError:
                return None
    return None


def backup_corpus() -> Path:
    """Make a dated sibling backup of the corpus directory."""
    ts = datetime.now(tz=UTC).strftime("%Y%m%dT%H%M%SZ")
    backup = CORPUS.parent / f"{CORPUS.name}.backup-{ts}"
    if backup.exists():
        raise RuntimeError(f"backup already exists: {backup}")
    print(
        f"Backing up {CORPUS.relative_to(REPO_ROOT)} → {backup.relative_to(REPO_ROOT)}", flush=True
    )
    shutil.copytree(CORPUS, backup)
    return backup


def process_folder(folder: Path, timeout: int, tmp_root: Path) -> dict:
    """Rerun one shipment folder. Return a result dict."""
    t0 = time.time()
    stage = tmp_root / f"{folder.name}.stage"
    out = tmp_root / f"{folder.name}.out"
    staged = 0
    rc = -1
    report: dict | None = None
    err: str | None = None
    try:
        staged = stage_folder(folder, stage)
        if staged == 0:
            return {
                "folder": folder.name,
                "staged": 0,
                "status": "skipped_no_pdfs",
                "elapsed_s": round(time.time() - t0, 1),
            }
        out.mkdir(parents=True, exist_ok=True)
        rc, stdout, stderr = run_pipeline(stage, out, timeout)
        report = parse_report(stdout)
        if rc != 0:
            err = (stderr or "")[-500:]
        # On success, replace generated files in the original folder.
        if rc == 0:
            clear_generated(folder)
            for item in out.rglob("*"):
                if item.is_file():
                    rel = item.relative_to(out)
                    dst = folder / rel
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(item, dst)
    except subprocess.TimeoutExpired:
        err = f"timeout after {timeout}s"
        rc = -2
    except Exception as e:
        err = f"{type(e).__name__}: {e}"
    finally:
        # Tear down tmp regardless.
        shutil.rmtree(stage, ignore_errors=True)
        shutil.rmtree(out, ignore_errors=True)

    return {
        "folder": folder.name,
        "staged": staged,
        "rc": rc,
        "status": "ok" if rc == 0 else "failed",
        "report": report,
        "error": err,
        "elapsed_s": round(time.time() - t0, 1),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--folder", help="Process only the named folder")
    ap.add_argument("--limit", type=int, help="Process at most N folders")
    ap.add_argument(
        "--skip-existing", action="store_true", help="Skip folders whose first XLSX already exists"
    )
    ap.add_argument(
        "--no-backup", action="store_true", help="Skip the corpus backup step (dangerous)"
    )
    ap.add_argument(
        "--timeout", type=int, default=600, help="Per-folder pipeline timeout (seconds)"
    )
    ap.add_argument("--report", type=Path, default=REPO_ROOT / "workspace" / "rerun_report.json")
    ap.add_argument("--tmp-root", type=Path, default=REPO_ROOT / "workspace" / "_rerun_tmp")
    args = ap.parse_args()

    # Resolve relative paths so relative_to(REPO_ROOT) works later.
    args.report = args.report.resolve()
    args.tmp_root = args.tmp_root.resolve()

    if not CORPUS.is_dir():
        print(f"corpus not found: {CORPUS}", file=sys.stderr)
        return 2
    if not RUN_PY.is_file():
        print(f"pipeline/run.py not found: {RUN_PY}", file=sys.stderr)
        return 2
    if not PYTHON.is_file():
        print(f".venv python not found: {PYTHON}", file=sys.stderr)
        return 2

    folders = sorted(p for p in CORPUS.iterdir() if p.is_dir())
    if args.folder:
        folders = [p for p in folders if p.name == args.folder]
    if args.limit:
        folders = folders[: args.limit]
    if not folders:
        print("no folders to process", file=sys.stderr)
        return 2

    # Backup only if processing >1 folder (smoke tests don't need it).
    if not args.no_backup and len(folders) > 1:
        backup_corpus()

    args.tmp_root.mkdir(parents=True, exist_ok=True)
    args.report.parent.mkdir(parents=True, exist_ok=True)

    results: list[dict] = []
    t0 = time.time()
    for i, folder in enumerate(folders, 1):
        print(f"[{i}/{len(folders)}] {folder.name}", flush=True)
        if args.skip_existing and any(folder.glob("*.xlsx")):
            print("    skip (existing xlsx present)", flush=True)
            results.append({"folder": folder.name, "status": "skipped_existing"})
            continue
        result = process_folder(folder, args.timeout, args.tmp_root)
        results.append(result)
        print(f"    {result['status']}  ({result['elapsed_s']}s)", flush=True)
        # Persist incremental so partial runs remain useful.
        args.report.write_text(
            json.dumps(
                {
                    "session_start": datetime.now(tz=UTC).isoformat(),
                    "total_folders": len(folders),
                    "processed": i,
                    "elapsed_s": round(time.time() - t0, 1),
                    "results": results,
                },
                indent=2,
            )
        )

    # Session summary.
    by_status: dict[str, int] = {}
    for r in results:
        by_status[r["status"]] = by_status.get(r["status"], 0) + 1
    print("\nSession summary:")
    for st, n in sorted(by_status.items()):
        print(f"  {st:20s} {n}")
    print(f"  total elapsed: {round(time.time() - t0, 1)}s")
    print(f"  report: {args.report.relative_to(REPO_ROOT)}")

    shutil.rmtree(args.tmp_root, ignore_errors=True)
    return 0 if by_status.get("ok", 0) == len(folders) else 1


if __name__ == "__main__":
    raise SystemExit(main())
