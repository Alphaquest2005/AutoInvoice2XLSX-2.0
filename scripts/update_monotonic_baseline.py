#!/usr/bin/env python3
"""Ratchet the strangler-fig monotonic baseline in the right direction.

Refuses to update any field in the wrong direction — i.e. you can't use this
to make pipeline/ bigger or src/autoinvoice/ smaller. That's the whole point.

Usage:
    python scripts/update_monotonic_baseline.py "reason for the change"
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
_BASELINE = _REPO_ROOT / "tests" / "integration" / "_monotonic_baseline.json"


def _count_py_files(root: Path) -> int:
    return sum(1 for p in root.rglob("*.py") if "__pycache__" not in p.parts)


def _loc(path: Path) -> int:
    return sum(1 for _ in path.open(encoding="utf-8", errors="ignore"))


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("usage: update_monotonic_baseline.py 'reason for the update'", file=sys.stderr)
        return 2
    reason = argv[1]
    current = {
        "pipeline_run_py_loc": _loc(_REPO_ROOT / "pipeline" / "run.py"),
        "pipeline_py_file_count": _count_py_files(_REPO_ROOT / "pipeline"),
        "src_autoinvoice_py_file_count": _count_py_files(_REPO_ROOT / "src" / "autoinvoice"),
    }
    baseline = json.loads(_BASELINE.read_text(encoding="utf-8"))

    errors: list[str] = []
    # pipeline_* numbers may only go DOWN (or stay equal).
    for k in ("pipeline_run_py_loc", "pipeline_py_file_count"):
        if current[k] > baseline[k]:
            errors.append(
                f"{k} grew: baseline={baseline[k]} current={current[k]}. "
                "This script ratchets, it does not regress."
            )
    # src_* may only go UP (or stay equal).
    for k in ("src_autoinvoice_py_file_count",):
        if current[k] < baseline[k]:
            errors.append(
                f"{k} shrank: baseline={baseline[k]} current={current[k]}. "
                "This script ratchets, it does not regress."
            )

    if errors:
        print("Refusing to update baseline:\n  " + "\n  ".join(errors), file=sys.stderr)
        return 1

    updated = {
        **baseline,
        **current,
        "updated_at": date.today().isoformat(),
        "updated_reason": reason,
    }
    _BASELINE.write_text(json.dumps(updated, indent=2) + "\n", encoding="utf-8")
    print(f"Baseline updated: {current}  ({reason})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
