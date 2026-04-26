"""Lightweight phase-timing perf log written as JSONL alongside pipeline output.

Used to diagnose which phases dominate per-folder runtime when a regression
folder times out. The instrument is OFF by default — call ``init(path)`` once
near the top of ``main()`` to enable it (typically inside ``args.output_dir``).
While disabled, ``phase(...)`` and ``event(...)`` are cheap no-ops.

Each line in the JSONL output is one of:

* ``{"event": "init", "pid": ..., "t": ...}``                           — first line
* ``{"event": "start", "name": ..., "t": ..., **meta}``                 — phase opened
* ``{"event": "end", "name": ..., "dur_s": ..., "t": ..., **meta}``     — phase closed
* ``{"event": "event", "name": ..., "dur_s": ..., "t": ..., **meta}``   — point-in-time
* ``{"event": "close", "t": ...}``                                      — on shutdown

Meta keys are free-form (``pdf``, ``pages``, ``quality``, ``cache_hit`` etc.).
Read the trace with ``jq -r '... | select(.event=="end") ...'`` or any tool.
"""
from __future__ import annotations

import json
import os
import threading
import time
from contextlib import contextmanager
from typing import Optional

_lock = threading.Lock()
_handle = None  # type: Optional[object]


def init(path: str) -> None:
    """Open ``path`` for append-mode JSONL writes. Idempotent — second call is a no-op."""
    global _handle
    if _handle is not None:
        return
    try:
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        _handle = open(path, "a", encoding="utf-8", buffering=1)
        _write({"event": "init", "pid": os.getpid(), "t": time.time()})
    except Exception:
        # Instrumentation must never crash the pipeline.
        _handle = None


def enabled() -> bool:
    return _handle is not None


def _write(rec: dict) -> None:
    if _handle is None:
        return
    try:
        with _lock:
            _handle.write(json.dumps(rec, default=str) + "\n")
    except Exception:
        pass


def event(name: str, dur_s: float = 0.0, **meta) -> None:
    """Record a point-in-time event (no enclosing phase)."""
    if _handle is None:
        return
    rec = {"event": "event", "name": name, "dur_s": round(float(dur_s), 3), "t": time.time()}
    rec.update(meta)
    _write(rec)


@contextmanager
def phase(name: str, **meta):
    """Context manager that records start + end + duration for a phase."""
    if _handle is None:
        yield
        return
    start = time.monotonic()
    start_rec = {"event": "start", "name": name, "t": time.time()}
    start_rec.update(meta)
    _write(start_rec)
    err = None
    try:
        yield
    except BaseException as e:  # noqa: BLE001 - we re-raise after logging
        err = type(e).__name__
        raise
    finally:
        dur = time.monotonic() - start
        end_rec = {"event": "end", "name": name, "dur_s": round(dur, 3), "t": time.time()}
        end_rec.update(meta)
        if err is not None:
            end_rec["error"] = err
        _write(end_rec)


def close() -> None:
    global _handle
    if _handle is None:
        return
    try:
        _write({"event": "close", "t": time.time()})
        _handle.close()
    finally:
        _handle = None
