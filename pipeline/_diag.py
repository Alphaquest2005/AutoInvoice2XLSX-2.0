"""Pipeline diagnostic instrumentation.

Provides a ``Stage`` context manager and a ``timed`` decorator that emit
structured START / END / FAIL records to stderr.  Used to trace slow or
hanging pipeline runs (in particular ``test_email_folder_regression``
which exceeds its 600s subprocess budget without surfacing where the
time is being spent).

Output format — one line per event, prefixed with ``[DIAG]`` so it can
be grepped from interleaved pipeline output:

    2026-04-25 23:55:01,123 [DIAG] START stage=phase-1-parse pid=12345
    2026-04-25 23:55:09,456 [DIAG] END   stage=phase-1-parse pid=12345 elapsed=8.33s
    2026-04-25 23:55:09,457 [DIAG] FAIL  stage=ocr pid=12345 elapsed=12.10s error=TimeoutError: ...

All output goes to stderr (unbuffered via flush) so it appears in real
time even when the parent process is capturing stdout.

Usage::

    from _diag import Stage

    with Stage("phase-1-parse"):
        do_work()

    with Stage("ocr", file=pdf_path):
        text = extract_pdf_text(pdf_path)

On exception the FAIL record is logged with the elapsed time and a
short traceback, then the exception is re-raised — never swallowed.
"""

from __future__ import annotations

import logging
import os
import sys
import time
import traceback
from contextlib import contextmanager

# Dedicated logger so callers can dial verbosity without touching the
# rest of the pipeline's logging config.  Default INFO + stderr handler.
logger = logging.getLogger("pipeline.diag")
if not logger.handlers:
    _h = logging.StreamHandler(sys.stderr)
    _h.setFormatter(logging.Formatter("%(asctime)s [DIAG] %(message)s"))
    logger.addHandler(_h)
    logger.setLevel(logging.INFO)
    logger.propagate = False


def _ctx_str(ctx):
    if not ctx:
        return ""
    parts = []
    for k, v in ctx.items():
        # Truncate long values (paths, dicts) so log lines stay readable
        sv = str(v)
        if len(sv) > 120:
            sv = sv[:117] + "..."
        parts.append(f"{k}={sv}")
    return " " + " ".join(parts)


class Stage:
    """Context manager that logs start, end, elapsed, and any error.

    Use as::

        with Stage("name", file=path, n=42):
            do_work()
    """

    def __init__(self, name, **ctx):
        self.name = name
        self.ctx = ctx
        self.t0 = 0.0
        self.pid = os.getpid()

    def __enter__(self):
        self.t0 = time.monotonic()
        logger.info(f"START stage={self.name} pid={self.pid}{_ctx_str(self.ctx)}")
        sys.stderr.flush()
        return self

    def __exit__(self, exc_type, exc, tb):
        elapsed = time.monotonic() - self.t0
        if exc_type is None:
            logger.info(
                f"END   stage={self.name} pid={self.pid} "
                f"elapsed={elapsed:.2f}s{_ctx_str(self.ctx)}"
            )
        else:
            err = f"{exc_type.__name__}: {exc}"
            logger.info(
                f"FAIL  stage={self.name} pid={self.pid} "
                f"elapsed={elapsed:.2f}s error={err}{_ctx_str(self.ctx)}"
            )
            for line in traceback.format_exception(exc_type, exc, tb):
                for sub in line.rstrip().split("\n"):
                    logger.info(f"  TRACE stage={self.name} {sub}")
        sys.stderr.flush()
        return False  # never suppress

    def mark(self, label, **extra):
        """Log an intermediate checkpoint inside an open Stage.

        Useful for breaking long stages into sub-steps without nesting
        another Stage block.
        """
        elapsed = time.monotonic() - self.t0
        merged = dict(self.ctx)
        merged.update(extra)
        logger.info(
            f"MARK  stage={self.name} step={label} pid={self.pid} "
            f"elapsed={elapsed:.2f}s{_ctx_str(merged)}"
        )
        sys.stderr.flush()


@contextmanager
def stage(name, **ctx):
    """Functional alias for ``Stage`` — ``with stage('foo'): ...``."""
    s = Stage(name, **ctx)
    with s:
        yield s


def timed(name=None):
    """Decorator: wrap a function in ``Stage(name)`` automatically.

    ``@timed()`` uses the function's qualified name; ``@timed("foo")``
    overrides.  Captures the first positional path-like arg (if any) as
    ``file=...`` context for convenience.
    """

    def deco(func):
        stage_name = name or func.__qualname__

        def wrapper(*args, **kwargs):
            ctx = {}
            if args:
                first = args[0]
                if isinstance(first, (str, bytes, os.PathLike)):
                    sv = os.fspath(first)
                    ctx["file"] = os.path.basename(sv) if os.sep in sv else sv
            with Stage(stage_name, **ctx):
                return func(*args, **kwargs)

        wrapper.__wrapped__ = func
        wrapper.__name__ = func.__name__
        wrapper.__qualname__ = func.__qualname__
        wrapper.__doc__ = func.__doc__
        return wrapper

    return deco
