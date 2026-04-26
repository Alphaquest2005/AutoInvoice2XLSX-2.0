"""Bootstrap loader for config/repo_layout.yaml.

This module is the chicken-and-egg of the SSOT-via-config pattern: every
other config loader can be reached via pipeline.config_loader, but the
path-layout config itself describes WHERE to find configs. So we bootstrap
with two unavoidable literals (the config dir name and this file's name)
and read every other path component out of the YAML.

The two literals here are gated bypasses approved in
.claude/magic-ok-approvals.yaml — they cannot grow without user review.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

# Bootstrap: we need to know the YAML's location before we can read it.
# These are the only two layout literals that exist in Python source — the
# rest live in the YAML itself.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_LAYOUT_PATH = (
    _REPO_ROOT
    / "config"            # magic-ok: bootstrap config dir for repo_layout.yaml
    / "repo_layout.yaml"  # magic-ok: bootstrap path for repo_layout itself
)


@lru_cache(maxsize=1)
def load_repo_layout() -> dict[str, Any]:
    """Return the parsed repo_layout.yaml content (minus _meta:).

    Cached for the life of the process — layout is invariant per-run.
    Tests that mutate the YAML on disk should call ``clear_cache()``.
    """
    with _LAYOUT_PATH.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if isinstance(data, dict):
        out = dict(data)
        out.pop("_meta", None)
        return out
    return {}


def clear_cache() -> None:
    load_repo_layout.cache_clear()
