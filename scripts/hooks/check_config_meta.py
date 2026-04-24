#!/usr/bin/env python3
"""Claude Code PreToolUse hook: enforce '_meta:' on every config file.

Triggers on Write/Edit/MultiEdit of any config/*.yaml, config/*.yml, or
config/*.json. Reconstructs the post-edit file content in memory, parses
it with config_registry.load_meta() + has_valid_meta(), and blocks if the
_meta: block is missing or incomplete.

Required _meta: fields:
    purpose         (non-empty string)
    covers          (non-empty list of strings)
    match_patterns  (non-empty list of strings)
Optional:
    loader, examples, update_checklist

Exit is always 0; the decision is conveyed via JSON on stdout.
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

_HOOKS_DIR = Path(__file__).resolve().parent
if str(_HOOKS_DIR) not in sys.path:
    sys.path.insert(0, str(_HOOKS_DIR))

import config_registry  # noqa: E402

SUPPORTED_TOOLS = {"Write", "Edit", "MultiEdit"}
CONFIG_SUFFIXES = {".yaml", ".yml", ".json"}


def _emit(decision):
    sys.stdout.write(json.dumps(decision))
    sys.stdout.flush()
    sys.exit(0)


def _allow():
    _emit({})


def _block(reason):
    _emit({"decision": "block", "reason": reason})


def _is_config_file(path_str: str) -> bool:
    if not path_str:
        return False
    p = Path(path_str)
    if p.suffix not in CONFIG_SUFFIXES:
        return False
    # Must be inside the repo's config/ directory.
    norm = str(p).replace("\\", "/")
    return "/config/" in norm or norm.startswith("config/")


def _read_current(path_str: str) -> str:
    try:
        return Path(path_str).read_text(encoding="utf-8")
    except (FileNotFoundError, OSError):
        return ""


def _apply_edit(source, old_string, new_string, replace_all=False):
    if replace_all:
        return source.replace(old_string, new_string)
    idx = source.find(old_string)
    if idx < 0:
        return source
    return source[:idx] + new_string + source[idx + len(old_string):]


def _reconstruct(tool_name, tool_input):
    file_path = tool_input.get("file_path")
    if not file_path or not _is_config_file(file_path):
        return None, None

    if tool_name == "Write":
        return file_path, tool_input.get("content", "") or ""

    if tool_name == "Edit":
        current = _read_current(file_path)
        return file_path, _apply_edit(
            current,
            tool_input.get("old_string", "") or "",
            tool_input.get("new_string", "") or "",
            bool(tool_input.get("replace_all")),
        )

    if tool_name == "MultiEdit":
        current = _read_current(file_path)
        for edit in tool_input.get("edits") or []:
            current = _apply_edit(
                current,
                edit.get("old_string", "") or "",
                edit.get("new_string", "") or "",
                bool(edit.get("replace_all")),
            )
        return file_path, current

    return None, None


def _load_meta_from_text(file_path: str, text: str):
    """config_registry.load_meta() reads from disk. We need to parse
    arbitrary text for the proposed post-edit content, so write to a temp
    file with the same suffix and delegate."""
    suffix = Path(file_path).suffix
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=suffix, delete=False, encoding="utf-8"
    ) as fh:
        fh.write(text)
        tmp_path = Path(fh.name)
    try:
        return config_registry.load_meta(tmp_path)
    finally:
        try:
            tmp_path.unlink()
        except OSError:
            pass


def _guidance(file_path: str, missing: list[str]) -> str:
    lines = [
        f"BLOCKED: config file '{file_path}' is missing a valid '_meta:' block.",
        "",
        f"Missing or empty required fields: {', '.join(missing)}",
        "",
        "Every config file under config/ MUST begin with a _meta: block so",
        "the magic-constant hook can route future literals to the right",
        "file. Minimum template:",
        "",
        "  _meta:",
        "    purpose: \"One-line description of what this config governs\"",
        "    covers:",
        "      - \"What kinds of values live here\"",
        "      - \"Which modules read them\"",
        "    match_patterns:",
        "      # Tokens the LLM will search for when placing a new literal",
        "      - \"document_type\"",
        "      - \"doc_type\"",
        "    loader: \"pipeline.config_loader:load_<name>\"  # optional",
        "    examples:                                         # optional",
        "      - \"resolve_doc_type('Budget Marine') -> '7400-000'\"",
        "    update_checklist:                                 # optional",
        "      - \"Add the key here, then run <related test>\"",
        "",
        "Add that block at the top of the file and try the edit again.",
    ]
    return "\n".join(lines)


def main():
    try:
        raw = sys.stdin.read()
        if not raw.strip():
            _allow()
        payload = json.loads(raw)
    except json.JSONDecodeError:
        _allow()
        return

    tool_name = payload.get("tool_name") or ""
    if tool_name not in SUPPORTED_TOOLS:
        _allow()
        return

    tool_input = payload.get("tool_input") or {}
    file_path, post_source = _reconstruct(tool_name, tool_input)
    if file_path is None or post_source is None:
        _allow()
        return

    meta = _load_meta_from_text(file_path, post_source)
    valid, missing = config_registry.has_valid_meta(meta)
    if valid:
        _allow()
        return

    _block(_guidance(file_path, missing))


if __name__ == "__main__":
    main()
