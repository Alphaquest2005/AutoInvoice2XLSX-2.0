#!/usr/bin/env python3
"""Claude Code PreToolUse adapter for the magic-constant detector.

Reads PreToolUse JSON from stdin (tool_name + tool_input with file_path +
content/new_string/new_source/edits), reconstructs the post-edit file
contents in memory, runs check_magic_constants.scan() on it, and emits a
decision JSON to stdout.

Decisions:
    - No violations            -> {} (empty — Claude Code proceeds normally)
    - Unbypassed violations    -> {"decision": "block", "reason": "<guidance>"}
    - Bypassed (# magic-ok:) violations still flow through the separate
      check_magic_ok_registry hook for approval.

Scope:
    - Only Python files (*.py). Everything else is allowed through.
    - scripts/hooks/*.py are policy-exempt (hooks cannot police themselves).
    - Tools handled: Write, Edit, MultiEdit, NotebookEdit.
    - Unknown tool_names pass through.

Exit codes are always 0; Claude Code reads the JSON decision, not the exit.
"""

import json
import sys
from pathlib import Path

# Import detector from sibling module
_HOOKS_DIR = Path(__file__).resolve().parent
if str(_HOOKS_DIR) not in sys.path:
    sys.path.insert(0, str(_HOOKS_DIR))

import check_magic_constants as detector  # noqa: E402

SUPPORTED_TOOLS = {"Write", "Edit", "MultiEdit", "NotebookEdit"}
POLICY_EXEMPT_PREFIXES = ("scripts/hooks/",)


def _emit(decision):
    """Write decision JSON to stdout and exit 0."""
    sys.stdout.write(json.dumps(decision))
    sys.stdout.flush()
    sys.exit(0)


def _allow():
    _emit({})


def _block(reason):
    _emit({"decision": "block", "reason": reason})


def _is_python_file(path_str):
    if not path_str:
        return False
    return path_str.endswith(".py")


def _is_policy_exempt(path_str):
    """scripts/hooks/*.py carve-out — the hooks ARE the policy."""
    if not path_str:
        return False
    norm = path_str.replace("\\", "/")
    return any(f"/{p}" in norm or norm.startswith(p)
               for p in POLICY_EXEMPT_PREFIXES)


def _read_current(path_str):
    try:
        return Path(path_str).read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""
    except Exception:
        return ""


def _apply_edit(source, old_string, new_string, replace_all=False):
    """Apply a single Edit substitution. Mirrors Claude Code semantics:
    old_string must occur exactly once unless replace_all=True."""
    if replace_all:
        return source.replace(old_string, new_string)
    # Non-replace_all: substitute the first occurrence. If the edit is
    # invalid (no occurrence), we still want to scan the unchanged source.
    idx = source.find(old_string)
    if idx < 0:
        return source
    return source[:idx] + new_string + source[idx + len(old_string):]


def _reconstruct(tool_name, tool_input):
    """Return (filename, post_edit_source) for scanning, or (None, None)
    if this call is not something we can or should analyze."""
    file_path = tool_input.get("file_path") or tool_input.get("notebook_path")
    if not file_path:
        return None, None
    if not _is_python_file(file_path):
        return None, None
    if _is_policy_exempt(file_path):
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

    if tool_name == "NotebookEdit":
        # Notebook cells are Python fragments; scanning the single cell
        # source is sufficient for magic-constant detection.
        return file_path, tool_input.get("new_source", "") or ""

    return None, None


def _config_inventory():
    """Return a short inventory string of live config files for guidance.
    Reads config/*.yaml|*.json filenames so the message is always current."""
    cfg_dir = _HOOKS_DIR.parent.parent / "config"
    try:
        files = sorted(
            p.name for p in cfg_dir.iterdir()
            if p.is_file() and p.suffix in {".yaml", ".yml", ".json"}
        )
    except (OSError, FileNotFoundError):
        return "(config/ not readable)"
    if not files:
        return "(no config files found)"
    return ", ".join(files)


def _format_violation(v):
    line = v.get("line", 0)
    kind = v.get("kind", "?")
    val = v.get("value_repr", "?")
    ctx = v.get("context", "")
    return f"  L{line} [{kind}] {val}\n    > {ctx}"


def _guidance_message(filename, violations):
    lines = [
        f"BLOCKED: magic-constant policy violation in {filename}",
        "",
        "The following literal values were introduced without a config",
        "source or an approved bypass:",
        "",
    ]
    for v in violations:
        lines.append(_format_violation(v))
    lines.extend([
        "",
        "HOW TO FIX (pick ONE):",
        "",
        "  1. MOVE THE VALUE TO CONFIG (preferred).",
        "     Current config files in config/:",
        f"       {_config_inventory()}",
        "     Each config file has a `_meta:` block describing what it",
        "     covers, its match_patterns, and how to load it. Read the",
        "     closest match and add your key there. If no file fits,",
        "     create a new config/<topic>.yaml with a `_meta:` block.",
        "     Then read the value via pipeline/config_loader.py — never",
        "     hardcode it in Python.",
        "",
        "  2. IF THE VALUE IS NOT POLICY (e.g. a regex pattern internal",
        "     to one function, an openpyxl default, an enum string the",
        "     library itself defines), request a per-line bypass by",
        "     appending '# magic-ok: <reason>' to the line. Bypasses",
        "     require EXPLICIT USER APPROVAL through the magic-ok",
        "     registry hook — you cannot self-approve.",
        "",
        "  3. IF THE VALUE IS A HARD-EXEMPT PYTHON TOKEN (0/1/-1/2/-2,",
        "     True/False/None, empty string, encoding name, file mode,",
        "     dunder name), the detector already allows it — check that",
        "     your literal is one of those.",
        "",
        "Do NOT attempt to silence this hook by moving the literal",
        "somewhere else in the same file. The detector walks the full AST.",
    ])
    return "\n".join(lines)


def main():
    try:
        raw = sys.stdin.read()
        if not raw.strip():
            _allow()
        payload = json.loads(raw)
    except json.JSONDecodeError:
        # Malformed input — don't block the user; fail open.
        _allow()
        return

    tool_name = payload.get("tool_name") or ""
    if tool_name not in SUPPORTED_TOOLS:
        _allow()
        return

    tool_input = payload.get("tool_input") or {}
    filename, post_source = _reconstruct(tool_name, tool_input)
    if filename is None or post_source is None:
        _allow()
        return

    violations = detector.scan(post_source, filename=filename)
    # Bypassed violations flow through the registry hook, not this one.
    open_violations = [v for v in violations if not v.get("has_bypass")]
    if not open_violations:
        _allow()
        return

    _block(_guidance_message(filename, open_violations))


if __name__ == "__main__":
    main()
