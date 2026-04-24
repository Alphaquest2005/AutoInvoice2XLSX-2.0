#!/usr/bin/env python3
"""Claude Code PreToolUse hook: gate every '# magic-ok:' bypass through
an explicit user-approval registry.

Flow:
    1. Parse PreToolUse JSON from stdin (Write | Edit | MultiEdit).
    2. Only handle Python files; policy-exempt scripts/hooks/*.py.
    3. Reconstruct the post-edit source.
    4. Extract every '# magic-ok: <reason>' line, pair it with the literal
       on that line (first ast.Constant at that line number).
    5. Compute fingerprint = sha256(f'{rel_path}|{literal_repr}|{reason}').
    6. Load .claude/magic-ok-approvals.yaml. Every required fingerprint
       MUST be registered there. If not: BLOCK with a copy-paste-ready
       YAML entry and instructions to add it.

The LLM cannot self-approve: updating the registry is itself a Write to a
config-like file, and the user always reviews new bypasses. The registry
file is owned by the user.
"""

from __future__ import annotations

import ast
import hashlib
import json
import re
import sys
from pathlib import Path

_HOOKS_DIR = Path(__file__).resolve().parent
if str(_HOOKS_DIR) not in sys.path:
    sys.path.insert(0, str(_HOOKS_DIR))

import check_magic_constants as detector  # noqa: E402

REPO_ROOT = _HOOKS_DIR.parent.parent
REGISTRY_PATH = REPO_ROOT / ".claude" / "magic-ok-approvals.yaml"
SUPPORTED_TOOLS = {"Write", "Edit", "MultiEdit"}
POLICY_EXEMPT_PREFIXES = ("scripts/hooks/",)
MAGIC_OK_RE = re.compile(r"#\s*magic-ok:\s*(\S.*?)\s*$")


def _emit(decision):
    sys.stdout.write(json.dumps(decision))
    sys.stdout.flush()
    sys.exit(0)


def _allow():
    _emit({})


def _block(reason):
    _emit({"decision": "block", "reason": reason})


def _is_python_file(path_str):
    return bool(path_str) and path_str.endswith(".py")


def _is_policy_exempt(path_str):
    if not path_str:
        return False
    norm = path_str.replace("\\", "/")
    return any(f"/{p}" in norm or norm.startswith(p)
               for p in POLICY_EXEMPT_PREFIXES)


def _read_current(path_str):
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
    if not file_path or not _is_python_file(file_path):
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
    return None, None


def _rel_path(file_path):
    p = Path(file_path)
    try:
        return str(p.resolve().relative_to(REPO_ROOT))
    except (ValueError, OSError):
        return str(p)


def _fingerprint(rel_path, literal_repr, reason):
    raw = f"{rel_path}|{literal_repr}|{reason}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _extract_pending_bypasses(file_path, source):
    """Pair each '# magic-ok: <reason>' line with the first ast.Constant
    literal on the same line. Returns a list of:
        {line, literal_repr, reason, fingerprint}
    """
    pending = []
    try:
        tree = ast.parse(source, filename=file_path)
    except SyntaxError:
        # We cannot reason about a file that doesn't parse; let the
        # detector hook surface that separately. Allow this hook through.
        return pending

    # Map line -> first Constant node on that line.
    constants_by_line = {}
    doc_ids = detector.collect_docstring_node_ids(tree)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant):
            continue
        if id(node) in doc_ids:
            continue
        if node.value is Ellipsis:
            continue
        if detector.is_exempt(node.value):
            continue
        line = getattr(node, "lineno", 0) or 0
        if line and line not in constants_by_line:
            constants_by_line[line] = node

    rel = _rel_path(file_path)
    for idx, line in enumerate(source.splitlines(), start=1):
        m = MAGIC_OK_RE.search(line)
        if not m:
            continue
        reason = m.group(1).strip()
        if not reason:
            # No reason => not a valid bypass; detector will flag the
            # underlying literal.
            continue
        node = constants_by_line.get(idx)
        if node is None:
            # No non-exempt literal on this line. Either user put the
            # comment on a line without a literal, or the literal is
            # hard-exempt. Either way, no approval needed.
            continue
        literal_repr = detector._truncate(node.value)
        pending.append({
            "line": idx,
            "literal_repr": literal_repr,
            "reason": reason,
            "fingerprint": _fingerprint(rel, literal_repr, reason),
            "rel_path": rel,
        })
    return pending


def _load_registry():
    """Parse .claude/magic-ok-approvals.yaml tolerantly.
    Returns the set of approved fingerprints."""
    if not REGISTRY_PATH.is_file():
        return set()
    try:
        text = REGISTRY_PATH.read_text(encoding="utf-8")
    except OSError:
        return set()

    fps = set()
    # Try PyYAML first; fall back to a regex sweep.
    try:
        import yaml  # type: ignore
        data = yaml.safe_load(text)
        if isinstance(data, dict):
            approvals = data.get("approvals") or []
            if isinstance(approvals, list):
                for item in approvals:
                    if isinstance(item, dict):
                        fp = item.get("fingerprint")
                        if isinstance(fp, str) and fp.strip():
                            fps.add(fp.strip())
        return fps
    except ImportError:
        pass
    # Fallback: grab anything that looks like `fingerprint: <hex>`.
    for m in re.finditer(r"fingerprint\s*:\s*['\"]?([0-9a-fA-F]{16,})['\"]?",
                         text):
        fps.add(m.group(1))
    return fps


def _guidance(file_path, unapproved):
    lines = [
        f"BLOCKED: new '# magic-ok:' bypass(es) in {_rel_path(file_path)}",
        "require explicit user approval before they can be added.",
        "",
        "A bypass lets a literal stay in code instead of moving to config.",
        "You (the LLM) CANNOT self-approve. The user must review each",
        "bypass and add it to .claude/magic-ok-approvals.yaml.",
        "",
        "Unapproved bypasses detected:",
        "",
    ]
    for b in unapproved:
        lines.append(f"  - L{b['line']}: {b['literal_repr']}")
        lines.append(f"      reason: {b['reason']}")
        lines.append(f"      fingerprint: {b['fingerprint']}")
        lines.append("")
    lines.extend([
        "TO PROCEED: ask the user to append the following entries to",
        ".claude/magic-ok-approvals.yaml (creating the file if needed with",
        "top-level key 'approvals:'):",
        "",
        "approvals:",
    ])
    for b in unapproved:
        lines.extend([
            f"  - fingerprint: \"{b['fingerprint']}\"",
            f"    path: \"{b['rel_path']}\"",
            f"    line: {b['line']}",
            f"    literal: {b['literal_repr']}",
            f"    reason: \"{b['reason']}\"",
        ])
    lines.extend([
        "",
        "Then retry the edit. If the user refuses a bypass, move the",
        "literal to a config file under config/ instead.",
    ])
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

    pending = _extract_pending_bypasses(file_path, post_source)
    if not pending:
        _allow()
        return

    approved = _load_registry()
    unapproved = [b for b in pending if b["fingerprint"] not in approved]
    if not unapproved:
        _allow()
        return

    _block(_guidance(file_path, unapproved))


if __name__ == "__main__":
    main()
