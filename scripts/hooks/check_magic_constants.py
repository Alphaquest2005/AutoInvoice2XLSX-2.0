#!/usr/bin/env python3
"""Magic-constant detector.

Walks every ast.Constant node in a Python source file. Reports any literal
that is not in the hard-exemption set AND does not have a '# magic-ok: <reason>'
comment on the same line.

Exemptions (Python syntax, not policy):
    - Numeric: 0, 1, -1, 2, -2
    - Booleans: True, False, None
    - Empty / single-whitespace strings
    - Encoding names: utf-8, ascii, latin-1, etc.
    - File-mode strings for open(): r, w, rb, wt, etc.
    - Dunder identifiers: __main__, __name__, __init__, etc.
    - Docstrings: first Expr statement of module/class/function body
    - Ellipsis (...)
    - Empty bytes b''

NOTE: scripts/hooks/*.py are policy-exempt at the settings.json matcher
level — the hook scripts ARE the policy and cannot follow themselves.

CLI:
    python3 check_magic_constants.py --file <path>
    cat <file> | python3 check_magic_constants.py --stdin --filename <path>
    python3 check_magic_constants.py --dry-run --path <dir>   # recursive
"""

import argparse
import ast
import json
import os
import re
import sys
from pathlib import Path

# Hard exemptions — Python syntax, not policy.
EXEMPT_NUMBERS = {0, 1, -1, 2, -2}
EXEMPT_STRINGS = frozenset({
    "", " ", "\n", "\t", "\r",
    # Encoding names (PEP 3120 / stdlib canon)
    "utf-8", "utf-16", "utf-32", "ascii", "latin-1", "iso-8859-1", "cp1252",
    # File-mode strings (open() contract)
    "r", "w", "a", "rb", "wb", "ab", "rt", "wt", "at",
    "r+", "w+", "a+", "rb+", "wb+", "ab+", "x", "xb", "xt",
})
DUNDER_RE = re.compile(r"^__[a-zA-Z_]+__$")
MAGIC_OK_RE = re.compile(r"#\s*magic-ok:\s*(\S.*?)\s*$")

# Role-based exemption tables (Path 1).
# These cover common non-policy roles where a literal is shape/format/
# template rather than a policy value. Full rationale in scan()'s
# _is_role_exempt() helper.
LOG_METHOD_NAMES = frozenset({
    "debug", "info", "warning", "warn", "error", "critical",
    "exception", "log", "fatal", "trace",
})
DICT_LOOKUP_METHODS = frozenset({"get", "pop", "setdefault"})
EXCEPTION_NAME_SUFFIXES = ("Error", "Exception", "Warning")


def is_exempt(value):
    """Return True if the literal is a hard-exempt Python syntax token."""
    if value is None:
        return True
    if isinstance(value, bool):
        return True
    if isinstance(value, (int, float)):
        return value in EXEMPT_NUMBERS
    if isinstance(value, str):
        if value in EXEMPT_STRINGS:
            return True
        if DUNDER_RE.match(value):
            return True
        return False
    if isinstance(value, bytes):
        return len(value) == 0
    return False


def collect_docstring_node_ids(tree):
    """Return the set of id(node) for every Constant node that is a docstring."""
    doc_ids = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef,
                             ast.FunctionDef, ast.AsyncFunctionDef)):
            body = getattr(node, "body", None) or []
            if body and isinstance(body[0], ast.Expr):
                v = body[0].value
                if isinstance(v, ast.Constant) and isinstance(v.value, str):
                    doc_ids.add(id(v))
    return doc_ids


def find_magic_ok_lines(source):
    """Return {line_number: reason} for every '# magic-ok: <reason>' comment."""
    results = {}
    for idx, line in enumerate(source.splitlines(), start=1):
        m = MAGIC_OK_RE.search(line)
        if m:
            reason = m.group(1).strip()
            if reason:
                results[idx] = reason
    return results


def _kind(value):
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, str):
        return "string"
    if isinstance(value, bytes):
        return "bytes"
    if isinstance(value, (int, float, complex)):
        return "number"
    return type(value).__name__


def _truncate(value, maxlen=200):
    s = repr(value)
    if len(s) > maxlen:
        s = s[:maxlen] + "..."
    return s


def _annotate_parents(tree):
    """Attach a .parent attribute to every child node. Root's parent is None."""
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            child.parent = node


def _enclosing_call(node):
    """Return the ast.Call that `node` is (directly) an argument of —
    either positional or a keyword value. None if not inside a call."""
    parent = getattr(node, "parent", None)
    if isinstance(parent, ast.Call):
        # Positional arg (or Call.func in pathological cases — harmless)
        return parent
    if isinstance(parent, ast.keyword):
        grand = getattr(parent, "parent", None)
        if isinstance(grand, ast.Call):
            return grand
    return None


def _is_role_exempt(node):
    """Return True if the Constant `node`'s AST role makes it non-policy.

    Exempt roles (Path 1):
        1. Dict key in a dict literal: {"key": value}
        2. Subscript index: obj["key"]
        3. First-arg of dict lookup methods: .get/.pop/.setdefault("key")
        4. F-string literal segment OR f-string format_spec piece
           (both are children of ast.JoinedStr)
        5. Argument of logger-level call (debug/info/warning/error/...)
        6. Argument of exception constructor call (name ends in
           Error/Exception/Warning)
        7. Short punctuation strings (length <= 3, no alphanumeric chars)

    These are shape/format/template — not policy values. The Budget Marine
    "7400-000" bug-class lives in Assign/Compare/Return/default-arg roles,
    which are all still flagged.
    """
    parent = getattr(node, "parent", None)
    if parent is None:
        return False

    # (1) Dict key
    if isinstance(parent, ast.Dict):
        if any(k is node for k in parent.keys):
            return True

    # (2) Subscript index
    if isinstance(parent, ast.Subscript):
        if parent.slice is node:
            return True
        # Py<3.9 wraps in ast.Index
        if isinstance(parent.slice, ast.Index) and parent.slice.value is node:
            return True

    # (4) F-string literal segment / format_spec
    if isinstance(parent, ast.JoinedStr):
        return True

    # (3)(5)(6) Call-based roles
    enclosing = _enclosing_call(node)
    if enclosing is not None:
        func = enclosing.func
        if isinstance(func, ast.Attribute):
            if func.attr in DICT_LOOKUP_METHODS:
                if enclosing.args and enclosing.args[0] is node:
                    return True
            if func.attr in LOG_METHOD_NAMES:
                return True
            if func.attr.endswith(EXCEPTION_NAME_SUFFIXES):
                return True
        elif isinstance(func, ast.Name):
            if func.id.endswith(EXCEPTION_NAME_SUFFIXES):
                return True

    # (7) Short punctuation
    if isinstance(node.value, str):
        s = node.value
        if s and len(s) <= 3 and not any(c.isalnum() for c in s):
            return True

    return False


def scan(source, filename="<unknown>"):
    """Return a list of violation dicts. Bypassed lines are still included
    (with has_bypass=True) so callers can audit the bypasses too."""
    try:
        tree = ast.parse(source, filename=filename)
    except SyntaxError as e:
        return [{
            "line": e.lineno or 0,
            "col": e.offset or 0,
            "kind": "syntax_error",
            "value_repr": None,
            "context": "",
            "has_bypass": False,
            "bypass_reason": None,
            "message": f"SyntaxError: {e.msg}",
        }]

    _annotate_parents(tree)
    doc_ids = collect_docstring_node_ids(tree)
    magic_ok = find_magic_ok_lines(source)
    source_lines = source.splitlines()

    violations = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant):
            continue
        if id(node) in doc_ids:
            continue
        val = node.value
        # ast.Constant can hold Ellipsis
        if val is Ellipsis:
            continue
        if is_exempt(val):
            continue
        if _is_role_exempt(node):
            continue
        line = getattr(node, "lineno", 0) or 0
        col = getattr(node, "col_offset", 0) or 0
        bypass = magic_ok.get(line)
        context = ""
        if 1 <= line <= len(source_lines):
            context = source_lines[line - 1].strip()
            if len(context) > 240:
                context = context[:240] + "..."
        violations.append({
            "line": line,
            "col": col,
            "kind": _kind(val),
            "value_repr": _truncate(val),
            "context": context,
            "has_bypass": bypass is not None,
            "bypass_reason": bypass,
        })
    return violations


def _iter_py_files(paths):
    for p in paths:
        path = Path(p)
        if path.is_file() and path.suffix == ".py":
            yield path
        elif path.is_dir():
            for sub in path.rglob("*.py"):
                # Skip venvs / caches automatically
                parts = set(sub.parts)
                if parts & {"__pycache__", ".venv", "venv", "node_modules",
                            ".git", "workspace", "memory"}:
                    continue
                yield sub


def main():
    parser = argparse.ArgumentParser(
        description="Detect magic constants in Python source.")
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--file", type=str, help="Python file to scan")
    src.add_argument("--stdin", action="store_true", help="Read from stdin")
    src.add_argument("--path", action="append", type=str,
                     help="Directory to scan recursively (may repeat)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report violations; exit 0 regardless")
    parser.add_argument("--filename", type=str, default="<stdin>",
                        help="Filename label (used with --stdin)")
    parser.add_argument("--show-bypassed", action="store_true",
                        help="Include bypassed lines in the report")
    args = parser.parse_args()

    payloads = []
    if args.file:
        p = Path(args.file)
        payloads.append((str(p), p.read_text(encoding="utf-8")))
    elif args.stdin:
        payloads.append((args.filename, sys.stdin.read()))
    else:
        for path in _iter_py_files(args.path):
            payloads.append((str(path), path.read_text(encoding="utf-8")))

    all_violations = {}
    total_open = 0
    total_bypassed = 0
    for filename, source in payloads:
        vs = scan(source, filename=filename)
        if not args.show_bypassed:
            vs_filtered = [v for v in vs if not v["has_bypass"]]
        else:
            vs_filtered = vs
        for v in vs:
            if v["has_bypass"]:
                total_bypassed += 1
            else:
                total_open += 1
        if vs_filtered:
            all_violations[filename] = vs_filtered

    report = {
        "summary": {
            "files_scanned": len(payloads),
            "files_with_violations": len(all_violations),
            "open_violations": total_open,
            "bypassed_violations": total_bypassed,
        },
        "violations": all_violations,
    }
    print(json.dumps(report, indent=2, ensure_ascii=False))
    if args.dry_run:
        sys.exit(0)
    sys.exit(1 if total_open else 0)


if __name__ == "__main__":
    main()
