#!/usr/bin/env python3
"""Shared config-registry module for the magic-constant hook system.

Discovers every configuration file under config/ (*.yaml, *.yml, *.json),
reads its '_meta:' block, and exposes the inventory to the other hooks.

'_meta:' block schema:
    _meta:
      purpose: <one-line string>            # required
      covers: [<str>, <str>, ...]           # required, non-empty
      match_patterns: [<str>, ...]          # required, non-empty; used by
                                            #   the LLM to find the right
                                            #   config file for a literal
      loader: <module:function>             # optional, dotted path
      examples: [<str>, ...]                # optional
      update_checklist: [<str>, ...]        # optional

Required fields are validated by has_valid_meta(); missing/empty required
fields make is_valid=False and the check_config_meta hook will block.

No third-party dependencies: YAML is read via a tolerant minimal loader
when PyYAML is not importable. The '_meta:' block is always near the top
of the file, so we only need enough YAML to parse a flat block followed by
simple lists of strings.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

try:
    import yaml  # type: ignore
    _HAS_YAML = True
except ImportError:
    yaml = None  # type: ignore
    _HAS_YAML = False


REPO_ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_DIR = REPO_ROOT / "config"

YAML_SUFFIXES = {".yaml", ".yml"}
JSON_SUFFIXES = {".json"}
REQUIRED_META_KEYS = ("purpose", "covers", "match_patterns")


# --------------------------------------------------------------- discovery


def iter_config_files(config_dir: Path | None = None):
    """Yield Path objects for every *.yaml|*.yml|*.json file under config/."""
    cfg_dir = Path(config_dir) if config_dir else CONFIG_DIR
    if not cfg_dir.is_dir():
        return
    for p in sorted(cfg_dir.iterdir()):
        if not p.is_file():
            continue
        if p.suffix in YAML_SUFFIXES or p.suffix in JSON_SUFFIXES:
            yield p


# --------------------------------------------------------------- parsing


def _parse_json(text: str) -> dict | None:
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return None
    if isinstance(data, dict):
        return data
    return None


def _parse_yaml_pyyaml(text: str) -> dict | None:
    try:
        data = yaml.safe_load(text)  # type: ignore[union-attr]
    except Exception:
        return None
    if isinstance(data, dict):
        return data
    return None


_META_HEADER_RE = re.compile(r"^_meta\s*:\s*$")
_LIST_ITEM_RE = re.compile(r"^\s+-\s+(.*)$")
_KEY_VALUE_RE = re.compile(r"^(\s*)([A-Za-z_][A-Za-z0-9_]*)\s*:\s*(.*)$")


def _strip_yaml_string(raw: str) -> str:
    """Remove surrounding quotes and trailing comments from a YAML scalar."""
    s = raw.strip()
    # Drop trailing YAML comments
    if "#" in s:
        # Naive but good enough for scalars without quoted '#'.
        s = s.split("#", 1)[0].rstrip()
    if (len(s) >= 2) and ((s[0] == '"' and s[-1] == '"')
                          or (s[0] == "'" and s[-1] == "'")):
        s = s[1:-1]
    return s


def _parse_meta_block_fallback(text: str) -> dict | None:
    """Tolerant mini-parser for JUST the _meta: block in a YAML file.

    Only used when PyYAML is unavailable. Recognizes:
        _meta:
          purpose: "..."
          covers: [inline, list]
          covers:
            - line
            - line
          loader: module.path:fn
    Nested mappings inside _meta: are not supported (none of our meta
    blocks need them).
    """
    lines = text.splitlines()
    in_meta = False
    meta_indent = None
    result: dict[str, Any] = {}
    current_key: str | None = None
    current_list: list[str] | None = None

    for line in lines:
        if not in_meta:
            if _META_HEADER_RE.match(line):
                in_meta = True
                meta_indent = None
            continue

        # End of meta: a non-indented, non-blank, non-comment line.
        if line.strip() == "" or line.lstrip().startswith("#"):
            continue
        stripped = line.lstrip()
        indent = len(line) - len(stripped)
        if indent == 0:
            break
        if meta_indent is None:
            meta_indent = indent
        if indent < meta_indent:
            break

        # List continuation?
        if current_list is not None and indent > meta_indent:
            m_item = _LIST_ITEM_RE.match(line)
            if m_item:
                current_list.append(_strip_yaml_string(m_item.group(1)))
                continue
            # Fallthrough: back to key/value parsing.

        m_kv = _KEY_VALUE_RE.match(line)
        if m_kv and len(m_kv.group(1)) == meta_indent:
            key = m_kv.group(2)
            val = m_kv.group(3).strip()
            if val == "":
                # A following indented list.
                current_key = key
                current_list = []
                result[key] = current_list
            elif val.startswith("[") and val.endswith("]"):
                inner = val[1:-1].strip()
                if inner == "":
                    result[key] = []
                else:
                    parts = [_strip_yaml_string(x) for x in inner.split(",")]
                    result[key] = [p for p in parts if p != ""]
                current_key = None
                current_list = None
            else:
                result[key] = _strip_yaml_string(val)
                current_key = None
                current_list = None

    return result or None


def load_meta(path: Path) -> dict | None:
    """Return the _meta: block as a dict, or None if absent/unparseable."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None

    if path.suffix in JSON_SUFFIXES:
        data = _parse_json(text)
        if not data:
            return None
        meta = data.get("_meta")
        return meta if isinstance(meta, dict) else None

    if path.suffix in YAML_SUFFIXES:
        if _HAS_YAML:
            data = _parse_yaml_pyyaml(text)
            if isinstance(data, dict):
                meta = data.get("_meta")
                return meta if isinstance(meta, dict) else None
            return None
        return _parse_meta_block_fallback(text)

    return None


# --------------------------------------------------------------- validation


def has_valid_meta(meta: dict | None) -> tuple[bool, list[str]]:
    """Return (is_valid, missing_or_empty_keys)."""
    if not isinstance(meta, dict):
        return False, list(REQUIRED_META_KEYS)
    missing: list[str] = []
    for key in REQUIRED_META_KEYS:
        if key not in meta:
            missing.append(key)
            continue
        value = meta[key]
        if key == "purpose":
            if not (isinstance(value, str) and value.strip()):
                missing.append(key)
        else:
            # covers, match_patterns must be non-empty lists of strings.
            if not (isinstance(value, list) and value
                    and all(isinstance(x, str) and x.strip() for x in value)):
                missing.append(key)
    return (not missing), missing


# --------------------------------------------------------------- inventory


def build_inventory(config_dir: Path | None = None) -> list[dict]:
    """Return a list of inventory entries, one per config file.

    Each entry:
        {
            "path": "config/columns.yaml",
            "name": "columns.yaml",
            "has_meta": bool,
            "valid": bool,
            "missing": [<required keys missing>],
            "purpose": <str or None>,
            "match_patterns": [<str>, ...],
            "loader": <str or None>,
        }
    """
    entries: list[dict] = []
    for p in iter_config_files(config_dir):
        meta = load_meta(p)
        valid, missing = has_valid_meta(meta)
        entries.append({
            "path": str(p.relative_to(REPO_ROOT))
                    if REPO_ROOT in p.parents else str(p),
            "name": p.name,
            "has_meta": meta is not None,
            "valid": valid,
            "missing": missing,
            "purpose": (meta.get("purpose") if isinstance(meta, dict) else None),
            "match_patterns": (meta.get("match_patterns")
                               if isinstance(meta, dict)
                               and isinstance(meta.get("match_patterns"), list)
                               else []),
            "loader": (meta.get("loader") if isinstance(meta, dict) else None),
        })
    return entries


def inventory_summary_text(entries: list[dict] | None = None) -> str:
    """Human-readable multi-line inventory string for hook guidance."""
    if entries is None:
        entries = build_inventory()
    if not entries:
        return "(no config files found)"
    lines = []
    for e in entries:
        flag = "" if e["valid"] else " [INVALID _meta]"
        purpose = e["purpose"] or "(no purpose declared)"
        patterns = ", ".join(e["match_patterns"]) if e["match_patterns"] else "-"
        lines.append(f"  - {e['name']}{flag}: {purpose}")
        lines.append(f"      match_patterns: {patterns}")
    return "\n".join(lines)


if __name__ == "__main__":
    # CLI dump for debugging.
    import sys
    inv = build_inventory()
    print(json.dumps(inv, indent=2, ensure_ascii=False))
    invalid = [e for e in inv if not e["valid"]]
    sys.exit(1 if invalid else 0)
