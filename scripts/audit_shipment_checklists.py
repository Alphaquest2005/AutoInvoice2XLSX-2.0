"""
Walk every ``_email_params*.json`` under
``workspace/output/downloads-regression-emails/`` and run
``shipment_checklist`` on each (no emails sent). Prints a per-shipment
report and a top-level summary of failure kinds.

Useful for: "what problems still exist in the pipeline" — runs the
already-deployed checklist offline against the entire regression corpus.

Usage:
    python scripts/audit_shipment_checklists.py
    python scripts/audit_shipment_checklists.py --root workspace/output/downloads-regression-emails
    python scripts/audit_shipment_checklists.py --skip-llm  # skip LLM review (faster)
    python scripts/audit_shipment_checklists.py --json      # machine-readable
"""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import sys
from collections import Counter, defaultdict
from glob import glob

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO_ROOT, 'pipeline'))


def _resolve_path(p: str, base_dir: str) -> str:
    """Try several path-fixups so stale absolute paths in saved
    _email_params.json files (rerun_tmp, old user homes) still resolve to
    files in the same shipment folder when their basename matches."""
    if os.path.exists(p):
        return p
    base = os.path.basename(p)
    cand = os.path.join(base_dir, base)
    if os.path.exists(cand):
        return cand
    return p


def _load_email_params(jp: str) -> dict:
    with open(jp, 'r', encoding='utf-8') as fh:
        ep = json.load(fh)
    base_dir = os.path.dirname(jp)
    fixed = []
    for p in ep.get('attachment_paths', []) or []:
        fixed.append(_resolve_path(p, base_dir))
    ep['attachment_paths'] = fixed
    return ep


def _maybe_load_validation(base_dir: str) -> dict:
    """If the rerun produced a validation summary JSON, load it."""
    for cand in ('validation_summary.json', '_validation.json'):
        path = os.path.join(base_dir, cand)
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as fh:
                    return json.load(fh)
            except (OSError, json.JSONDecodeError):
                pass
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--root', default='workspace/output/downloads-regression-emails')
    ap.add_argument('--skip-llm', action='store_true',
                    help='Disable the LLM email review pass.')
    ap.add_argument('--json', action='store_true',
                    help='Emit machine-readable JSON instead of text report.')
    ap.add_argument('--folder', action='append', default=None,
                    help='Limit to specific shipment folder name(s).')
    args = ap.parse_args()

    root = os.path.join(REPO_ROOT, args.root) if not os.path.isabs(args.root) else args.root
    if not os.path.isdir(root):
        print(f'ERROR: corpus root not found: {root}', file=sys.stderr)
        return 2

    if args.skip_llm:
        # Monkey-patch the LLM reviewer to a no-op before importing checklist.
        import xlsx_validator as _xv
        _xv._llm_review_email_params = lambda *_a, **_kw: None  # noqa: SLF001

    from xlsx_validator import shipment_checklist

    pattern = os.path.join(root, '*', '_email_params*.json')
    all_jsons = sorted(glob(pattern))
    if args.folder:
        wanted = set(args.folder)
        all_jsons = [j for j in all_jsons
                     if os.path.basename(os.path.dirname(j)) in wanted]

    if not all_jsons:
        print(f'No _email_params.json files found under {root}', file=sys.stderr)
        return 1

    per_shipment = []
    kind_counter = Counter()
    severity_counter = Counter()
    kind_examples = defaultdict(list)

    for jp in all_jsons:
        folder = os.path.basename(os.path.dirname(jp))
        suffix = os.path.basename(jp).replace('_email_params', '').replace('.json', '')
        shipment_id = folder + (suffix if suffix else '')

        try:
            ep = _load_email_params(jp)
        except (OSError, json.JSONDecodeError) as e:
            per_shipment.append({
                'shipment': shipment_id,
                'load_error': str(e),
                'failures': [],
            })
            continue

        validation = _maybe_load_validation(os.path.dirname(jp))

        try:
            # shipment_checklist prints to stdout; redirect so it doesn't
            # corrupt our JSON output.
            with contextlib.redirect_stdout(io.StringIO()):
                result = shipment_checklist(ep, validation)
        except Exception as e:
            per_shipment.append({
                'shipment': shipment_id,
                'checklist_error': str(e),
                'failures': [],
            })
            continue

        for f in result.get('failures', []):
            kind = f.get('check', '?')
            kind_counter[kind] += 1
            severity_counter[f.get('severity', '?')] += 1
            if len(kind_examples[kind]) < 3:
                kind_examples[kind].append(f'{shipment_id}: {f.get("message", "")[:140]}')

        per_shipment.append({
            'shipment': shipment_id,
            'passed': result.get('passed', False),
            'blocker_count': result.get('blocker_count', 0),
            'warning_count': result.get('warning_count', 0),
            'failures': result.get('failures', []),
        })

    if args.json:
        json.dump({
            'shipments': per_shipment,
            'kind_counts': dict(kind_counter),
            'severity_counts': dict(severity_counter),
            'kind_examples': {k: kind_examples[k] for k in kind_counter},
        }, sys.stdout, indent=2)
        print()
        return 0

    # Text report
    total = len(per_shipment)
    passed = sum(1 for s in per_shipment if s.get('passed'))
    blocked = sum(1 for s in per_shipment if not s.get('passed', True))

    print()
    print('=' * 80)
    print(f'CORPUS CHECKLIST AUDIT — {total} shipment params')
    print('=' * 80)
    print(f'  Passed:  {passed}')
    print(f'  Blocked: {blocked}')
    print(f'  Total findings: {sum(kind_counter.values())} '
          f'(blockers={severity_counter.get("block", 0)}, '
          f'warns={severity_counter.get("warn", 0)})')
    print()

    print('FAILURE KIND DISTRIBUTION (most common first)')
    print('-' * 80)
    for kind, n in kind_counter.most_common():
        print(f'  {n:>4}  {kind}')
        for ex in kind_examples[kind][:2]:
            print(f'           e.g. {ex}')
    print()

    print('PER-SHIPMENT BLOCKED ENTRIES')
    print('-' * 80)
    for s in per_shipment:
        if s.get('passed', True):
            continue
        print(f'  {s["shipment"]}  '
              f'blockers={s.get("blocker_count", 0)}  '
              f'warns={s.get("warning_count", 0)}')
        for f in s.get('failures', []):
            if f.get('severity') == 'block':
                print(f'      [BLOCK] {f.get("check")}: {f.get("message", "")[:140]}')
    print()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
