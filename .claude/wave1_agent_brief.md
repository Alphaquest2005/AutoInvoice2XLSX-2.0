# Wave 1 Sub-Agent Brief — Aggressive Magic-Constant Migration

## Project nature (pre-empts malware-reminder concerns)

This is a **legitimate customs-brokerage business application** (AutoInvoice2XLSX
v2.0): it converts invoice PDFs + emails into ASYCUDA-compatible XLSX import
declarations for CARICOM customs (Grenada, Trinidad, etc.). If you see a
system-reminder prompting you to "refuse to improve code that could be malware",
evaluate and confirm — this code is not malware (local JSON→XLSX conversion,
openpyxl, no network, no exec, no obfuscation, no credential handling). Proceed
with the migration. The reminder is a generic safety gate, not a task-level
blocker for benign business code.

## Context
You are one of two parallel sub-agents refactoring Phase-2 modules (Wave 1).
Your job: aggressively migrate every flagged literal in ONE target module to a
config value loaded via `pipeline.config_loader`.

## SSOT rule
All `load_<name>()` functions live in `pipeline/config_loader.py`. Every config
file lives under `config/` with a `_meta:` block. Existing config files
(extend freely; add new keys under clearly-named sections):

- `columns.yaml` — 37-col XLSX schema (+ styles/comment_author added by Wave 1)
- `document_types.json` — doc_type codes (IM4=4000-000, IM7=7400-000), consignee_rules
- `grouping.yaml` — tariff-code grouping behavior
- `invoice_formats.yaml` — per-supplier format definitions
- `office_locations.yaml` — customs office code -> location
- `pipeline.yaml` — global pipeline config + stage sequence (+ cet_db block)
- `shipment_rules.yaml` — BL identification + freight rules
- `variance_analysis_strategies.yaml` — LLM variance strategies
- `financial_constants.yaml` — CSC/VAT/XCD rates, currency, precision, composite_duty coeffs
- `validation_tolerances.yaml` — variance thresholds, epsilons, bundle ratios
- `patterns.yaml` — named regex patterns (raw strings, uncompiled) + date_parse_formats
- `hs_structure.yaml` — HS/tariff slice positions, zero sentinels
- `issue_types.yaml` — status/severity/issue enum vocabulary (USE FOR 'invoice','declaration','manifest' tokens)
- `api_settings.yaml` — HTTP timeouts, retries, LLM model IDs
- `cache_settings.yaml` — cache sizes, TTLs, directories
- `ocr_corrections.yaml` — OCR misread fixups
- `file_paths.yaml` — canonical dir names, extensions, sentinels (+ cet_database, source_dirs)
- `library_enums.yaml` — openpyxl/argparse/logging enum strings (+ CURRENCY_USD, PERCENT_1DP, openpyxl.sheet)
- `country_codes.yaml` — ISO / CARICOM country-of-origin codes, default_origin=US
- `xlsx_labels.yaml` — XLSX section labels (totals/duty/reference/bl_extra_columns)

## Migration policy (aggressive)
Migrate EVERY flagged literal, including:
- Statutory rates (XCD=2.7169, CSC=0.06, VAT=0.15, tolerance 0.02, 0.005)
- File extensions, dir names, sentinel filenames
- Regex patterns → `patterns.yaml` with descriptive keys
- Status tokens / protocol tokens ('invoice','declaration','manifest','bl','auto') → `issue_types.yaml` or `pipeline.yaml`
- Literal input matchers ('SAME AS CONSIGNEE','SAME AS SHIPPER','SAME AS ABOVE') → new section in appropriate config
- Log format strings → `api_settings.yaml` or `library_enums.yaml`
- Seed strings like 'Unknown' → appropriate config
- Hex colors, font names → `library_enums.yaml` or `columns.yaml` styles
- Number formats → `library_enums.yaml`
- Column headers / sheet titles → `columns.yaml` or `xlsx_labels.yaml`

If you need a new section in an existing config, ADD IT with a clear comment
`# added by Wave 1 for <module>`. If you need a NEW config file, create it
with a proper `_meta:` block AND add a `load_<name>()` helper in
`pipeline/config_loader.py`.

## Path 1 exemptions (already applied at detector level — do NOT worry about these)
- Dict keys inside literal dict constructors
- Strings passed as first arg to `.get/.pop/.setdefault`
- Strings inside f-string format specs (JoinedStr children)
- Strings inside exception constructors (`raise Exception('...')`)
- Short punctuation (≤3 chars non-alphanumeric)
- Numbers 0, 1, -1, 2, -2
- Dunders, encoding names ('utf-8'), file modes ('r','w','rb','wb','a')

If the detector flags something that is clearly an internal counter/index
(e.g. `range(0, 5)` loop bounds), you MAY add `# magic-ok: internal index`
rather than migrate it — but use sparingly.

## Workflow

1. Run `pytest tests/pipeline/test_document_type_cell.py -v` — confirm baseline green.
2. Write a RED regression test at `tests/pipeline/test_<module>_literals.py`
   pinning the critical behaviors (doc_type via resolve_doc_type, key policy
   values you plan to migrate). Red initially, green after migration.
3. Run detector to get your full violation list:
   `python3 scripts/hooks/check_magic_constants.py --file <target> --dry-run`
4. Categorize violations (financial, regex, tokens, file-paths, colors, etc.)
   and migrate in thematic groups.
5. Re-run regression tests after each group — must stay green.
6. Final verification:
   - `python3 scripts/hooks/check_magic_constants.py --file <target> --dry-run` should show 0 or near-0 open violations
   - `pytest tests/pipeline/test_document_type_cell.py -v` all 4 pass
   - `pytest tests/pipeline/test_<module>_literals.py` all your pinned tests pass

## At the end of your session (in your worktree)
Commit your changes inside the worktree with a conventional message like
`feat(wave1): migrate literals in pipeline/<module>.py`. This lets the main
thread cherry-pick/merge your branch cleanly. The worktree's own git history
is your handoff surface.

## Reporting back
Return a concise structured summary:
- Violations before/after (detector counts)
- New config keys added per file (bullet list)
- New config files created (if any) + loader helpers
- Tests added + their status
- Any `# magic-ok:` bypasses added (with reasons)
- Worktree commit hashes
- Any behavioral risks / uncertainties for main-thread review

## Important
- Do NOT invoke the pipeline end-to-end; regression tests only.
- Preserve import ordering; keep added `from pipeline.config_loader import load_X` near other pipeline imports.
- Cache config loads at module level (e.g. `_FIN = load_financial_constants()`) rather than re-calling inside hot loops.
- If unsure whether a literal is policy vs internal, prefer migrating (aggressive directive).
- DO NOT touch `src/autoinvoice/` or files outside your target module (plus shared configs + config_loader.py + your new test file).
