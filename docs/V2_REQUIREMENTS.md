# AutoInvoice2XLSX v2.0 — Authoritative Requirements Catalog

> Consolidated from every directive, correction, and success-confirmation Joseph
> has given across the v2.0 restructuring sessions. This is the single source
> of truth for what "done" means. If an implementation decision is not covered
> here, it is out of scope until added.
>
> **Never ask the user to repeat himself. If a requirement is ambiguous, read
> this file first; only then ask.**

---

## 0. Non-negotiable laws

These override everything else — convenience, performance, elegance, schedule.

| #   | Law                                                                                   | Source directive (verbatim, paraphrased if indicated)                                                                             |
| --- | ------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| L1  | **SSOT** — one implementation per responsibility. No duplicates. No "parallel" code. | "Settings loading duplicated in 3 places" was the v1 sin that triggered v2.                                                       |
| L2  | **Zero variance** — `TOTAL $ VARIANCE` must be an Excel formula that evaluates to 0. | "variance check must = $0.00 as Excel formula (not value)"                                                                        |
| L3  | **Honest data** — never absorb variance silently into ADJUSTMENTS or any other cell.  | "remove honest-mode variance absorption" (commit b3d1f54); "no variance absorption"                                               |
| L4  | **End-nodes only** — only 8-digit CARICOM CET codes ever appear in output.            | CARICOM skill golden rule; "END-NODES ONLY"                                                                                       |
| L5  | **All items accounted for** — every line item must be classified & placed somewhere. | Split-item verification tests; CC-charge cross-validation                                                                         |
| L6  | **Full context before action** — read first, implement second, never guess.           | "never implement without full context, always read first" (feedback memory)                                                       |
| L7  | **TDD, always** — failing test → implementation → passing test. No exceptions.        | "strict TDD"                                                                                                                      |
| L8  | **No silent failures** — 0-items, empty OCR, missing codes must escalate, not swallow. | ANDREA_L regression: parser returned 0 items silently                                                                             |
| L9  | **Copy-first, refactor-later** — strangler-fig migration, not rewrites.               | "Copy interfaces/types verbatim first, get working, then refactor incrementally" (feedback memory)                                |
| L10 | **Never repeat the user** — extract requirements from history, don't re-ask.          | "i cant repeat myself like that ultrathink" (current session)                                                                     |

---

## 1. Domain correctness (CARICOM customs)

### 1.1 Tariff classification

- **R1.1.1** Only 8-digit CARICOM CET codes may appear in generated XLSX output
  (no 4/6-digit HS headings, no US HTS codes).
- **R1.1.2** Classification must consult, in priority order:
  1. Static rules (`config/classification_rules.json`)
  2. Persistent SKU store (SQLite `code_repository`)
  3. LLM fallback (Anthropic Claude) with cached answers
- **R1.1.3** LLM classification answers must be cached to disk and persisted to
  the SKU store so the same SKU is never re-classified across runs.
- **R1.1.4** ASYCUDA duty formula (`reference_asycuda_duty_formula.md`) is the
  canonical duty cross-check:
  `duty = CET + CSC(6%) + VAT(15% on CIF+CET+CSC)`.
- **R1.1.5** Invalid code mapping is a **data asset, not a code constant** —
  it lives in JSON/SQLite, not in Python literals.

### 1.2 Arithmetic integrity

- **R1.2.1** `VARIANCE_CHECK` cell is always an Excel formula referencing live
  cells. Never a hard-coded 0.
- **R1.2.2** `ADJUSTMENTS` may only contain legitimate, labelled adjustments
  (free-shipping netting, documented discounts). It is **not** a dumping
  ground for reconciliation slop.
- **R1.2.3** Free-shipping netting: when `shipping ≈ free_shipping` (within
  $0.01), both freight and free_shipping must zero out. Orphan
  `free_shipping` (without matching freight) must not create a deduction.
  (Regression tests: `test_free_shipping_nets_to_zero`,
  `test_free_shipping_orphan_ignored`.)
- **R1.2.4** Currency parsing must accept OCR comma-as-decimal (`9,31` → 9.31)
  while still honouring thousands separators (`1,234.56` → 1234.56,
  `1,000` → 1000.0). Regression: `test_convert_type_comma_decimal`.
- **R1.2.5** CC-charge cross-validation: the sum of items + prorated tax per
  declaration must match each CC charge within $2.

### 1.3 Multi-declaration splitting

- **R1.3.1** One XLSX file per HAWB (House Air Waybill), not one per shipment
  folder.
- **R1.3.2** Split manifests (e.g. ANDREA_L) must be processed exactly once —
  no double-counting when a shipment yields multiple declarations.
- **R1.3.3** CC charges guide item grouping for Amazon invoices: items whose
  totals + prorated tax approximate each CC transaction form one declaration.

### 1.4 Expected Entries

- **R1.4.1** `Expected Entries` is the **number of generated XLSX sheets /
  declarations**, not the source invoice count. (Feedback memory.)

---

## 2. Input pipeline

### 2.1 PDF extraction

- **R2.1.1** Multi-engine OCR with deterministic fallback order:
  pdfplumber → PyMuPDF → Tesseract (+ LLM vision for handwritten).
- **R2.1.2** Every extraction is **disk-cached by SHA1** of PDF bytes.
  Re-running a shipment must hit cache, not re-OCR.
- **R2.1.3** LLM vision calls for handwritten declarations must:
  - disk-cache the result;
  - retry with backoff on timeout;
  - **never block item-splitting** when the call fails — escalate, don't
    swallow. (Memory: `feedback_vision_caching.md`.)
- **R2.1.4** 0-items/empty-text outcomes are **escalation events**, not
  warnings. They must produce a flagged report entry, not a silent `return
  None` / `continue`.

### 2.2 Format parsing

- **R2.2.1** Format parsers are **data-driven**: one YAML spec per supplier
  format in `config/formats/`. New formats are added by adding a YAML file,
  not by writing Python.
- **R2.2.2** `config/formats/alibaba_marketplace_invoice.yaml` is known
  corrupted; do **not** mass-fix. Flag for Joseph's manual review.
  (Memory: `project_broken_alibaba_yaml.md`.)
- **R2.2.3** Page-reorder must apply to both single-invoice and batch paths
  (commit b3d1f54).

---

## 3. Output pipeline

### 3.1 XLSX generation

- **R3.1.1** Column spec is data (YAML/JSON), not code. A new column never
  requires touching the writer.
- **R3.1.2** Column structure is preserved verbatim from the template —
  column count, order, widths, named ranges, and validation remain stable.
- **R3.1.3** Every formula in the template is preserved as a formula
  (including `VARIANCE_CHECK`, `TOTAL_CIF`, duty roll-ups).
- **R3.1.4** 41 historical XLSX files have `VARIANCE_CHECK=0` (value, not
  formula) from the pre-fix variance_fixer. Current code must self-heal
  on re-open; do not migrate those historical files.
  (Memory: `project_variance_check_historical.md`.)

### 3.2 Email delivery

- **R3.2.1** Only problematic entries (LLM could not solve, non-zero
  variance, missing codes) are emailed.
- **R3.2.2** Zero-variance entries complete silently — **do not email them**.
  (Current session directive.)
- **R3.2.3** Email sender is a single adapter; no duplicate SMTP code paths.
- **R3.2.4** Send-history is persisted so the same XLSX is never re-sent
  automatically after retry.

---

## 4. Architecture

### 4.1 Hexagonal / Ports & Adapters

- **R4.1.1** Dependencies flow inward: `composition → application → domain`.
  Domain imports **nothing** from adapters.
- **R4.1.2** Every external boundary is a port with exactly one production
  adapter and one in-memory test adapter:
  - `PdfExtractorPort` → composite OCR adapter
  - `ConfigProviderPort` → YAML/JSON adapter
  - `CodeRepositoryPort` → SQLite adapter
  - `XlsxWriterPort` / `XlsxReaderPort` → openpyxl adapter
  - `EmailGatewayPort` → SMTP/IMAP adapter
  - `LlmClientPort` → Anthropic adapter
  - `FileSystemPort` → OS adapter
  - `ClassificationStorePort` → JSON/SQLite adapter
- **R4.1.3** All wiring happens in `composition/container.py` using
  constructor injection. No service locator, no globals.

### 4.2 SSOT enforcement

- **R4.2.1** The existence of **two parallel implementations** of the same
  responsibility is a build failure, not a style issue.
- **R4.2.2** As the `src/autoinvoice/` hex port becomes functional for a
  bounded context, the equivalent file(s) under `pipeline/` must be
  **deleted**, not left "for reference".

### 4.3 Clean code / SOLID

- **R4.3.1** No file over ~500 LOC without an architectural justification.
  `pipeline/run.py` at 4418 LOC is the current worst offender and a v1
  regression (v1 complaint was 1200 LOC).
- **R4.3.2** Single Responsibility per module; Dependency Inversion at every
  external boundary.
- **R4.3.3** Naming is self-documenting; comments explain *why*, never *what*.

---

## 5. Process & quality gates

### 5.1 TDD / BDD

- **R5.1.1** Red → Green → Refactor. New behaviour ships with a failing test
  first.
- **R5.1.2** BDD `.feature` files (Gherkin) capture user-visible behaviour;
  unit tests cover domain logic.
- **R5.1.3** Coverage threshold: **≥85%** on domain + application. Adapters
  covered by contract tests; composition covered by smoke tests.

### 5.2 Automated quality gates

- **R5.2.1** Pre-commit hook runs: `ruff` (lint + format), `mypy`, `pytest`.
  Commit is blocked on failure.
- **R5.2.2** No `--no-verify` unless Joseph explicitly authorises for the
  specific commit.
- **R5.2.3** Auto-commit checkpoints are acceptable (see recent
  `chore(auto): claude session checkpoint` commits) but must still pass
  hooks.

### 5.3 Migration hygiene

- **R5.3.1** Strangler-fig: migrate **one bounded context at a time**; each
  migration ends with the old code deleted and a green test suite.
- **R5.3.2** Progress is measured by two monotonically-decreasing numbers:
  `LOC(pipeline/run.py)` and `len(find pipeline -name '*.py')`, and one
  monotonically-increasing number: coverage on `src/autoinvoice/`.

---

## 6. Behavioural contracts (Joseph's working style)

- **R6.1** Terse output. Do not summarise diffs back at him.
- **R6.2** Don't propose changes to unread code.
- **R6.3** Confirmations count as directives — when Joseph says "yes that
  was right", save why, not just that.
- **R6.4** When blocked, escalate with a concrete question; don't churn.
- **R6.5** Preserve every uncommitted file; never `git checkout .` or
  `git reset --hard` without explicit authorisation.

---

## 7. Traceability

Every requirement above maps to one or more of:

- A verbatim user directive (this session or prior — see
  `/home/joseph/.claude/projects/.../memory/`).
- A golden rule in `CARICOM_COMPREHENSIVE_SKILL.md`.
- A section of `PIPELINE_SPECIFICATION.md`.
- A committed regression test under `tests/`.

When a new directive arrives, append it here with its source — **never**
scatter it across memory files only.
