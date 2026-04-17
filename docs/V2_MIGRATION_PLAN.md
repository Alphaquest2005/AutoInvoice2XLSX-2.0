# AutoInvoice2XLSX v2.0 — Migration Plan (Strangler-Fig Resolution)

> Companion to `docs/V2_REQUIREMENTS.md`. This document answers the Option A
> vs Option B question Joseph raised:
>
> > "i really dont understand the impleementation and what to suggest because
> > i think both have valuable code"
>
> **Neither pure A nor pure B. The answer is strangler-fig.**

---

## 1. The dilemma, restated

| Option | Description                                                                       | Problem                                                                        |
| ------ | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| A      | Retire `src/autoinvoice/` entirely; admit `pipeline/` is canon.                  | Discards months of hex-architecture work; cements the 4418-LOC monolith as the future. Violates R4.1, R4.3. |
| B      | Finish the hex migration; move responsibilities from `pipeline/` into `src/`.    | Slow; until it completes you have **both** codebases = SSOT violation (L1, R4.2.1). |

**Synthesis:** Option B is the destination. Option A is what the *end state*
looks like from a distance (one codebase). The migration itself is a
**strangler-fig** that makes SSOT true *per bounded context*, one at a time,
with old code **deleted** as new code takes over.

This matches R4.2.2, R5.3.1, and the `feedback_copy_first_refactor_later.md`
memory exactly.

---

## 2. Governing invariants during migration

1. **Two implementations of the same responsibility is a build failure.** The
   moment `src/autoinvoice/X` is wired in, the corresponding `pipeline/X.py`
   files are deleted in the same commit. No "for reference" copies.
2. **The pipeline never stops working.** Each migration step ships with a
   green end-to-end run of a real shipment from `workspace/shipments/`.
3. **Progress is visible in numbers**, not prose:
   - `wc -l pipeline/run.py` strictly decreasing
   - `find pipeline -name '*.py' | wc -l` strictly decreasing
   - `pytest --cov=src/autoinvoice` strictly increasing, target ≥85%
4. **Every migrated responsibility has a failing-then-passing test first**
   (R5.1.1). No migration without a test.
5. **Copy verbatim first, refactor second** (L9). The first commit moving a
   responsibility should preserve behaviour bit-for-bit; style/clean-up
   lands in a follow-up commit.

---

## 3. Bounded contexts, ordered by migration priority

Ordering criterion: closeness-to-port × risk-reduction-value × LOC-retired.

| #   | Context                        | Current location (pipeline/)                                        | Target (src/autoinvoice/)                                          | Why this order                                                                  |
| --- | ------------------------------ | ------------------------------------------------------------------- | ------------------------------------------------------------------ | ------------------------------------------------------------------------------- |
| 1   | **SSOT contract test**         | —                                                                   | `tests/integration/test_ssot_contract.py`                          | Prevents regression *while* migrating. Ships first.                             |
| 2   | **Classification**             | `run.py` classifier sections, `stages/*classif*`, JSON rules loader | `domain/services/classifier.py` + `adapters/storage/sqlite_code_repo.py` | Closest to a port already; high ROI; de-risks R1.1.*.                        |
| 3   | **Variance / arithmetic**      | `xlsx_validator.py` (1584 LOC), variance_fixer fragments            | `domain/services/variance.py` + `adapters/xlsx/openpyxl_writer.py` | Directly enforces L2, L3. Kills a 1584-LOC file.                                |
| 4   | **Format parsing**             | `format_parser.py` (2795 LOC), `config/formats/*.yaml`              | `domain/services/parser.py` + `adapters/config/yaml_config_provider.py` | Biggest single file; data-driven already, so the port is obvious.          |
| 5   | **PDF / OCR extraction**       | `multi_ocr.py`, `stages/extract*`                                   | `adapters/pdf/composite_extractor.py`                              | Already partially ported; finish and delete the old module.                     |
| 6   | **Split-manifest & CC-charge** | `run.py` splitting logic, `_extract_cc_charges`                     | `domain/services/splitter.py`                                      | Fixes the ANDREA_L double-processing regression (R1.3.2).                        |
| 7   | **XLSX generation**            | `run.py` write paths, template loading                              | `adapters/xlsx/openpyxl_writer.py` + `domain/models/xlsx_spec.py`  | Locks in R3.1.*.                                                                |
| 8   | **Email / send-history**       | `run.py` email section, history JSON writes                         | `adapters/email/*`                                                 | Enforces R3.2.*; also the zero-variance-no-email policy.                        |
| 9   | **Composition root**           | `run.py` `main()`, CLI args                                         | `composition/cli.py` + `composition/container.py`                  | Last — once all ports are wired, the old `main()` has nothing left to do.        |
| 10  | **Delete `pipeline/run.py`**   | `run.py` (→ 0 LOC)                                                  | —                                                                  | End state. Option A's goal, reached by Option B's means.                         |

---

## 4. Per-step ritual

Each bounded context follows the same five-phase ritual. No shortcuts.

### Phase A — Characterisation
1. Read every `pipeline/` file that touches this context.
2. Catalogue public entry points, side-effects, file I/O, and config inputs.
3. Write the characterisation test(s) that pin current behaviour on a real
   shipment fixture from `workspace/shipments/`. Must go green **against
   the current `pipeline/` code**.

### Phase B — Port definition
1. Define / verify the port in `src/autoinvoice/domain/ports/`.
2. Define / verify the domain model(s) in `src/autoinvoice/domain/models/`.
3. Contract test for the port against a fake adapter.

### Phase C — Adapter implementation (copy-first)
1. Copy relevant `pipeline/` code verbatim into the adapter, adapting only
   the surface to match the port.
2. Run characterisation test from Phase A against the new adapter — must
   go green with **zero behavioural diff**.

### Phase D — Switch + delete
1. Wire the new adapter into `composition/container.py`.
2. Remove the `pipeline/` code path. **Delete the file(s) in the same
   commit.** No "reference" copies.
3. Re-run a real shipment end-to-end; variance must remain zero where it
   was zero before.

### Phase E — Refactor (separate commit)
Only after D is green: apply clean-code / SOLID improvements. This is the
only phase where behaviour may change (e.g. to fix a pre-existing bug
documented in the characterisation test).

---

## 5. Numerical exit criteria

Migration is complete when **all** hold simultaneously:

- [ ] `find pipeline -name '*.py' | wc -l` == 0
- [ ] `wc -l pipeline/run.py` == 0 (file deleted)
- [ ] `pytest --cov=src/autoinvoice --cov-fail-under=85` green
- [ ] `ruff check src tests` clean
- [ ] `mypy src` clean
- [ ] A full run of every shipment under `workspace/shipments/` produces
      XLSX files with `VARIANCE_CHECK` = formula evaluating to 0 (or a
      flagged problem report, never silent).
- [ ] `docs/V2_REQUIREMENTS.md` has no unchecked requirement.

---

## 6. What lands first (concretely)

Step 1 — SSOT contract test — lands in the same commit as this plan.

Contents of `tests/integration/test_ssot_contract.py`:

- Asserts there is exactly **one** class that writes XLSX output.
- Asserts there is exactly **one** code path that sends email.
- Asserts there is exactly **one** classification rule loader.
- Asserts there is exactly **one** variance-check writer.
- Each assertion is currently expected to **fail** (red) — that is the
  point. The test encodes the SSOT debt as a green-able target.

The test starts red; every subsequent migration step turns one assertion
green; the last migration step leaves the whole test green and the
`pipeline/` directory empty.

---

## 7. Open questions (escalate, do not invent answers)

- **Q1** `alibaba_marketplace_invoice.yaml` — abandon, rewrite from a sample
  invoice, or import from v1 git history? (Memory says don't mass-fix.)
- **Q2** Historical 41 XLSX files with `VARIANCE_CHECK=0` value — leave
  untouched, or back-fill with a one-off script? (Memory says leave.)
- **Q3** LLM vision quota / cost ceiling for handwritten declarations in
  bulk reruns — current behaviour is "retry forever"; acceptable?

Joseph will be asked exactly once, with these three questions bundled,
before any step that would force an answer.
