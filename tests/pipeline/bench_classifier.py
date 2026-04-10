#!/usr/bin/env python3
# ruff: noqa: E501
"""
Standalone classification benchmark harness (WS-B1).

This is NOT a pytest test. Invoke it directly from the command line:

    # Live mode (burns real LLM credits, records responses):
    python tests/pipeline/bench_classifier.py \\
        --folder /mnt/d/OneDrive/Clients/WebSource/Downloads \\
        --limit 3 --mode live

    # Replay mode (free, deterministic, from recorded JSON):
    python tests/pipeline/bench_classifier.py \\
        --folder /mnt/d/OneDrive/Clients/WebSource/Downloads --mode replay

    # Mock mode (free, returns canned fallbacks — less informative):
    python tests/pipeline/bench_classifier.py \\
        --folder /mnt/d/OneDrive/Clients/WebSource/Downloads --mode mock

Flags:
    --folder PATH         PDF folder (default: env AUTOINVOICE_DOWNLOADS or /mnt/d/OneDrive/Clients/WebSource/Downloads)
    --limit N             Process only first N PDFs (default: 5). Pass 0 for unlimited.
    --mode {live,replay,mock}
    --record-path PATH    Where to read/write recorded LLM responses
                          (default: tests/pipeline/bench_classifier_recordings.json)
    --items-cache PATH    Cached extracted items (default: tests/pipeline/bench_classifier_items.json)
    --output PATH         Where to write result markdown (default: stdout)
    --ground-truth PATH   Assessed JSON (default: data/assessed_classifications.json)
    --verbose             Enable DEBUG logging

Safety:
    --limit 0 unlocks the full corpus. The default of 5 is deliberate so accidental
    runs don't cost real LLM credits. The user should invoke the full run manually.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Any

# ── Path wiring ────────────────────────────────────────────────────────────
# Put the pipeline/ directory on sys.path so `import classifier`,
# `import classifier_batch`, and `from core.llm_client import ...` work.
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_PIPELINE_DIR = os.path.join(_REPO_ROOT, "pipeline")
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


logger = logging.getLogger("bench_classifier")


# ── Record/replay LLM stub ─────────────────────────────────────────────────


class StubLLMClient:
    """
    Drop-in replacement for core.llm_client.LLMClient used by the benchmark.

    Supports three modes:
      - live    : call the real underlying client, record every response
      - replay  : return recorded responses; fail loudly on a miss
      - mock    : return a canned UNKNOWN-ish response for every prompt

    The recording key is SHA256(system_prompt + '|||' + user_message + '|||' + extra)
    matching the cache-key shape used by the real LLMClient.
    """

    # Per-item canned mock response (small JSON, matches expected shape)
    _MOCK_SINGLE = {
        "code": "99999999",
        "category": "UNKNOWN",
        "confidence": 0.3,
        "reasoning": "mock",
    }

    def __init__(self, mode: str, record_path: str):
        self.mode = mode
        self.record_path = record_path
        self.recordings: dict[str, str] = {}
        self.misses: list[str] = []
        self.calls: int = 0  # number of call()/call_json() invocations
        self.prompts_sent: int = 0  # cumulative number of items sent (parsed from batches)
        self._real_client = None

        if mode in ("replay", "live") and os.path.exists(record_path):
            try:
                with open(record_path, encoding="utf-8") as f:
                    self.recordings = json.load(f)
                logger.info(f"[stub] loaded {len(self.recordings)} recordings from {record_path}")
            except Exception as e:
                logger.warning(f"[stub] failed to load recordings: {e}")
                self.recordings = {}

        if mode == "live":
            # Lazily import — avoid importing if we don't need a real client
            try:
                from core.llm_client import LLMClient  # type: ignore

                self._real_client = LLMClient()
            except Exception as e:
                raise RuntimeError(
                    f"[stub] live mode requires core.llm_client.LLMClient: {e}"
                ) from e

    # ── Public API matching LLMClient ────────────────────────────────────

    def call(
        self,
        user_message: str,
        system_prompt: str = "",
        max_tokens: int = 4096,
        use_cache: bool = True,
        cache_key_extra: str = "",
    ) -> str | None:
        self.calls += 1
        # Rough item count: count numbered lines of the form "N. "
        import re as _re

        self.prompts_sent += max(
            1, len(_re.findall(r"^\s*\d+\.\s", user_message, flags=_re.MULTILINE))
        )

        key = self._cache_key(system_prompt, user_message, cache_key_extra)

        if self.mode == "replay":
            if key not in self.recordings:
                snippet = user_message[:120].replace("\n", " ")
                self.misses.append(snippet)
                raise RuntimeError(
                    f"[stub replay] no recording for key={key[:12]}…; "
                    f"prompt starts with: {snippet!r}. "
                    f"Re-run with --mode live to refresh recordings."
                )
            return self.recordings[key]

        if self.mode == "mock":
            return self._build_mock_response(user_message)

        # live: call the real client, record the response
        assert self._real_client is not None
        text = self._real_client.call(
            user_message=user_message,
            system_prompt=system_prompt,
            max_tokens=max_tokens,
            use_cache=False,  # bypass the real in-memory cache so we always hit the API
            cache_key_extra=cache_key_extra,
        )
        if text:
            self.recordings[key] = text
            self._save_recordings()
        return text

    def call_json(
        self,
        user_message: str,
        system_prompt: str = "",
        max_tokens: int = 4096,
        use_cache: bool = True,
        cache_key_extra: str = "",
    ) -> dict | None:
        text = self.call(
            user_message,
            system_prompt=system_prompt,
            max_tokens=max_tokens,
            use_cache=use_cache,
            cache_key_extra=cache_key_extra,
        )
        if not text:
            return None
        json_start = text.find("{")
        json_end = text.rfind("}") + 1
        if json_start >= 0 and json_end > json_start:
            try:
                return json.loads(text[json_start:json_end])
            except json.JSONDecodeError as e:
                logger.warning(f"[stub] JSON decode failed: {e}")
                return None
        return None

    # ── Helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _cache_key(system_prompt: str, user_message: str, extra: str = "") -> str:
        content = f"{system_prompt}|||{user_message}|||{extra}"
        return hashlib.sha256(content.encode()).hexdigest()

    def _save_recordings(self) -> None:
        os.makedirs(os.path.dirname(self.record_path), exist_ok=True)
        tmp = self.record_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.recordings, f, indent=2)
        os.replace(tmp, self.record_path)

    def _build_mock_response(self, user_message: str) -> str:
        """Return a JSON string that the batch parser can consume."""
        import re as _re

        nums = _re.findall(r"^\s*(\d+)\.\s", user_message, flags=_re.MULTILINE)
        obj = {str(n): dict(self._MOCK_SINGLE) for n in nums} if nums else dict(self._MOCK_SINGLE)
        return json.dumps(obj)


# ── Monkeypatch helpers ────────────────────────────────────────────────────


def install_stub_llm(stub: StubLLMClient) -> None:
    """
    Install the stub so BOTH the single-item and batch classification paths
    route their LLM calls through it, and so neither path hits the real web.

    Interception points:
      1. core.llm_client.get_llm_client / module-level _client — used by the
         batch classifier's primary call path (_call_llm).
      2. classifier.classify_with_llm — monkey-patched to build a single-item
         prompt and call stub.call_json() directly. This covers the single
         classifier's LLM fallback (both anthropic-SDK and urllib branches).
      3. classifier._gather_web_context and classifier._fetch_url — stubbed
         to return empty strings, so benchmark runs never hit DuckDuckGo /
         HTS.gov. The benchmark compares classifier logic, not network.
      4. classifier._search_duckduckgo / _search_hts_gov — return None.
    """
    import core.llm_client as _llm_mod  # type: ignore

    _llm_mod._client = stub  # type: ignore[attr-defined]
    _llm_mod.get_llm_client = lambda: stub  # type: ignore[assignment]

    import classifier as _cls  # type: ignore

    def _stub_classify_with_llm(description, web_results, config=None):
        prompt = _cls._build_classification_prompt(description, web_results or "")
        raw = stub.call_json(
            user_message=prompt,
            system_prompt="",
            max_tokens=200,
            use_cache=False,
            cache_key_extra="single_classify_v1",
        )
        if not raw or not isinstance(raw, dict):
            return None
        code = str(raw.get("code", "")).replace(".", "").replace(" ", "")
        if len(code) != 8 or not code.isdigit():
            return None
        base_dir = (config or {}).get("base_dir", ".")
        code = _cls.validate_and_correct_code(code, base_dir)
        confidence = raw.get("confidence", 0.75)
        if isinstance(confidence, str):
            try:
                confidence = float(confidence)
            except ValueError:
                confidence = 0.75
        if confidence < 0.4:
            return None
        return {
            "code": code,
            "category": raw.get("category", "LLM_CLASSIFIED"),
            "confidence": confidence,
            "source": "llm_classification",
            "notes": raw.get("reasoning", "Classified by LLM (stub)"),
        }

    _cls.classify_with_llm = _stub_classify_with_llm  # type: ignore[assignment]
    _cls._gather_web_context = lambda desc, cfg=None: ""  # type: ignore[assignment]
    _cls._fetch_url = lambda url, timeout=10: None  # type: ignore[assignment]
    _cls._search_duckduckgo = lambda terms: None  # type: ignore[assignment]
    _cls._search_hts_gov = lambda terms: None  # type: ignore[assignment]

    # Also patch the classifier_batch module's reference to _gather_web_context
    # (it imported it by name at module load time).
    try:
        import classifier_batch as _clsb  # type: ignore

        _clsb._gather_web_context = lambda desc, cfg=None: ""  # type: ignore[assignment]
    except Exception:
        pass

    logger.debug("[stub] installed on core.llm_client, classifier, classifier_batch")


# ── Corpus extraction ──────────────────────────────────────────────────────


def _list_pdfs(folder: str, limit: int) -> list[str]:
    if not os.path.isdir(folder):
        raise FileNotFoundError(f"PDF folder not found: {folder}")
    pdfs = sorted(
        os.path.join(folder, name)
        for name in os.listdir(folder)
        if name.lower().endswith(".pdf") and os.path.isfile(os.path.join(folder, name))
    )
    if limit and limit > 0:
        pdfs = pdfs[:limit]
    return pdfs


def _extract_text_pdfplumber(pdf_path: str) -> str:
    try:
        import pdfplumber  # type: ignore
    except ImportError:
        return ""

    try:
        with pdfplumber.open(pdf_path) as pdf:
            parts = []
            for page in pdf.pages:
                parts.append(page.extract_text() or "")
            return "\n\n".join(parts).strip()
    except Exception as e:
        logger.debug(f"pdfplumber failed on {pdf_path}: {e}")
        return ""


_registry = None  # lazy-loaded FormatRegistry


def _get_registry():
    global _registry
    if _registry is None:
        try:
            from format_registry import FormatRegistry  # type: ignore

            _registry = FormatRegistry(base_dir=_REPO_ROOT)
        except Exception as e:
            logger.warning(f"FormatRegistry load failed: {e}")
            _registry = False
    return _registry if _registry else None


def _parse_text_to_items(text: str) -> list[dict[str, Any]]:
    """
    Parse PDF text into a flat list of item dicts.

    Strategy:
      1. Try FormatRegistry's YAML-driven parsers (matches known suppliers).
      2. Fall back to parse_text_file() (legacy generic parsers).
    """
    items: list[dict[str, Any]] = []

    reg = _get_registry()
    if reg is not None:
        try:
            parser = reg.get_parser(text)
            if parser is not None:
                result = parser.parse(text)
                if isinstance(result, dict) and result.get("status") == "success":
                    for invoice in result.get("invoices", []) or []:
                        for raw in invoice.get("items", []) or []:
                            desc = (raw.get("description") or "").strip()
                            if not desc:
                                continue
                            items.append(
                                {
                                    "description": desc,
                                    "sku": raw.get("sku", ""),
                                    "quantity": raw.get("quantity", 1),
                                    "unit_cost": raw.get("unit_cost", 0),
                                    "total_cost": raw.get("total_cost", 0),
                                }
                            )
                    if items:
                        return items
        except Exception as e:
            logger.debug(f"FormatRegistry parse failed: {e}")

    # Fallback: legacy generic parser (requires temp file)
    try:
        from format_parser import parse_text_file  # type: ignore
    except ImportError:
        return items

    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, encoding="utf-8") as tf:
        tf.write(text)
        tmp_path = tf.name
    try:
        result = parse_text_file(tmp_path)
    finally:
        with contextlib.suppress(OSError):
            os.unlink(tmp_path)

    if not isinstance(result, dict) or result.get("status") != "success":
        return items

    for invoice in result.get("invoices", []) or []:
        for raw in invoice.get("items", []) or []:
            desc = (raw.get("description") or "").strip()
            if not desc:
                continue
            items.append(
                {
                    "description": desc,
                    "sku": raw.get("sku", ""),
                    "quantity": raw.get("quantity", 1),
                    "unit_cost": raw.get("unit_cost", 0),
                    "total_cost": raw.get("total_cost", 0),
                }
            )
    return items


def build_item_corpus(folder: str, limit: int, cache_path: str) -> list[dict[str, Any]]:
    """Extract items from all PDFs in folder (up to limit). Cache to cache_path."""
    cache_key = f"{os.path.abspath(folder)}|limit={limit}"
    if os.path.exists(cache_path):
        try:
            with open(cache_path, encoding="utf-8") as f:
                cached = json.load(f)
            if cached.get("key") == cache_key:
                logger.info(f"[corpus] cache hit: {len(cached.get('items', []))} items")
                return cached["items"]
        except Exception as e:
            logger.debug(f"[corpus] cache load failed: {e}")

    pdfs = _list_pdfs(folder, limit)
    logger.info(f"[corpus] extracting items from {len(pdfs)} PDFs…")

    items: list[dict[str, Any]] = []
    for i, pdf in enumerate(pdfs, 1):
        logger.info(f"[corpus] ({i}/{len(pdfs)}) {os.path.basename(pdf)}")
        text = _extract_text_pdfplumber(pdf)
        if not text:
            continue
        parsed = _parse_text_to_items(text)
        for it in parsed:
            it["_source_pdf"] = os.path.basename(pdf)
        items.extend(parsed)

    os.makedirs(os.path.dirname(cache_path) or ".", exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump({"key": cache_key, "items": items}, f, indent=2)
    logger.info(f"[corpus] extracted {len(items)} items → {cache_path}")
    return items


# ── Ground truth ───────────────────────────────────────────────────────────


def _load_ground_truth(path: str) -> dict[str, str]:
    """Return {normalized_desc: 8-digit code} from assessed_classifications.json."""
    if not os.path.exists(path):
        logger.warning(f"[ground-truth] not found: {path}")
        return {}
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    entries = data.get("entries", {}) or {}
    out: dict[str, str] = {}
    for desc_norm, entry in entries.items():
        if desc_norm == "_metadata":
            continue
        code = entry.get("code") if isinstance(entry, dict) else None
        if code and len(code) == 8:
            out[desc_norm] = code
    return out


def _normalize_for_lookup(desc: str) -> str:
    """Match classifier._normalize_for_assessed."""
    from classifier import _normalize_for_assessed  # type: ignore

    return _normalize_for_assessed(desc)


# ── Benchmark core ─────────────────────────────────────────────────────────


@dataclass
class ItemResult:
    description: str
    code: str
    source: str
    latency_ms: float = 0.0


@dataclass
class RunStats:
    name: str
    total_items: int = 0
    wall_clock_sec: float = 0.0
    llm_calls: int = 0
    llm_items_sent: int = 0
    results: list[ItemResult] = field(default_factory=list)
    source_counts: dict[str, int] = field(default_factory=dict)


def _load_rules(base_dir: str) -> tuple[list[dict], set]:
    path = os.path.join(base_dir, "rules", "classification_rules.json")
    if not os.path.exists(path):
        return [], set()
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    rules = sorted(data.get("rules", []), key=lambda r: r.get("priority", 0), reverse=True)
    noise = set(data.get("word_analysis", {}).get("noise_words", []))
    return rules, noise


def run_single_pass(
    items: list[dict[str, Any]],
    base_dir: str,
    stub: StubLLMClient,
) -> RunStats:
    """Classify items one-at-a-time via classify_item() + LLM fallback."""
    from classifier import (  # type: ignore
        classify_item,
        lookup_hs_code_web,
    )

    rules, noise = _load_rules(base_dir)
    stats = RunStats(name="single")
    stats.total_items = len(items)
    llm_calls_start = stub.calls
    llm_items_start = stub.prompts_sent

    t_start = time.perf_counter()
    for it in items:
        desc = it.get("description", "")
        if not desc:
            stats.results.append(ItemResult(desc, "UNKNOWN", "none"))
            stats.source_counts["none"] = stats.source_counts.get("none", 0) + 1
            continue

        item_start = time.perf_counter()
        match = classify_item(desc, rules, noise, base_dir)
        if match and match.get("code") and match["code"] != "UNKNOWN":
            code = match["code"]
            src = match.get("source") or "rule"
        else:
            web_match = lookup_hs_code_web(
                desc, config={"base_dir": base_dir, "llm_classification": {"enabled": True}}
            )
            if web_match and web_match.get("code") and web_match["code"] != "UNKNOWN":
                code = web_match["code"]
                src = web_match.get("source", "web_or_llm")
            else:
                code = "UNKNOWN"
                src = "none"
        latency = (time.perf_counter() - item_start) * 1000
        stats.results.append(ItemResult(desc, code, src, latency))
        stats.source_counts[src] = stats.source_counts.get(src, 0) + 1

    stats.wall_clock_sec = time.perf_counter() - t_start
    stats.llm_calls = stub.calls - llm_calls_start
    stats.llm_items_sent = stub.prompts_sent - llm_items_start
    return stats


def run_batch_pass(
    items: list[dict[str, Any]],
    base_dir: str,
    stub: StubLLMClient,
) -> RunStats:
    """Classify items via classify_items_batch()."""
    from classifier_batch import classify_items_batch  # type: ignore

    rules, noise = _load_rules(base_dir)
    stats = RunStats(name="batch")
    stats.total_items = len(items)
    llm_calls_start = stub.calls
    llm_items_start = stub.prompts_sent

    t_start = time.perf_counter()
    results = classify_items_batch(
        items,
        rules=rules,
        noise_words=noise,
        config={"base_dir": base_dir},
    )
    stats.wall_clock_sec = time.perf_counter() - t_start

    for it, res in zip(items, results, strict=False):
        desc = it.get("description", "")
        code = (res or {}).get("code") or "UNKNOWN"
        # classify_item() rule-matches don't set a 'source' key; infer it
        # from the presence of a rule_id so the layer-hit breakdown is
        # comparable to the single-classifier path.
        if res and res.get("source"):
            src = res["source"]
        elif res and res.get("rule_id"):
            src = "rule"
        elif code == "UNKNOWN":
            src = "none"
        else:
            src = "unknown_source"
        stats.results.append(ItemResult(desc, code, src))
        stats.source_counts[src] = stats.source_counts.get(src, 0) + 1

    stats.llm_calls = stub.calls - llm_calls_start
    stats.llm_items_sent = stub.prompts_sent - llm_items_start
    return stats


# ── Reporting ──────────────────────────────────────────────────────────────


def _accuracy(stats: RunStats, gt: dict[str, str]) -> dict[str, float]:
    total = 0
    exact = six = four = 0
    for r in stats.results:
        key = _normalize_for_lookup(r.description)
        truth = gt.get(key)
        if not truth:
            continue
        total += 1
        if r.code == truth:
            exact += 1
        if r.code[:6] == truth[:6]:
            six += 1
        if r.code[:4] == truth[:4]:
            four += 1
    if total == 0:
        return {"covered": 0, "exact": 0.0, "six": 0.0, "four": 0.0}
    return {
        "covered": total,
        "exact": exact / total,
        "six": six / total,
        "four": four / total,
    }


def build_markdown_report(
    single: RunStats,
    batch: RunStats,
    gt: dict[str, str],
    corpus_info: dict[str, Any],
    stub: StubLLMClient,
) -> str:
    acc_single = _accuracy(single, gt)
    acc_batch = _accuracy(batch, gt)

    unique_descriptions = len({r.description for r in single.results})
    disagreements: list[tuple[str, str, str]] = []
    for s, b in zip(single.results, batch.results, strict=False):
        if s.code != b.code:
            disagreements.append((s.description, s.code, b.code))

    lines: list[str] = []
    lines.append("# Classifier Benchmark Report")
    lines.append("")
    lines.append(f"- Corpus folder : `{corpus_info.get('folder', '?')}`")
    lines.append(f"- PDFs          : {corpus_info.get('pdf_count', '?')}")
    lines.append(f"- Total items   : {single.total_items}")
    lines.append(f"- Unique descs  : {unique_descriptions}")
    lines.append(f"- Mode          : {stub.mode}")
    lines.append("")

    lines.append("## Runtime summary")
    lines.append("")
    lines.append("| metric | single | batch |")
    lines.append("|---|---:|---:|")
    lines.append(f"| wall clock (sec) | {single.wall_clock_sec:.2f} | {batch.wall_clock_sec:.2f} |")
    lines.append(f"| LLM calls | {single.llm_calls} | {batch.llm_calls} |")
    lines.append(f"| LLM item-slots sent | {single.llm_items_sent} | {batch.llm_items_sent} |")
    lines.append("")

    lines.append("## Accuracy (ground-truth assessed_classifications.json)")
    lines.append("")
    lines.append("| metric | single | batch |")
    lines.append("|---|---:|---:|")
    lines.append(f"| covered items | {int(acc_single['covered'])} | {int(acc_batch['covered'])} |")
    lines.append(
        f"| @ 8-digit | {acc_single['exact'] * 100:.1f}% | {acc_batch['exact'] * 100:.1f}% |"
    )
    lines.append(f"| @ 6-digit | {acc_single['six'] * 100:.1f}% | {acc_batch['six'] * 100:.1f}% |")
    lines.append(
        f"| @ 4-digit | {acc_single['four'] * 100:.1f}% | {acc_batch['four'] * 100:.1f}% |"
    )
    lines.append("")

    lines.append("## Layer-hit breakdown")
    lines.append("")
    lines.append("| source | single | batch |")
    lines.append("|---|---:|---:|")
    all_sources = sorted(set(single.source_counts.keys()) | set(batch.source_counts.keys()))
    for src in all_sources:
        lines.append(
            f"| {src} | {single.source_counts.get(src, 0)} | {batch.source_counts.get(src, 0)} |"
        )
    lines.append("")

    lines.append(f"## Single vs Batch disagreements: {len(disagreements)}")
    lines.append("")
    if disagreements:
        lines.append("| # | description | single | batch |")
        lines.append("|---|---|---|---|")
        for i, (d, s, b) in enumerate(disagreements[:10], 1):
            lines.append(f"| {i} | {d[:60]} | {s} | {b} |")
    lines.append("")

    return "\n".join(lines)


# ── CLI ────────────────────────────────────────────────────────────────────


def _default_folder() -> str:
    """
    Resolve the PDF corpus folder.

    Priority:
      1. $AUTOINVOICE_DOWNLOADS (explicit env var)
      2. $AUTOINVOICE_BENCH_CORPUS (benchmark-specific override)
      3. /mnt/d/OneDrive/Clients/WebSource/Downloads (the 123-PDF production corpus)

    Note: the Downloads corpus is mostly scanned image PDFs that require OCR.
    pdfplumber-only extraction may yield few items; run full pipeline OCR for
    production benchmarks. For development, the workspace CONTAINER DOCUMENTS
    folder contains text-based PDFs that extract cleanly.
    """
    return (
        os.environ.get("AUTOINVOICE_DOWNLOADS")
        or os.environ.get("AUTOINVOICE_BENCH_CORPUS")
        or "/mnt/d/OneDrive/Clients/WebSource/Downloads"
    )


def _default_record_path() -> str:
    return os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "bench_classifier_recordings.json"
    )


def _default_items_cache() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "bench_classifier_items.json")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Classifier benchmark harness")
    parser.add_argument("--folder", default=_default_folder())
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="PDFs to process (default 5; pass 0 for unlimited)",
    )
    parser.add_argument("--mode", choices=["live", "replay", "mock"], default="replay")
    parser.add_argument("--record-path", default=_default_record_path())
    parser.add_argument("--items-cache", default=_default_items_cache())
    parser.add_argument("--output", default=None)
    parser.add_argument(
        "--ground-truth",
        default=os.path.join(_REPO_ROOT, "data", "assessed_classifications.json"),
    )
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    # ── corpus ────────────────────────────────────────────────────────
    items = build_item_corpus(args.folder, args.limit, args.items_cache)
    if not items:
        print("No items extracted — check --folder and pdfplumber install", file=sys.stderr)
        return 2

    corpus_info = {
        "folder": args.folder,
        "pdf_count": len({it.get("_source_pdf", "") for it in items}),
    }

    # ── stub ──────────────────────────────────────────────────────────
    stub = StubLLMClient(args.mode, args.record_path)
    install_stub_llm(stub)

    # Also clear classifier module caches so runs are deterministic
    try:
        import classifier as _cls  # type: ignore

        _cls._assessed_classifications = None  # type: ignore[attr-defined]
        _cls._cet_valid_codes = None  # type: ignore[attr-defined]
        _cls._invalid_codes_from_file = None  # type: ignore[attr-defined]
    except Exception:
        pass

    # ── single pass ───────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("Running SINGLE classifier pass…")
    single_stats = run_single_pass(items, _REPO_ROOT, stub)
    logger.info(
        f"single: {single_stats.wall_clock_sec:.2f}s, "
        f"{single_stats.llm_calls} LLM calls, "
        f"{single_stats.llm_items_sent} item-slots"
    )

    # ── batch pass ────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("Running BATCH classifier pass…")
    batch_stats = run_batch_pass(items, _REPO_ROOT, stub)
    logger.info(
        f"batch: {batch_stats.wall_clock_sec:.2f}s, "
        f"{batch_stats.llm_calls} LLM calls, "
        f"{batch_stats.llm_items_sent} item-slots"
    )

    # ── report ────────────────────────────────────────────────────────
    gt = _load_ground_truth(args.ground_truth)
    report = build_markdown_report(single_stats, batch_stats, gt, corpus_info, stub)

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(report)
        logger.info(f"Report written to {args.output}")
    else:
        print(report)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
