"""Phase 5 — unified hybrid OCR system.

Tests for ``pipeline/multi_ocr.py``, the single OCR pathway that replaces
the five ad-hoc chains scattered across text_extractor.py, pdf_splitter.py,
pdf_extractor.py, bl_parser.py, workflow/batch.py, and supplier_resolver.py.

The module runs every available combination of (preprocessing × OCR engine)
and reconciles the outputs into a consensus text plus a token map for
field-level recovery. These tests guard:

* the preprocessing variants (upscale, CLAHE+Otsu, adaptive threshold,
  sharpen+contrast) produce the expected shape/binarization,
* engine wrappers gracefully degrade when their backing package is
  missing (pdfplumber, pytesseract, paddleocr, vision API),
* the fuzzy line-alignment clusters similar lines across variants and
  drops unsupported skeleton lines ("hallucinations"),
* per-token voting in ``consensus_line`` picks the majority token,
  honors engine priority on ties, and fires the digit-wins-in-numeric-
  slot rule that fixes the ANDREA_L "SHEIN ... + 8.19" → "1 8.19" case,
* the token map collects every (variant_id, line) pair that saw a
  given price so downstream parsers can recover orphan prices,
* ``extract_text`` short-circuits on clean digital PDFs via pdfplumber
  (no OCR at all),
* the per-PDF-sha1 cache roundtrips correctly and is invalidated when
  ``CONFIG_VERSION`` bumps.
"""

from __future__ import annotations

import os
import sys
from types import SimpleNamespace

import numpy as np
import pytest

_PIPELINE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "pipeline"))
if _PIPELINE_DIR not in sys.path:
    sys.path.insert(0, _PIPELINE_DIR)

import multi_ocr  # noqa: E402

# ─── Fixtures ──────────────────────────────────────────────────────────


def _solid_rgb(h: int = 100, w: int = 100, color: int = 200) -> np.ndarray:
    return np.full((h, w, 3), color, dtype=np.uint8)


def _gradient_rgb(h: int = 100, w: int = 100) -> np.ndarray:
    img = np.zeros((h, w, 3), dtype=np.uint8)
    for x in range(w):
        img[:, x, :] = int(255 * x / max(w - 1, 1))
    return img


# ─── Preprocessing variants ───────────────────────────────────────────


def test_preprocess_original_returns_same_image():
    img = _solid_rgb()
    out = multi_ocr.preprocess_original(img)
    assert out.shape == img.shape
    assert np.array_equal(out, img)


def test_preprocess_upscale_2x_doubles_dimensions():
    img = _solid_rgb(100, 150)
    out = multi_ocr.preprocess_upscale_2x(img)
    # Graceful degrade: if cv2 missing, returns unchanged. Otherwise 2x.
    if out.shape == img.shape:
        pytest.skip("cv2 unavailable; preprocess_upscale_2x is a no-op")
    assert out.shape[0] == 200
    assert out.shape[1] == 300


def test_preprocess_clahe_otsu_returns_binary_or_noop():
    img = _gradient_rgb()
    out = multi_ocr.preprocess_clahe_otsu(img)
    if out.ndim == 3:
        pytest.skip("cv2 unavailable; preprocess_clahe_otsu is a no-op")
    assert out.ndim == 2
    uniq = set(np.unique(out).tolist())
    assert uniq.issubset({0, 255})


def test_preprocess_adaptive_gaussian_returns_binary_or_noop():
    img = _gradient_rgb()
    out = multi_ocr.preprocess_adaptive_gaussian(img)
    if out.ndim == 3:
        pytest.skip("cv2 unavailable; preprocess_adaptive_gaussian is a no-op")
    assert out.ndim == 2
    uniq = set(np.unique(out).tolist())
    assert uniq.issubset({0, 255})


def test_preprocess_sharpen_contrast_preserves_color_or_noop():
    img = _solid_rgb()
    out = multi_ocr.preprocess_sharpen_contrast(img)
    # Either Pillow ran and color channels preserved, or PIL missing -> noop
    assert out.ndim == 3
    assert out.shape[2] == 3


# ─── Engine wrappers ──────────────────────────────────────────────────


def test_engine_pdfplumber_extracts_digital_text(monkeypatch, tmp_path):
    fake_text = "SHEIN Clasi Plus Polka Dot Skirt 1 $8.19\nGrand Total: 67.80"

    class _FakePdf:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        pages = [SimpleNamespace(extract_text=lambda: fake_text)]

    class _FakePdfplumber:
        @staticmethod
        def open(path):
            return _FakePdf()

    monkeypatch.setitem(sys.modules, "pdfplumber", _FakePdfplumber)
    out = multi_ocr.engine_pdfplumber(str(tmp_path / "anything.pdf"))
    assert "SHEIN Clasi Plus" in out
    assert "8.19" in out


def test_engine_pdfplumber_returns_empty_on_missing_package(monkeypatch):
    monkeypatch.setitem(sys.modules, "pdfplumber", None)
    out = multi_ocr.engine_pdfplumber("/nonexistent.pdf")
    assert out == ""


def test_engine_tesseract_passes_psm_argument(monkeypatch):
    calls = []

    def _fake_image_to_string(pil_img, lang=None, config=None):
        calls.append({"lang": lang, "config": config})
        return "hello world"

    fake_pt = SimpleNamespace(
        image_to_string=_fake_image_to_string,
        pytesseract=SimpleNamespace(tesseract_cmd="/fake/tesseract"),
    )
    monkeypatch.setitem(sys.modules, "pytesseract", fake_pt)

    out = multi_ocr.engine_tesseract(_solid_rgb(), psm=6)
    assert "hello world" in out
    assert "--psm 6" in calls[0]["config"]

    multi_ocr.engine_tesseract(_solid_rgb(), psm=11)
    assert "--psm 11" in calls[-1]["config"]


def test_engine_tesseract_returns_empty_when_package_missing(monkeypatch):
    monkeypatch.setitem(sys.modules, "pytesseract", None)
    out = multi_ocr.engine_tesseract(_solid_rgb(), psm=6)
    assert out == ""


def test_engine_paddleocr_returns_empty_when_package_missing(monkeypatch):
    monkeypatch.setitem(sys.modules, "paddleocr", None)
    out = multi_ocr.engine_paddleocr(_solid_rgb())
    assert out == ""


def test_engine_vision_api_skipped_without_api_key():
    out = multi_ocr.engine_vision_api(_solid_rgb(), api_key="", base_url="https://fake", model="x")
    assert out == ""


# ─── Consensus: line alignment ────────────────────────────────────────


def test_align_lines_clusters_similar_lines_across_variants():
    variant_texts = {
        "v1": "SHEIN Skirt 1 8.19\nGrand Total 67.80",
        "v2": "SHEIN Skirt 1 8.19\nGrand Total 67.80",
        "v3": "SHEIN Skirt + 8.19\nGrand Total 67.80",
    }
    clusters = multi_ocr.align_lines(variant_texts)
    assert len(clusters) == 2
    assert all(len(c) == 3 for c in clusters)
    first_ids = {vid for vid, _ in clusters[0]}
    assert first_ids == {"v1", "v2", "v3"}


def test_align_lines_drops_unsupported_skeleton_lines():
    variant_texts = {
        "v1": "Line A\nHallucinated noise line\nLine C",
        "v2": "Line A\nLine C",
        "v3": "Line A\nLine C",
    }
    clusters = multi_ocr.align_lines(variant_texts)
    text = "\n".join(multi_ocr.consensus_line(c) for c in clusters)
    assert "Hallucinated" not in text
    assert "Line A" in text
    assert "Line C" in text


def test_align_lines_keeps_single_variant_lines():
    variant_texts = {"solo": "Line A\nLine B"}
    clusters = multi_ocr.align_lines(variant_texts)
    assert len(clusters) == 2


# ─── Consensus: per-token voting ──────────────────────────────────────


def test_consensus_line_picks_majority_token():
    cluster = [
        ("v1", "SHEIN Skirt 1 8.19"),
        ("v2", "SHEIN Skirt 1 8.19"),
        ("v3", "SHEIN Skirt 1 8.19"),
    ]
    out = multi_ocr.consensus_line(cluster)
    assert "SHEIN" in out
    assert "Skirt" in out
    assert "1" in out
    assert "8.19" in out


def test_consensus_line_digit_wins_over_symbol_majority():
    cluster = [
        ("v1", "SHEIN Skirt 1 8.19"),
        ("v2", "SHEIN Skirt 1 8.19"),
        ("v3", "SHEIN Skirt + 8.19"),
        ("v4", "SHEIN Skirt + 8.19"),
        ("v5", "SHEIN Skirt 1 8.19"),
    ]
    out = multi_ocr.consensus_line(cluster)
    # 3/5 say "1", 2/5 say "+" — plain majority gives "1"
    assert "1" in out.split()
    assert "+" not in out.split()


def test_consensus_line_digit_wins_even_when_minority():
    """Adversarial: only 1/4 variants see the digit, but its slot is numeric
    (the next token is 8.19), so the digit-priority-in-numeric-slot rule
    must override the plain majority."""
    cluster = [
        ("v1", "Skirt 1 8.19"),
        ("v2", "Skirt + 8.19"),
        ("v3", "Skirt + 8.19"),
        ("v4", "Skirt + 8.19"),
    ]
    out = multi_ocr.consensus_line(cluster)
    assert "1" in out.split()
    assert "+" not in out.split()


def test_consensus_line_engine_priority_breaks_ties():
    # Vision API (100) outranks PaddleOCR (60); 1-vs-1 tie, vision wins
    cluster = [
        ("original+paddleocr", "SHEIN Skirt 2 8.19"),
        ("original+vision_api", "SHEIN Skirt 1 8.19"),
    ]
    out = multi_ocr.consensus_line(cluster)
    tokens = out.split()
    assert "1" in tokens
    assert "2" not in tokens


def test_consensus_line_single_entry_returns_original():
    cluster = [("v1", "only line here")]
    out = multi_ocr.consensus_line(cluster)
    assert "only" in out
    assert "line" in out


# ─── Token map ────────────────────────────────────────────────────────


def test_build_token_map_collects_all_price_contexts():
    variant_texts = {
        "v1": "SHEIN Skirt 1 8.19\nTotal 67.80",
        "v2": "SHEIN Skirt + 8.19\nTotal 67.80",
        "v3": "SHEIN Skirt 1 $8.19\nTotal $67.80",
    }
    token_map = multi_ocr.build_token_map(variant_texts)
    assert "8.19" in token_map
    variant_ids = {vid for vid, _ in token_map["8.19"]}
    assert variant_ids == {"v1", "v2", "v3"}
    assert "67.80" in token_map


def test_find_token_contexts_helper():
    variant_texts = {
        "v1": "Skirt 1 8.19",
        "v2": "Skirt + 8.19",
    }
    token_map = multi_ocr.build_token_map(variant_texts)
    ctx = multi_ocr.find_token_contexts(token_map, "8.19")
    assert len(ctx) == 2
    assert multi_ocr.find_token_contexts(token_map, "99.99") == []


# ─── build_consensus orchestration ────────────────────────────────────


def test_build_consensus_end_to_end():
    variant_texts = {
        "upscale_2x+tesseract_psm6": "SHEIN Skirt 1 8.19\nGrand Total 67.80",
        "clahe_otsu+tesseract_psm6": "SHEIN Skirt 1 8.19\nGrand Total 67.80",
        "original+paddleocr": "SHEIN Skirt + 8.19\nGrand Total 67.80",
    }
    result = multi_ocr.build_consensus(variant_texts)
    assert isinstance(result, multi_ocr.MultiOcrResult)
    assert "SHEIN Skirt" in result.text
    assert "8.19" in result.text
    assert result.confidence > 0
    assert "8.19" in result.token_map
    assert len(result.token_map["8.19"]) == 3
    assert set(result.variants.keys()) == set(variant_texts.keys())


def test_build_consensus_handles_empty_input():
    result = multi_ocr.build_consensus({})
    assert result.text == ""
    assert result.variants == {}
    assert result.confidence == 0.0


# ─── extract_text short-circuit ───────────────────────────────────────


def test_extract_text_short_circuits_on_clean_digital_pdf(tmp_path, monkeypatch):
    clean_text = (
        "SHEIN US Services, LLC\n"
        "Invoice No.: INVUS 202407280025 22187\n"
        "SHEIN Skirt 1 $8.19\n"
        "Grand Total: 67.80"
    )

    class _FakePdf:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        pages = [SimpleNamespace(extract_text=lambda: clean_text)]

    class _FakePdfplumber:
        @staticmethod
        def open(path):
            return _FakePdf()

    monkeypatch.setitem(sys.modules, "pdfplumber", _FakePdfplumber)

    fake_pdf = tmp_path / "digital.pdf"
    fake_pdf.write_bytes(b"%PDF-1.4 fake content")

    result = multi_ocr.extract_text(str(fake_pdf), use_cache=False, base_dir=str(tmp_path))
    assert result.engine_used == "pdfplumber"
    assert "SHEIN" in result.text
    assert "8.19" in result.text
    assert result.confidence == 1.0


def test_extract_text_missing_file_returns_empty(tmp_path):
    result = multi_ocr.extract_text(
        str(tmp_path / "does_not_exist.pdf"),
        use_cache=False,
        base_dir=str(tmp_path),
    )
    assert result.text == ""
    assert result.engine_used == "none"


# ─── Cache ────────────────────────────────────────────────────────────


def test_cache_roundtrip(tmp_path):
    original = multi_ocr.MultiOcrResult(
        text="SHEIN Skirt 1 8.19",
        variants={"v1": "SHEIN Skirt 1 8.19"},
        token_map={"8.19": [("v1", "SHEIN Skirt 1 8.19")]},
        confidence=0.87,
        method_stats={"v1": 18},
        page_count=1,
        engine_used="hybrid(v1)",
    )
    multi_ocr.cache_save("abc123", original, base_dir=str(tmp_path))
    loaded = multi_ocr.cache_load("abc123", base_dir=str(tmp_path))
    assert loaded is not None
    assert loaded.text == original.text
    assert loaded.variants == original.variants
    # token_map tuples may become lists after JSON; compare normalized
    assert list(loaded.token_map.keys()) == list(original.token_map.keys())
    assert loaded.token_map["8.19"][0][0] == "v1"
    assert loaded.confidence == pytest.approx(0.87)


def test_cache_load_missing_returns_none(tmp_path):
    assert multi_ocr.cache_load("nonexistent_hash", base_dir=str(tmp_path)) is None


def test_cache_rejects_stale_config_version(tmp_path, monkeypatch):
    result = multi_ocr.MultiOcrResult(
        text="x",
        variants={},
        token_map={},
        confidence=1.0,
        method_stats={},
        page_count=1,
        engine_used="pdfplumber",
    )
    monkeypatch.setattr(multi_ocr, "CONFIG_VERSION", "v-old")
    multi_ocr.cache_save("hash1", result, base_dir=str(tmp_path))

    monkeypatch.setattr(multi_ocr, "CONFIG_VERSION", "v-new")
    loaded = multi_ocr.cache_load("hash1", base_dir=str(tmp_path))
    assert loaded is None


# ─── ANDREA_L regression: "+" / "4-" → digit recovery ─────────────────


def test_andrea_regression_plus_becomes_one_in_consensus():
    """Real invoice GSUNJG55T00QV70 had one variant (PaddleOCR) read the
    quantity column as '+', while Tesseract with CLAHE+Otsu preprocessing
    read it cleanly as '1'. The consensus must produce '1 8.19'."""
    variant_texts = {
        "upscale_2x+tesseract_psm6": "SHEIN Clasi Plus Polka Skirt 1 8.19",
        "clahe_otsu+tesseract_psm6": "SHEIN Clasi Plus Polka Skirt 1 8.19",
        "original+paddleocr": "SHEIN Clasi Plus Polka Skirt + 8.19",
    }
    result = multi_ocr.build_consensus(variant_texts)
    tokens = result.text.split()
    assert "1" in tokens
    assert "+" not in tokens
    assert "8.19" in tokens
    # Token map preserves every variant that saw the price
    assert len(result.token_map["8.19"]) == 3


def test_andrea_regression_dash_suffix_quantity():
    """Another ANDREA_L variant: PaddleOCR produced '4- 6.57' instead of
    '1 6.57'. The digit-priority rule must pick the '1' from the variants
    that got it right."""
    variant_texts = {
        "clahe_otsu+tesseract_psm6": "SHEIN Clasi Plus Geo Pencil Skirt 1 6.57",
        "upscale_2x+tesseract_psm6": "SHEIN Clasi Plus Geo Pencil Skirt 1 6.57",
        "original+paddleocr": "SHEIN Clasi Plus Geo Pencil Skirt 4- 6.57",
    }
    result = multi_ocr.build_consensus(variant_texts)
    tokens = result.text.split()
    assert "1" in tokens
    assert "4-" not in tokens
    assert "6.57" in tokens
