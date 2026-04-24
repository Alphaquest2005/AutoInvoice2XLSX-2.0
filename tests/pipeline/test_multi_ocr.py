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


# ─── Vision-authoritative short-circuit (H&M 1504495444 regression) ────
#
# Background: on the H&M receipt 1504495444.pdf the cached OCR had 7
# tesseract variants agree on mangled SKUs ("Heaite", "Fe ise", "Sis
# 002008", "ff eee") while glm_ocr read every 13-digit ART.NO cleanly.
# Token-level majority voting inherited the tesseract garbage and threw
# away glm_ocr's correct reads. The vision-authoritative path ensures
# that whenever a purpose-built vision OCR engine produced a non-trivial
# structured result, its text is used verbatim.


def _hm_receipt_variants():
    """Shared fixture: mimics the real cache observed on 1504495444.pdf.

    glm_ocr reads the full 13-digit SKUs cleanly; every tesseract variant
    produces an identical mangled prefix column. The M/56 Beige Straw Hat
    line also diverges on price ($8.99 correct vs $6.99 tesseract-only).
    """
    glm_text = (
        "ORDER SUMMARY\n"
        "ART.NO\tDESC.\tSIZE\tCOLOUR\tQTY.\tUNIT PRICE\tDISCOUNT\tTOTAL PRICE\n"
        "1206305006007\tFlared Twill Pants\t12\tDark gray\t1\t$20.99\t-$13.00\t$7.99\n"
        "1215205001003\tStraw Hat\tM/56\tBeige\t1\t$19.99\t-$11.00\t$8.99\n"
        "1206305005007\tFlared Twill Pants\t12\tLight gray\t1\t$20.99\t-$13.00\t$7.99\n"
        "1057731002004\tStraw Hat\tL/58\tLight beige\t1\t$17.99\t-$8.00\t$9.99\n"
        "1229647001004\tFrayed-edge Straw Hat\tL/58\tLight beige\t1\t$12.99\t-$7.00\t$5.99\n"
        "1187610004005\tTwist-detail Shirt Dress\tL\tYellow/tie-dye\t1\t$29.99\t-$17.00\t$12.99\n"
        "1073337004001\tPendant Necklace\tNOSIZE\tGold-colored\t1\t$5.99\t-$2.00\t$3.99\n"
        "SUBTOTAL: $57.93\nTOTAL: $67.98"
    )
    mangled_tess = (
        "Heaite Flared Twill Pants 12 Dark gray 1 $20.99 -$13.00 $7.99\n"
        "Fe ise Straw Hat M56 Beige 1 $19.99 -$11.00 $6.99\n"
        "1206305005007 Flared Twill Pants 12 Light gray 1 $20.99 -$13.00 $7.99\n"
        "Sis 002008 Straw Hat U58 Light beige 1 $17.99 -$8.00 $9.99\n"
        "1229647001004 Frayed-edge Straw Hat OG hs Light beige 1 $1299 -$7.00 $5.99\n"
        "_AMfe7610004005 Twist-detail Shirt Dress L Yellowhie-dye 1 $29.99 -$17.00 $12.99\n"
        "ff eee Pendant Necklace NOSIZE Gold- 1 $5.99 -$2.00 $3.99\n"
        "SUBTOTAL: $57.93\nTOTAL: $67.98"
    )
    return {
        "original+glm_ocr": glm_text,
        "original+tesseract_psm6": mangled_tess,
        "upscale_2x+tesseract_psm6": mangled_tess,
        "clahe_otsu+tesseract_psm6": mangled_tess,
        "adaptive_gaussian+tesseract_psm6": mangled_tess,
        "upscale_2x+tesseract_psm4": mangled_tess,
        "clahe_otsu+tesseract_psm4": mangled_tess,
    }


def test_build_consensus_vision_authoritative_preserves_full_skus():
    """The H&M 1504495444 reference case: glm_ocr's clean 13-digit SKUs
    must survive unmodified even when 6 tesseract variants outvote it
    with mangled alphanumeric hallucinations."""
    variants = _hm_receipt_variants()
    result = multi_ocr.build_consensus(variants)
    # Full 13-digit SKUs intact from glm_ocr
    for sku in (
        "1206305006007",
        "1215205001003",
        "1057731002004",
        "1229647001004",
        "1187610004005",
        "1073337004001",
    ):
        assert sku in result.text, f"missing SKU {sku}"
    # Tesseract garbage does NOT leak into consensus text
    for garbage in ("Heaite", "Fe ise", "Sis 002008", "_AMfe7610004005", "ff eee"):
        assert garbage not in result.text, f"garbage {garbage!r} leaked in"
    # engine_used labels the authoritative base
    assert result.engine_used == "authoritative(original+glm_ocr)"
    assert result.confidence == 1.0


def test_build_consensus_vision_authoritative_picks_correct_price():
    """The M/56 Beige Straw Hat disagreement — glm_ocr says $8.99
    (reconciles with $57.93 subtotal), 6 tesseract variants say $6.99.
    Authoritative path keeps $8.99; $6.99 must not appear on that row."""
    variants = _hm_receipt_variants()
    result = multi_ocr.build_consensus(variants)
    # Authoritative text is the glm_ocr base — its M/56 row has $8.99.
    m56_lines = [ln for ln in result.text.splitlines() if "M/56" in ln or "M56" in ln]
    assert m56_lines, "M/56 Beige row missing"
    assert any("$8.99" in ln for ln in m56_lines)
    # The tesseract-only $6.99 is never on the authoritative row
    for line in m56_lines:
        assert "$6.99" not in line


def test_build_consensus_vision_authoritative_still_populates_token_map():
    """Authoritative mode keeps the full cross-variant token_map so
    downstream parsers can still cross-reference any engine's reads."""
    variants = _hm_receipt_variants()
    result = multi_ocr.build_consensus(variants)
    # $6.99 only comes from tesseract variants — token_map must still
    # record those sightings even though the consensus text ignores them
    assert "6.99" in result.token_map
    engines = {vid for vid, _ in result.token_map["6.99"]}
    assert any("tesseract" in e for e in engines)
    # And $8.99 should be sourced from glm_ocr
    assert "8.99" in result.token_map
    assert any("glm_ocr" in vid for vid, _ in result.token_map["8.99"])


def test_build_consensus_skips_authoritative_when_no_vision_engine():
    """Without any vision engine, the normal hybrid consensus path runs."""
    variant_texts = {
        "upscale_2x+tesseract_psm6": "SHEIN Skirt 1 $8.19\nGrand Total $67.80",
        "clahe_otsu+tesseract_psm6": "SHEIN Skirt 1 $8.19\nGrand Total $67.80",
        "original+paddleocr": "SHEIN Skirt + $8.19\nGrand Total $67.80",
    }
    result = multi_ocr.build_consensus(variant_texts)
    assert not result.engine_used.startswith("authoritative(")
    assert result.engine_used.startswith("hybrid(")
    assert "8.19" in result.text


def test_build_consensus_skips_authoritative_when_vision_empty():
    """An empty glm_ocr output (API failure) must NOT trigger the
    authoritative path — the short-circuit gate rejects it and we
    fall through to the normal consensus code path."""
    variant_texts = {
        "original+glm_ocr": "",
        "upscale_2x+tesseract_psm6": "SHEIN Skirt 1 $8.19\nGrand Total $67.80",
        "clahe_otsu+tesseract_psm6": "SHEIN Skirt 1 $8.19\nGrand Total $67.80",
    }
    result = multi_ocr.build_consensus(variant_texts)
    assert not result.engine_used.startswith("authoritative(")
    # The _vision_authoritative_pick gate rejected the empty variant
    assert multi_ocr._vision_authoritative_pick(variant_texts) is None


def test_build_consensus_skips_authoritative_when_vision_too_short():
    """Short vision output (e.g. error message with no prices) must not
    trigger authoritative mode."""
    variant_texts = {
        "original+glm_ocr": "Error: unable to parse.",  # <100 chars, 0 prices
        "upscale_2x+tesseract_psm6": (
            "SHEIN Skirt 1 $8.19\nGrand Total $67.80\nShipping $5.00\nTax $1.00"
        ),
        "clahe_otsu+tesseract_psm6": (
            "SHEIN Skirt 1 $8.19\nGrand Total $67.80\nShipping $5.00\nTax $1.00"
        ),
    }
    result = multi_ocr.build_consensus(variant_texts)
    assert not result.engine_used.startswith("authoritative(")


def test_build_consensus_skips_authoritative_when_vision_lacks_prices():
    """Vision output that is long but has <3 prices is not trustworthy
    as a structured read — fall through to normal consensus."""
    long_prose = "This is a long paragraph without any numeric pricing. " * 5
    variant_texts = {
        "original+glm_ocr": long_prose + " Total cost was $5.00.",  # only 1 price
        "upscale_2x+tesseract_psm6": "SHEIN $8.19\nTotal $67.80\nTax $5.00\nMore $3.00",
        "clahe_otsu+tesseract_psm6": "SHEIN $8.19\nTotal $67.80\nTax $5.00\nMore $3.00",
    }
    result = multi_ocr.build_consensus(variant_texts)
    assert not result.engine_used.startswith("authoritative(")


def test_vision_authoritative_prefers_glm_ocr_over_vision_api():
    """glm_ocr (priority 110) must beat vision_api (priority 100) when
    both qualify, so the more specialised document engine wins."""
    filler = " filler text to reach minimum chars " * 5
    variant_texts = {
        "original+glm_ocr": ("Line A $1.00\nLine B $2.00\nLine C $3.00" + filler),
        "original+vision_api": ("Other A $4.00\nOther B $5.00\nOther C $6.00" + filler),
    }
    result = multi_ocr.build_consensus(variant_texts)
    assert result.engine_used == "authoritative(original+glm_ocr)"
    # glm_ocr's content is the one that survives
    assert "Line A" in result.text
    assert "Other A" not in result.text


def test_vision_authoritative_pick_returns_none_for_empty_input():
    assert multi_ocr._vision_authoritative_pick({}) is None


def test_vision_authoritative_pick_returns_none_when_no_vision_engine():
    """Only tesseract variants — no vision engine exists to be authoritative."""
    variants = {
        "upscale_2x+tesseract_psm6": "SHEIN Skirt 1 $8.19\nGrand Total $67.80\nTax $5.00",
        "clahe_otsu+tesseract_psm6": "SHEIN Skirt 1 $8.19\nGrand Total $67.80\nTax $5.00",
    }
    assert multi_ocr._vision_authoritative_pick(variants) is None


def test_cache_load_retro_applies_vision_authoritative(tmp_path):
    """A cache written BEFORE the authoritative short-circuit existed
    stored a mangled hybrid-consensus text alongside a clean glm_ocr
    variant. On load we detect this and swap the stored text for the
    vision read without forcing a re-OCR."""
    variants = _hm_receipt_variants()
    # Simulate the stale cache: an ugly hybrid consensus text instead
    # of glm_ocr's clean one.
    stale = multi_ocr.MultiOcrResult(
        text=(
            "Heaite Flared Twill Pants 12 Dark gray 1 $20.99 -$13.00 $7.99\n"
            "ff eee Pendant Necklace NOSIZE Gold- 1 $5.99 -$2.00 $3.99"
        ),
        variants=variants,
        token_map={"7.99": [("original+glm_ocr", "x")]},
        confidence=0.30,
        method_stats={vid: len(t) for vid, t in variants.items()},
        page_count=1,
        engine_used="hybrid(adaptive_gaussian+tesseract_psm6,clahe_otsu+tesseract_psm6,"
        "clahe_otsu+tesseract_psm4,original+glm_ocr,original+tesseract_psm6,"
        "upscale_2x+tesseract_psm4,upscale_2x+tesseract_psm6)",
    )
    multi_ocr.cache_save("hm_hash", stale, base_dir=str(tmp_path))

    loaded = multi_ocr.cache_load("hm_hash", base_dir=str(tmp_path))
    assert loaded is not None
    # Retro-fixed text is glm_ocr's clean output
    assert "1206305006007" in loaded.text
    assert "Heaite" not in loaded.text
    assert "ff eee" not in loaded.text
    assert loaded.engine_used == "authoritative(original+glm_ocr)"
    assert loaded.confidence == 1.0
