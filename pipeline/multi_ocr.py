#!/usr/bin/env python3
"""
multi_ocr — unified hybrid OCR for AutoInvoice2XLSX.

This module is the single OCR pathway for the project. It replaces the
five ad-hoc chains that used to live in ``text_extractor.py``,
``pdf_splitter.py::ocr_page``, ``pdf_extractor.py``, ``bl_parser.py``,
``workflow/batch.py``, and ``stages/supplier_resolver.py``.

Philosophy
----------
OCR failure modes are **orthogonal** across engines and across image
preprocessing strategies. PaddleOCR reading "1" as "+" in a mobile
screenshot invoice has nothing to do with Tesseract's LSTM model
occasionally dropping commas, and both are unrelated to pdfplumber's
inability to see anything in a scanned PDF. Instead of picking one chain
and living with its failures, run every available combination and
reconcile the outputs.

Pipeline
--------
1. ``_render_pages`` — render the PDF once at high DPI via PyMuPDF.
2. Preprocessing variants (5) — applied in-memory to each rendered page:
   * ``original``             — no-op baseline
   * ``upscale_2x``           — cv2 INTER_CUBIC 2x upscale
   * ``clahe_otsu``           — grayscale → denoise → CLAHE → Otsu → morph open
   * ``adaptive_gaussian``    — grayscale → 2x → adaptive Gaussian threshold
   * ``sharpen_contrast``     — Pillow unsharp mask + contrast enhance
3. OCR engines (6) — each engine runs against a specific subset of
   preprocessed variants determined by ``QUALITY_MATRIX``:
   * ``pdfplumber``           — PDF-native digital text (runs once)
   * ``tesseract_psm6``       — Tesseract LSTM OEM=3, uniform block
   * ``tesseract_psm4``       — Tesseract, single column variable size
   * ``tesseract_psm11``      — Tesseract, sparse text
   * ``paddleocr``            — PaddleOCR CNN with angle classifier
   * ``glm_ocr``              — Z.AI GLM-OCR (purpose-built document OCR)
   * ``vision_api``           — Anthropic/Z.AI vision model (lazy)
4. ``build_consensus`` — fuzzy line alignment across all variant texts,
   then per-token voting with:
   * engine-priority tie-break (vision > pdfplumber > tesseract_psm6 > ...)
   * digit-wins-in-numeric-slot rule (fixes "SHEIN... + 8.19" → "1 8.19"
     even when the digit is a minority token, as long as an adjacent
     slot is numeric-dominant).
5. ``build_token_map`` — collect every ``(variant_id, full_line)`` that
   saw each price-like token, so downstream parsers that fail to match
   an item line can query ``token_map['8.19']`` and pick whichever
   variant line parses cleanly.

Short-circuit
-------------
Clean digital PDFs (pdfplumber returns ≥50 chars with at least one
digit) skip the entire OCR matrix and return immediately.

Cache
-----
Per-``sha1(pdf_bytes)`` cache at ``data/ocr_cache/<aa>/<sha1>.json``
keyed by ``CONFIG_VERSION``. Bump ``CONFIG_VERSION`` when preprocessing,
engines, or consensus logic change — stale entries are rejected on
load.

Public API
----------
* ``extract_text(pdf_path, *, quality='standard', use_cache=True)
  -> MultiOcrResult``
* ``MultiOcrResult`` dataclass — ``.text``, ``.variants``, ``.token_map``,
  ``.confidence``, ``.method_stats``, ``.page_count``, ``.engine_used``.
* ``find_token_contexts(token_map, token)`` — helper for orphan recovery.
* ``preprocess_*``, ``engine_*``, ``align_lines``, ``consensus_line``,
  ``build_token_map``, ``build_consensus`` — exposed for testing.
"""

from __future__ import annotations

import base64
import concurrent.futures
import dataclasses
import difflib
import hashlib
import io
import json
import logging
import os
import re
import sys
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Bump this string when preprocessing, engines, or consensus change.
# Any cached result with a different version string is rejected on load.
CONFIG_VERSION = "multi_ocr_v2_2026_04_13"

DEFAULT_DPI = 400

# Engine priority used to break ties during token voting.
# Higher wins. pdfplumber is trusted on digital PDFs; vision_api is the
# oracle for scanned content; Tesseract PSM=6 is the baseline raster OCR.
ENGINE_PRIORITY: Dict[str, int] = {
    "glm_ocr": 110,
    "vision_api": 100,
    "pdfplumber": 90,
    "tesseract_psm6": 80,
    "tesseract_psm4": 70,
    "paddleocr": 60,
    "tesseract_psm11": 50,
}


# ─── Data types ────────────────────────────────────────────────────────


@dataclasses.dataclass
class MultiOcrResult:
    """Structured output of the hybrid OCR pipeline.

    Attributes:
        text: Single consensus text, ready for downstream parsers.
        variants: Every per-(preprocess, engine) text that was produced.
            Keys are variant ids like ``"upscale_2x+tesseract_psm6"``
            or bare engine names for engines that do not use preprocessing
            (``"pdfplumber"``).
        token_map: ``{price_token: [(variant_id, full_line), ...]}``.
            For each numeric token seen in any variant, collects the
            full line(s) that contained it so downstream parsers can
            cross-reference when their own parse fails.
        confidence: Fraction of aligned line-clusters where every
            variant agreed (after normalization). 1.0 on digital PDFs.
        method_stats: ``{variant_id: char_count}`` — useful for telling
            at a glance which engine contributed the most.
        page_count: Number of pages processed.
        engine_used: Short summary label (``"pdfplumber"`` for digital
            short-circuit, ``"hybrid(var1,var2,...)"`` otherwise,
            ``"none"`` when no engine produced anything).
    """

    text: str
    variants: Dict[str, str]
    token_map: Dict[str, List[Tuple[str, str]]]
    confidence: float
    method_stats: Dict[str, int]
    page_count: int
    engine_used: str


def _empty_result(page_count: int = 0) -> MultiOcrResult:
    return MultiOcrResult(
        text="",
        variants={},
        token_map={},
        confidence=0.0,
        method_stats={},
        page_count=page_count,
        engine_used="none",
    )


# ─── Preprocessing variants ────────────────────────────────────────────


def preprocess_original(img):
    """Return the image unchanged — baseline variant for engines that
    handle their own normalization internally (PaddleOCR)."""
    return img


def preprocess_upscale_2x(img):
    """Double each dimension with cv2 cubic interpolation. This is the
    "zoom" the user asked about — a universal OCR accuracy booster for
    small glyphs that any single OCR engine tends to misread."""
    try:
        import cv2  # type: ignore
    except ImportError:
        return img
    try:
        h, w = img.shape[:2]
        return cv2.resize(img, (w * 2, h * 2), interpolation=cv2.INTER_CUBIC)
    except Exception as e:
        logger.debug(f"preprocess_upscale_2x failed: {e}")
        return img


def preprocess_clahe_otsu(img):
    """Grayscale → denoise → CLAHE contrast → Otsu threshold → morph open.

    This is the v1 ``_preprocess_image`` pipeline from
    ``pdf_splitter.py``, preserved verbatim. Best for clean scans of
    printed documents with uneven lighting.
    """
    try:
        import cv2  # type: ignore
        import numpy as np  # type: ignore
    except ImportError:
        return img
    try:
        if img.ndim == 3:
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        else:
            gray = img
        gray = cv2.fastNlMeansDenoising(gray, h=10)
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        gray = clahe.apply(gray)
        _, binary = cv2.threshold(
            gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU
        )
        kernel = np.ones((2, 2), np.uint8)
        binary = cv2.morphologyEx(
            binary, cv2.MORPH_OPEN, kernel, iterations=1
        )
        return binary
    except Exception as e:
        logger.debug(f"preprocess_clahe_otsu failed: {e}")
        return img


def preprocess_adaptive_gaussian(img):
    """Grayscale → 2x upscale → adaptive Gaussian threshold.

    This is the v1 ``_preprocess_image_adaptive`` pipeline from
    ``pdf_splitter.py``. Better than Otsu for receipts, mobile
    screenshots, and documents with heavy lighting gradients.
    """
    try:
        import cv2  # type: ignore
    except ImportError:
        return img
    try:
        if img.ndim == 3:
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        else:
            gray = img
        h, w = gray.shape
        gray = cv2.resize(gray, (w * 2, h * 2), interpolation=cv2.INTER_CUBIC)
        return cv2.adaptiveThreshold(
            gray,
            255,
            cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY,
            31,
            15,
        )
    except Exception as e:
        logger.debug(f"preprocess_adaptive_gaussian failed: {e}")
        return img


def preprocess_sharpen_contrast(img):
    """Pillow: contrast 1.8x + unsharp mask. Unlike the threshold-based
    variants, this preserves color and grayscale information — which is
    important for Vision API (the model benefits from seeing anti-aliased
    text) and for PaddleOCR's own internal binarization."""
    try:
        from PIL import Image, ImageEnhance, ImageFilter  # type: ignore
        import numpy as np  # type: ignore
    except ImportError:
        return img
    try:
        pil = Image.fromarray(img)
        pil = ImageEnhance.Contrast(pil).enhance(1.8)
        pil = pil.filter(ImageFilter.UnsharpMask(radius=1, percent=150))
        return np.array(pil)
    except Exception as e:
        logger.debug(f"preprocess_sharpen_contrast failed: {e}")
        return img


PREPROCESSING_VARIANTS = {
    "original": preprocess_original,
    "upscale_2x": preprocess_upscale_2x,
    "clahe_otsu": preprocess_clahe_otsu,
    "adaptive_gaussian": preprocess_adaptive_gaussian,
    "sharpen_contrast": preprocess_sharpen_contrast,
}


# ─── OCR engine wrappers ───────────────────────────────────────────────


def engine_pdfplumber(pdf_path: str) -> str:
    """Run pdfplumber against the PDF directly (no image input).

    pdfplumber extracts embedded text from digital PDFs. For scanned
    PDFs it returns "" and the hybrid path continues.
    """
    try:
        import pdfplumber  # type: ignore
    except ImportError:
        return ""
    if pdfplumber is None:  # sentinel set by tests to simulate missing pkg
        return ""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            parts = []
            for page in pdf.pages:
                t = page.extract_text() or ""
                if t:
                    parts.append(t)
            return "\n\n".join(parts).strip()
    except Exception as e:
        logger.debug(f"pdfplumber failed for {pdf_path}: {e}")
        return ""


def _is_wsl_windows_tesseract() -> bool:
    """True when we're on WSL calling a Windows tesseract.exe binary."""
    try:
        import pytesseract  # type: ignore
        cmd = getattr(pytesseract.pytesseract, 'tesseract_cmd', '')
        return '/mnt/c/' in cmd or cmd.startswith('C:\\')
    except Exception:
        return False


def _configure_tesseract():
    """Point pytesseract at an installed binary (cross-platform) and
    return the module, or None if pytesseract isn't importable."""
    try:
        import pytesseract  # type: ignore
    except ImportError:
        return None
    if pytesseract is None:
        return None
    # Only auto-detect binary if pytesseract doesn't already have a
    # non-default command set (allows tests to inject a fake path).
    current_cmd = getattr(pytesseract.pytesseract, 'tesseract_cmd', 'tesseract')
    if current_cmd in ('tesseract', ''):
        candidates = [
            r"C:\Program Files\Tesseract-OCR\tesseract.exe",
            r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
            "/mnt/c/Program Files/Tesseract-OCR/tesseract.exe",
            "/usr/bin/tesseract",
            "/usr/local/bin/tesseract",
        ]
        try:
            for p in candidates:
                if os.path.exists(p):
                    pytesseract.pytesseract.tesseract_cmd = p
                    break
        except Exception:
            # Tests may pass a simplified fake where attribute assignment
            # fails; that's fine — the fake handles image_to_string directly.
            pass
    return pytesseract


def _to_windows_path(wsl_path: str) -> str:
    """Convert a WSL path to a Windows path for use by Windows binaries."""
    if wsl_path.startswith('/mnt/'):
        # /mnt/c/foo → C:\foo
        parts = wsl_path.split('/')
        drive = parts[2].upper()
        rest = '\\'.join(parts[3:])
        return f"{drive}:\\{rest}"
    return wsl_path


def engine_tesseract(img, psm: int = 6) -> str:
    """Run Tesseract LSTM (OEM=3) at the given PSM against an image.

    On WSL with a Windows tesseract.exe binary, pytesseract's default
    temp-file path (/tmp/) is inaccessible to the Windows binary.
    We save the image to a Windows-accessible temp path and convert
    the path to Windows format before calling tesseract.
    """
    try:
        from PIL import Image  # type: ignore
        import numpy as np  # type: ignore
    except ImportError:
        Image = None  # type: ignore
        np = None  # type: ignore
    pytesseract = _configure_tesseract()
    if pytesseract is None:
        return ""
    try:
        if np is not None and isinstance(img, np.ndarray) and Image is not None:
            pil = Image.fromarray(img)
        else:
            pil = img
        config = f"--psm {psm} --oem 3"

        # WSL + Windows tesseract: pytesseract writes temp files to /tmp/
        # which Windows tesseract.exe can't read. Save image to a
        # Windows-accessible path and call tesseract via subprocess directly.
        tess_cmd = getattr(pytesseract.pytesseract, 'tesseract_cmd', '')
        if '/mnt/c/' in tess_cmd:
            import tempfile
            import subprocess
            # Use the pipeline directory as temp (on /mnt/c/ = Windows-accessible)
            win_tmp = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.ocr_tmp')
            os.makedirs(win_tmp, exist_ok=True)
            fd, tmp_img = tempfile.mkstemp(suffix='.png', dir=win_tmp)
            os.close(fd)
            fd2, tmp_out = tempfile.mkstemp(suffix='', dir=win_tmp)
            os.close(fd2)
            try:
                pil.save(tmp_img)
                # Convert file paths to Windows format for tesseract.exe,
                # but keep the binary path as WSL path (subprocess needs it)
                win_img = _to_windows_path(tmp_img)
                win_out = _to_windows_path(tmp_out)
                cmd = [tess_cmd, win_img, win_out, '-l', 'eng',
                       '--psm', str(psm), '--oem', '3']
                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=30
                )
                out_file = tmp_out + '.txt'
                if os.path.exists(out_file):
                    with open(out_file, 'r', encoding='utf-8', errors='replace') as f:
                        text = f.read()
                    os.remove(out_file)
                    return text or ""
                return ""
            finally:
                for p in (tmp_img, tmp_out):
                    try:
                        os.remove(p)
                    except OSError:
                        pass
        else:
            text = pytesseract.image_to_string(pil, lang="eng", config=config)
            return text or ""
    except Exception as e:
        logger.debug(f"tesseract psm={psm} failed: {e}")
        return ""


_PADDLE_INSTANCE: Any = None


def engine_paddleocr(img) -> str:
    """Run PaddleOCR with angle classification on an image array."""
    try:
        import paddleocr  # type: ignore
    except ImportError:
        return ""
    if paddleocr is None:
        return ""
    try:
        from paddleocr import PaddleOCR  # type: ignore
    except ImportError:
        return ""
    try:
        import numpy as np  # type: ignore
        if not isinstance(img, np.ndarray):
            img = np.array(img)
        global _PADDLE_INSTANCE
        if _PADDLE_INSTANCE is None:
            _PADDLE_INSTANCE = PaddleOCR(
                use_angle_cls=True, lang="en", show_log=False
            )
        result = _PADDLE_INSTANCE.ocr(img, cls=True)
        lines = []
        if result and result[0]:
            for line in result[0]:
                if line and len(line) >= 2:
                    lines.append(line[1][0])
        return "\n".join(lines)
    except Exception as e:
        logger.debug(f"paddleocr failed: {e}")
        return ""


def engine_vision_api(
    img, api_key: str, base_url: str, model: str
) -> str:
    """Send the page image to an Anthropic-compatible vision API (Z.AI
    GLM or Claude). Returns "" if no API key is configured or the HTTP
    call fails."""
    if not api_key:
        return ""
    try:
        from anthropic import Anthropic  # type: ignore
        from PIL import Image  # type: ignore
        import numpy as np  # type: ignore
    except ImportError:
        return ""
    try:
        if isinstance(img, np.ndarray):
            pil = Image.fromarray(img)
        else:
            pil = img
        buf = io.BytesIO()
        pil.save(buf, format="PNG")
        b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
        client = Anthropic(api_key=api_key, base_url=base_url)
        response = client.messages.create(
            model=model,
            max_tokens=4096,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "image",
                            "source": {
                                "type": "base64",
                                "media_type": "image/png",
                                "data": b64,
                            },
                        },
                        {
                            "type": "text",
                            "text": (
                                "Extract ALL text from this invoice page. "
                                "Return only the extracted text, preserving "
                                "the layout as much as possible. Do not add "
                                "commentary."
                            ),
                        },
                    ],
                }
            ],
        )
        return response.content[0].text if response.content else ""
    except Exception as e:
        logger.debug(f"vision_api failed: {e}")
        return ""


def engine_glm_ocr(img, api_key: str) -> str:
    """Send the page image to Z.AI GLM-OCR (purpose-built document OCR).

    Uses the OpenAI-compatible endpoint at /api/paas/v4/chat/completions
    with Bearer auth — GLM models are NOT on the Anthropic proxy.
    """
    if not api_key:
        return ""
    try:
        import numpy as np  # type: ignore
        from PIL import Image  # type: ignore
    except ImportError:
        return ""
    try:
        if isinstance(img, np.ndarray):
            pil = Image.fromarray(img)
        else:
            pil = img
        buf = io.BytesIO()
        pil.save(buf, format="PNG")
        b64 = base64.b64encode(buf.getvalue()).decode("utf-8")

        from urllib.request import Request, urlopen

        endpoint = "https://api.z.ai/api/coding/paas/v4/chat/completions"
        request_data = json.dumps({
            "model": "glm-4.6v",
            "max_tokens": 4096,
            "messages": [{
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": f"data:image/png;base64,{b64}",
                        }
                    },
                    {
                        "type": "text",
                        "text": (
                            "Extract ALL text from this document page. "
                            "Return only the extracted text, preserving "
                            "the layout as much as possible. Do not add "
                            "commentary."
                        ),
                    }
                ]
            }]
        }).encode("utf-8")

        req = Request(
            endpoint,
            data=request_data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
        )

        with urlopen(req, timeout=60) as response:
            result = json.loads(response.read().decode("utf-8"))

        return result["choices"][0]["message"]["content"].strip()
    except Exception as e:
        logger.debug(f"glm_ocr failed: {e}")
        return ""


# ─── Page rendering ────────────────────────────────────────────────────


def _render_pages(pdf_path: str, dpi: int = DEFAULT_DPI) -> List[Any]:
    """Render every page of a PDF to numpy image arrays via PyMuPDF."""
    try:
        import fitz  # type: ignore
        import numpy as np  # type: ignore
        from PIL import Image  # type: ignore
    except ImportError:
        return []
    images: List[Any] = []
    try:
        doc = fitz.open(pdf_path)
        mat = fitz.Matrix(dpi / 72, dpi / 72)
        for page in doc:
            pix = page.get_pixmap(matrix=mat)
            pil = Image.open(io.BytesIO(pix.tobytes("png")))
            if pil.mode != "RGB":
                pil = pil.convert("RGB")
            images.append(np.array(pil))
        doc.close()
    except Exception as e:
        logger.warning(f"page rendering failed for {pdf_path}: {e}")
    return images


# ─── Quality matrix (which variants × which engines) ─────────────────


# Maps quality level -> {engine_name: tuple_of_variant_names_or_None}.
# None means the engine runs directly against the PDF path (not images).
QUALITY_MATRIX: Dict[str, Dict[str, Optional[Tuple[str, ...]]]] = {
    "fast": {
        "pdfplumber": None,
        "tesseract_psm6": ("clahe_otsu",),
    },
    "standard": {
        "pdfplumber": None,
        "tesseract_psm6": (
            "original",
            "upscale_2x",
            "clahe_otsu",
            "adaptive_gaussian",
            "sharpen_contrast",
        ),
        "tesseract_psm4": ("upscale_2x", "clahe_otsu"),
        "paddleocr": ("original", "upscale_2x", "sharpen_contrast"),
        "glm_ocr": ("original",),
    },
    "deep": {
        "pdfplumber": None,
        "tesseract_psm6": (
            "original",
            "upscale_2x",
            "clahe_otsu",
            "adaptive_gaussian",
            "sharpen_contrast",
        ),
        "tesseract_psm4": ("upscale_2x", "clahe_otsu"),
        "tesseract_psm11": ("sharpen_contrast",),
        "paddleocr": ("original", "upscale_2x", "sharpen_contrast"),
        "glm_ocr": ("original",),
        "vision_api": ("original",),
    },
}


def _resolve_api_key() -> str:
    """Resolve LLM API key from environment or pipeline config."""
    key = os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("ZAI_API_KEY", "")
    if key:
        return key
    # Fall back to pipeline config (settings.json loaded by classifier)
    try:
        from classifier import _load_llm_settings
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        settings = _load_llm_settings(base_dir)
        return settings.get("api_key", "")
    except Exception:
        return ""


def _run_engine_on_image(engine_name: str, img) -> str:
    if engine_name.startswith("tesseract_psm"):
        psm = int(engine_name.replace("tesseract_psm", ""))
        return engine_tesseract(img, psm=psm)
    if engine_name == "paddleocr":
        return engine_paddleocr(img)
    if engine_name == "glm_ocr":
        api_key = _resolve_api_key()
        return engine_glm_ocr(img, api_key=api_key)
    if engine_name == "vision_api":
        api_key = _resolve_api_key()
        base_url = os.environ.get(
            "ANTHROPIC_BASE_URL", "https://api.z.ai/api/anthropic"
        )
        model = os.environ.get("VISION_MODEL", "glm-4.7")
        return engine_vision_api(
            img, api_key=api_key, base_url=base_url, model=model
        )
    return ""


def run_all_variants(
    pdf_path: str, quality: str = "standard"
) -> Dict[str, str]:
    """Run every (preprocessing × engine) combination in the quality
    matrix. Returns ``{variant_id: text}`` where variant_id is either
    a bare engine name (for pdfplumber) or ``"{variant}+{engine}"``."""
    matrix = QUALITY_MATRIX.get(quality, QUALITY_MATRIX["standard"])
    variant_texts: Dict[str, str] = {}

    # pdfplumber is PDF-native and runs once.
    if "pdfplumber" in matrix:
        text = engine_pdfplumber(pdf_path)
        if text:
            variant_texts["pdfplumber"] = text

    image_engines = {k: v for k, v in matrix.items() if v is not None}
    if not image_engines:
        return variant_texts

    images = _render_pages(pdf_path)
    if not images:
        return variant_texts

    # Preprocess once per (variant, page) — de-duped across engines.
    needed: set = set()
    for variants in image_engines.values():
        for v in variants or ():
            needed.add(v)

    preprocessed: Dict[str, List[Any]] = {}
    for v_name in needed:
        fn = PREPROCESSING_VARIANTS.get(v_name)
        if fn is None:
            continue
        try:
            preprocessed[v_name] = [fn(img) for img in images]
        except Exception as e:
            logger.debug(f"preprocessing {v_name} failed: {e}")

    # Build tasks, then run in a thread pool.
    tasks: List[Tuple[str, str, List[Any]]] = []
    for engine_name, variants in image_engines.items():
        for v_name in variants or ():
            if v_name not in preprocessed:
                continue
            variant_id = f"{v_name}+{engine_name}"
            tasks.append((variant_id, engine_name, preprocessed[v_name]))

    def _run_task(task):
        variant_id, engine_name, imgs = task
        parts = []
        for img in imgs:
            try:
                parts.append(_run_engine_on_image(engine_name, img))
            except Exception as e:
                logger.debug(f"{variant_id} failed on one page: {e}")
                parts.append("")
        return variant_id, "\n\n".join(p for p in parts if p)

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as executor:
        for variant_id, text in executor.map(_run_task, tasks):
            if text:
                variant_texts[variant_id] = text

    return variant_texts


# ─── Consensus: tokenization + alignment + voting ────────────────────


# A token is either a money/number blob, a word, or a standalone symbol.
# Numeric blobs are preserved as single tokens so the voter can treat
# "$8.19", "8.19", "1", and "2024-07-28" as atomic units.
_TOKEN_RE = re.compile(r"\$?\d+(?:[.,]\d+)*|[A-Za-z]+|[^\s\w]")

# A "numeric token" for the digit-priority rule: pure integers, decimals,
# or currency-prefixed decimals. Dates (2024-07-28) are NOT considered
# numeric tokens because they should not compete with qty/price slots.
_NUMERIC_TOKEN = re.compile(r"^\$?\d+(?:\.\d+)?$")

# Price pattern used by the token map. Matches "8.19", "67.80", "$8.19".
_PRICE_PATTERN = re.compile(r"\$?(\d+\.\d{2})\b")


def _tokenize_line(line: str) -> List[str]:
    return [t for t in _TOKEN_RE.findall(line) if t]


def _normalize_line(line: str) -> str:
    return re.sub(r"\s+", " ", line.strip().lower())


def _engine_of(variant_id: str) -> str:
    return variant_id.split("+", 1)[-1] if "+" in variant_id else variant_id


def _priority_of(variant_id: str) -> int:
    return ENGINE_PRIORITY.get(_engine_of(variant_id), 0)


def _pick_skeleton(variant_texts: Dict[str, str]) -> Tuple[str, str]:
    """Return ``(variant_id, text)`` of the best variant to use as the
    alignment reference. Prefers high engine-priority + high digit
    density. Deterministic tie-break by variant_id for reproducibility.
    """
    if not variant_texts:
        return ("", "")

    def _score(item):
        vid, text = item
        priority = _priority_of(vid)
        digit_density = len(_PRICE_PATTERN.findall(text))
        # Negative vid for reverse alphabetical tie-break (so higher wins
        # via max); actually we want deterministic, so use vid as a
        # string sort key (ascending by default).
        return (priority, digit_density, -sum(ord(c) for c in vid))

    best_vid, best_text = max(variant_texts.items(), key=_score)
    return best_vid, best_text


def _pick_best_fallback(variant_texts: Dict[str, str]) -> Tuple[str, str]:
    """Pick the best variant for the low-confidence fallback.

    Unlike ``_pick_skeleton`` (which weights engine priority highest),
    this prefers **digit density first** — when consensus failed, the
    variant with the most numeric content (prices, quantities, invoice
    numbers) is the most useful for downstream extraction.
    """
    if not variant_texts:
        return ("", "")

    def _score(item):
        vid, text = item
        digit_density = len(_PRICE_PATTERN.findall(text))
        text_length = len(text)
        priority = _priority_of(vid)
        return (digit_density, text_length, priority, vid)

    best_vid, best_text = max(variant_texts.items(), key=_score)
    return best_vid, best_text


def align_lines(
    variant_texts: Dict[str, str],
) -> List[List[Tuple[str, str]]]:
    """Cluster corresponding lines across variants by fuzzy match.

    Returns a list of clusters. Each cluster is a list of
    ``(variant_id, line)`` tuples. The first entry in each cluster is
    the skeleton line. Skeleton lines with zero matching lines from
    other variants are considered hallucinations and dropped (unless
    there is only a single variant total).
    """
    if not variant_texts:
        return []

    skel_vid, skeleton_text = _pick_skeleton(variant_texts)
    skel_lines = [l for l in skeleton_text.splitlines() if l.strip()]

    other_lines: Dict[str, List[str]] = {}
    for vid, text in variant_texts.items():
        if vid == skel_vid:
            continue
        other_lines[vid] = [l for l in text.splitlines() if l.strip()]

    used_idx: Dict[str, set] = {vid: set() for vid in other_lines}

    clusters: List[List[Tuple[str, str]]] = []
    for skel_line in skel_lines:
        cluster: List[Tuple[str, str]] = [(skel_vid, skel_line)]
        skel_norm = _normalize_line(skel_line)

        for vid, lines in other_lines.items():
            best_ratio = 0.0
            best_idx = -1
            for i, line in enumerate(lines):
                if i in used_idx[vid]:
                    continue
                r = difflib.SequenceMatcher(
                    None, skel_norm, _normalize_line(line)
                ).ratio()
                if r > best_ratio:
                    best_ratio = r
                    best_idx = i
            if best_ratio >= 0.55 and best_idx >= 0:
                cluster.append((vid, lines[best_idx]))
                used_idx[vid].add(best_idx)

        # Drop single-entry clusters when there are multiple variants —
        # they are almost always hallucinations (lines only one variant
        # saw). Keep them when we only have a single variant total.
        if len(cluster) >= 2 or len(variant_texts) == 1:
            clusters.append(cluster)

    return clusters


def _align_cluster_tokens(
    cluster: List[Tuple[str, str]],
) -> List[List[Tuple[str, str]]]:
    """Within a cluster, align every line's tokens to the skeleton line's
    tokens via difflib. Returns a list ``positions`` where each entry is
    a list of ``(variant_id, token)`` contributions at that position."""
    if not cluster:
        return []

    ref_vid, ref_line = cluster[0]
    ref_tokens = _tokenize_line(ref_line)
    positions: List[List[Tuple[str, str]]] = [
        [(ref_vid, t)] for t in ref_tokens
    ]

    for vid, line in cluster[1:]:
        other_tokens = _tokenize_line(line)
        matcher = difflib.SequenceMatcher(
            None, ref_tokens, other_tokens, autojunk=False
        )
        for tag, i1, i2, j1, j2 in matcher.get_opcodes():
            if tag == "equal":
                for k in range(i2 - i1):
                    positions[i1 + k].append((vid, other_tokens[j1 + k]))
            elif tag == "replace":
                n = min(i2 - i1, j2 - j1)
                for k in range(n):
                    positions[i1 + k].append((vid, other_tokens[j1 + k]))
            # 'delete' — ref has tokens not in other; other contributes nothing
            # 'insert' — other has tokens not in ref; we drop them

    return positions


def consensus_line(cluster: List[Tuple[str, str]]) -> str:
    """Produce a single consensus line from a cluster of variant lines.

    Uses per-token voting with:
    * engine-priority tie-break (highest ENGINE_PRIORITY wins on ties)
    * digit-wins-in-numeric-slot rule: if any numeric token is present
      at a position AND the position is either self-majority-numeric or
      adjacent to a majority-numeric position, a numeric token always
      wins over symbol noise (even if the digit is a minority vote).
    """
    if not cluster:
        return ""
    if len(cluster) == 1:
        return cluster[0][1]

    positions = _align_cluster_tokens(cluster)
    if not positions:
        return ""

    # Precompute votes and engines per position
    pos_votes: List[Counter] = []
    pos_engines: List[Dict[str, List[str]]] = []
    for contributions in positions:
        votes: Counter = Counter()
        engines: Dict[str, List[str]] = defaultdict(list)
        for vid, tok in contributions:
            if not tok:
                continue
            votes[tok] += 1
            engines[tok].append(vid)
        pos_votes.append(votes)
        pos_engines.append(engines)

    def _is_numeric_position(idx: int) -> bool:
        votes = pos_votes[idx]
        if not votes:
            return False
        num = sum(c for t, c in votes.items() if _NUMERIC_TOKEN.match(t))
        total = sum(votes.values())
        return num >= 1 and num * 2 >= total

    def _is_numeric_context(idx: int) -> bool:
        votes = pos_votes[idx]
        if not votes:
            return False
        has_digit = any(_NUMERIC_TOKEN.match(t) for t in votes)
        if not has_digit:
            return False
        if _is_numeric_position(idx):
            return True
        for adj in (idx - 1, idx + 1):
            if 0 <= adj < len(pos_votes) and _is_numeric_position(adj):
                return True
        return False

    consensus_tokens: List[str] = []
    for idx, votes in enumerate(pos_votes):
        if not votes:
            continue
        engines = pos_engines[idx]

        def _pick(cands: Dict[str, int]) -> str:
            return max(
                cands.items(),
                key=lambda kv: (
                    kv[1],
                    max(_priority_of(e) for e in engines[kv[0]]),
                    kv[0],  # stable tertiary tie-break
                ),
            )[0]

        if _is_numeric_context(idx):
            numeric_votes = {
                t: c for t, c in votes.items() if _NUMERIC_TOKEN.match(t)
            }
            if numeric_votes:
                consensus_tokens.append(_pick(numeric_votes))
                continue

        consensus_tokens.append(_pick(dict(votes)))

    return " ".join(consensus_tokens)


def build_token_map(
    variant_texts: Dict[str, str],
) -> Dict[str, List[Tuple[str, str]]]:
    """For each price-like token appearing in any variant, collect every
    ``(variant_id, full_line)`` pair where it was seen."""
    token_map: Dict[str, List[Tuple[str, str]]] = defaultdict(list)
    for vid, text in variant_texts.items():
        for line in text.splitlines():
            if not line.strip():
                continue
            for m in _PRICE_PATTERN.finditer(line):
                token = m.group(1)
                token_map[token].append((vid, line))
    return dict(token_map)


def find_token_contexts(
    token_map: Dict[str, List[Tuple[str, str]]], token: str
) -> List[Tuple[str, str]]:
    """Return every ``(variant_id, line)`` pair where ``token`` was seen."""
    return list(token_map.get(token, []))


def _compute_confidence(
    clusters: List[List[Tuple[str, str]]],
) -> float:
    if not clusters:
        return 0.0
    agreed = 0
    for cluster in clusters:
        norms = {_normalize_line(line) for _, line in cluster}
        if len(norms) == 1:
            agreed += 1
    return agreed / len(clusters)


def _is_sensible_line(line: str) -> bool:
    """Heuristic: does *line* look like real invoice content or OCR garbage?

    A line is sensible if it carries meaningful signal:
    - Contains a price/currency pattern  (strong signal)
    - Contains a multi-digit number      (moderate signal)
    - Contains a real English/product word ≥3 chars AND the line isn't
      majority punctuation/symbols        (weak but sufficient)
    """
    stripped = line.strip()
    if len(stripped) < 4:
        return False

    # Strong: contains a price like 8.19 or $12.50
    if _PRICE_PATTERN.search(stripped):
        return True

    # Moderate: contains a multi-digit number (qty, invoice #, date component)
    if re.search(r"\d{2,}", stripped):
        # But reject lines that are ONLY numbers with no alpha context
        # (e.g. OCR garbage "123456789")
        if re.search(r"[A-Za-z]{2,}", stripped):
            return True
        # Pure number lines are OK if they look like invoice numbers
        if re.match(r"^[\d\s.,-]+$", stripped) and len(stripped) < 30:
            return True

    # Weak: word-based signal. OCR garbage produces short fragments like
    # "SRS Aaa rT Sis ae nena" — many 2-4 char pieces that aren't real words.
    # Real invoice text has longer average word length.
    all_words = re.findall(r"[A-Za-z]+", stripped)
    if all_words:
        avg_len = sum(len(w) for w in all_words) / len(all_words)
        long_words = [w for w in all_words if len(w) >= 6]
        # Accept if: average word length ≥ 4 AND (has a 6+ char word OR ≥3 words)
        if avg_len >= 4.0 and (long_words or len(all_words) >= 3):
            alpha_chars = sum(c.isalnum() or c.isspace() for c in stripped)
            total_chars = len(stripped)
            if total_chars > 0 and alpha_chars / total_chars >= 0.60:
                return True

    return False


def _build_super_text(variant_texts: Dict[str, str]) -> str:
    """Build a 'super text' by merging unique sensible lines from all variants.

    Instead of falling back to a single best variant when consensus confidence
    is low, this unions the content across ALL variants:

    1. Start with the best variant (by digit density + priority) as the base.
    2. For every other variant, find lines NOT already present in the base
       (using fuzzy matching to avoid near-duplicates).
    3. Filter those unique lines through a sensibility heuristic to reject
       OCR garbage (random symbols, fragmented characters).
    4. Insert accepted lines at the end of the base, grouped by source variant.

    The result captures text that different OCR engines extracted from different
    parts of the document while filtering out nonsense that bad engines
    hallucinated.
    """
    if not variant_texts:
        return ""

    # Use the best variant as the base
    best_vid, base_text = _pick_best_fallback(variant_texts)
    base_lines = [l for l in base_text.splitlines() if l.strip()]
    base_norms = [_normalize_line(l) for l in base_lines]

    # Collect unique sensible lines from other variants
    extra_lines: List[str] = []
    for vid, text in variant_texts.items():
        if vid == best_vid:
            continue
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            norm = _normalize_line(stripped)

            # Skip if it's a near-duplicate of any base line
            is_dup = False
            for bn in base_norms:
                if difflib.SequenceMatcher(None, norm, bn).ratio() > 0.80:
                    is_dup = True
                    break
            if is_dup:
                continue

            # Skip if it's a near-duplicate of an already-added extra line
            extra_dup = False
            for el in extra_lines:
                if difflib.SequenceMatcher(
                    None, norm, _normalize_line(el)
                ).ratio() > 0.80:
                    extra_dup = True
                    break
            if extra_dup:
                continue

            # Only keep sensible lines
            if _is_sensible_line(stripped):
                extra_lines.append(stripped)

    if extra_lines:
        logger.info(
            f"Super-text: merged {len(extra_lines)} unique lines from "
            f"{len(variant_texts) - 1} other variant(s) into base '{best_vid}'"
        )
        # Append extras after base — they're lines the best variant missed
        return "\n".join(base_lines + extra_lines)
    else:
        return base_text


def build_consensus(variant_texts: Dict[str, str]) -> MultiOcrResult:
    """Top-level consensus builder: align, vote, build token map, package.

    When consensus confidence falls below ``_LOW_CONFIDENCE_THRESHOLD``,
    the token-level voting has destroyed too much data (prices, quantities,
    invoice numbers).  In that case, fall back to the highest-priority
    individual variant (by engine priority × digit density) instead of
    emitting the degraded consensus.
    """
    if not variant_texts:
        return _empty_result()

    clusters = align_lines(variant_texts)
    consensus_lines = [consensus_line(c) for c in clusters]
    consensus_text = "\n".join(l for l in consensus_lines if l)
    token_map = build_token_map(variant_texts)
    confidence = _compute_confidence(clusters)
    method_stats = {vid: len(t) for vid, t in variant_texts.items()}

    # Low-confidence fallback: build a "super text" that merges unique
    # sensible lines from ALL variants instead of picking just one.
    # The consensus process can drop numeric tokens when alignment across
    # many variants disagrees; the super-text approach preserves content
    # that only specific engines captured (e.g., glm_ocr reading prices
    # that tesseract missed, or vice versa).
    _LOW_CONFIDENCE_THRESHOLD = 0.50
    _MIN_VARIANTS_FOR_FALLBACK = 4  # With ≤3 variants, majority voting works fine
    if confidence < _LOW_CONFIDENCE_THRESHOLD and len(variant_texts) >= _MIN_VARIANTS_FOR_FALLBACK:
        super_text = _build_super_text(variant_texts)
        if super_text and len(super_text) > len(consensus_text) * 0.8:
            best_vid = _pick_best_fallback(variant_texts)[0]
            logger.warning(
                f"Consensus confidence {confidence:.2f} < {_LOW_CONFIDENCE_THRESHOLD} "
                f"— using super-text merge (base '{best_vid}', "
                f"{len(super_text)} chars vs {len(consensus_text)} consensus chars)"
            )
            consensus_text = super_text
            engine_used = f"super({best_vid}+{len(variant_texts)-1}others)"
        else:
            engine_used = "hybrid(" + ",".join(sorted(variant_texts.keys())) + ")"
    else:
        engine_used = "hybrid(" + ",".join(sorted(variant_texts.keys())) + ")"

    return MultiOcrResult(
        text=consensus_text,
        variants=dict(variant_texts),
        token_map=token_map,
        confidence=confidence,
        method_stats=method_stats,
        page_count=1,
        engine_used=engine_used,
    )


# ─── Cache ─────────────────────────────────────────────────────────────


def _default_base_dir() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _cache_path(pdf_sha1: str, base_dir: Optional[str] = None) -> str:
    base = base_dir or _default_base_dir()
    return os.path.join(
        base, "data", "ocr_cache", pdf_sha1[:2], f"{pdf_sha1}.json"
    )


def cache_save(
    pdf_sha1: str,
    result: MultiOcrResult,
    base_dir: Optional[str] = None,
) -> None:
    path = _cache_path(pdf_sha1, base_dir=base_dir)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Convert tuples in token_map to lists for JSON
    serializable_token_map = {
        k: [list(pair) for pair in v] for k, v in result.token_map.items()
    }
    payload = {
        "config_version": CONFIG_VERSION,
        "pdf_sha1": pdf_sha1,
        "result": {
            "text": result.text,
            "variants": result.variants,
            "token_map": serializable_token_map,
            "confidence": result.confidence,
            "method_stats": result.method_stats,
            "page_count": result.page_count,
            "engine_used": result.engine_used,
        },
    }
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f)
    except Exception as e:
        logger.debug(f"cache_save failed: {e}")


def cache_load(
    pdf_sha1: str, base_dir: Optional[str] = None
) -> Optional[MultiOcrResult]:
    path = _cache_path(pdf_sha1, base_dir=base_dir)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if payload.get("config_version") != CONFIG_VERSION:
            return None
        r = payload["result"]
        token_map = {
            k: [tuple(pair) for pair in v]
            for k, v in r.get("token_map", {}).items()
        }
        text = r["text"]
        variants = r["variants"]
        confidence = r["confidence"]
        engine_used = r["engine_used"]

        # Re-apply super-text merge on cached results that used the old
        # single-variant fallback or raw consensus.
        _LOW_CONFIDENCE_THRESHOLD = 0.50
        _MIN_VARIANTS_FOR_FALLBACK = 4
        if (confidence < _LOW_CONFIDENCE_THRESHOLD
                and len(variants) >= _MIN_VARIANTS_FOR_FALLBACK
                and not engine_used.startswith("super(")):
            super_text = _build_super_text(variants)
            if super_text and len(super_text) > len(text) * 0.8:
                best_vid = _pick_best_fallback(variants)[0]
                logger.warning(
                    f"Cache hit with low confidence {confidence:.2f} "
                    f"— applying super-text merge (base '{best_vid}')"
                )
                text = super_text
                engine_used = f"super({best_vid}+{len(variants)-1}others)"

        return MultiOcrResult(
            text=text,
            variants=variants,
            token_map=token_map,
            confidence=confidence,
            method_stats=r["method_stats"],
            page_count=r["page_count"],
            engine_used=engine_used,
        )
    except Exception as e:
        logger.debug(f"cache_load failed for {path}: {e}")
        return None


def _pdf_sha1(pdf_path: str) -> str:
    h = hashlib.sha1()
    with open(pdf_path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


# ─── Top-level entry ──────────────────────────────────────────────────


def _is_clean_digital(text: str) -> bool:
    """Heuristic: pdfplumber output is considered usable if it has at
    least 50 characters and contains at least one digit."""
    if not text or len(text) < 50:
        return False
    return bool(re.search(r"\d", text))


def extract_text(
    pdf_path: str,
    *,
    quality: str = "standard",
    use_cache: bool = True,
    base_dir: Optional[str] = None,
) -> MultiOcrResult:
    """Unified entry point for every OCR request in the pipeline.

    Workflow:
    1. If ``use_cache`` is set and a cache entry exists under the PDF's
       SHA-1, return it immediately.
    2. Try pdfplumber first. If it returns clean digital text, short-
       circuit and return with engine_used='pdfplumber'.
    3. Otherwise, run the full ``quality`` matrix of (preprocess × engine)
       combinations in a thread pool.
    4. Reconcile the variants via ``build_consensus`` and return.
    5. Save the result to the cache (best-effort).

    Args:
        pdf_path: Path to the PDF file.
        quality: 'fast' (2 engines) | 'standard' (up to 11) | 'deep' (13 + vision_api).
        use_cache: If True, read/write the per-PDF-sha1 cache.
        base_dir: Base directory for the cache (default: repo root).

    Returns:
        A ``MultiOcrResult`` (possibly empty on failure).
    """
    if not os.path.exists(pdf_path):
        return _empty_result()

    pdf_sha1: Optional[str] = None
    if use_cache:
        try:
            pdf_sha1 = _pdf_sha1(pdf_path)
            cached = cache_load(pdf_sha1, base_dir=base_dir)
            if cached is not None:
                return cached
        except Exception as e:
            logger.debug(f"cache check failed for {pdf_path}: {e}")

    # 1. Digital short-circuit
    digital_text = engine_pdfplumber(pdf_path)
    if _is_clean_digital(digital_text):
        variants = {"pdfplumber": digital_text}
        token_map = build_token_map(variants)
        result = MultiOcrResult(
            text=digital_text,
            variants=variants,
            token_map=token_map,
            confidence=1.0,
            method_stats={"pdfplumber": len(digital_text)},
            page_count=1,
            engine_used="pdfplumber",
        )
        if use_cache and pdf_sha1:
            cache_save(pdf_sha1, result, base_dir=base_dir)
        return result

    # 2. Full hybrid matrix
    variant_texts = run_all_variants(pdf_path, quality=quality)
    if digital_text and "pdfplumber" not in variant_texts:
        variant_texts["pdfplumber"] = digital_text

    if not variant_texts:
        result = _empty_result()
    else:
        result = build_consensus(variant_texts)

    if use_cache and pdf_sha1:
        cache_save(pdf_sha1, result, base_dir=base_dir)
    return result


# ─── CLI ───────────────────────────────────────────────────────────────


def _emit_progress(stage: str, message: str, item: int = 0, total: int = 0) -> None:
    payload = {"stage": stage, "message": message, "item": item, "total": total}
    print(f"PROGRESS:{json.dumps(payload)}", flush=True)


def _emit_result(
    status: str,
    method: str,
    char_count: int,
    output_path: str,
    error: str = "",
) -> None:
    result = {
        "status": status,
        "method": method,
        "char_count": char_count,
        "output": output_path,
        "error": error,
    }
    print(json.dumps(result), flush=True)


def _cli_main(argv: Optional[List[str]] = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Unified hybrid OCR for AutoInvoice2XLSX"
    )
    parser.add_argument("--input", required=True, help="Input PDF file")
    parser.add_argument("--output", required=True, help="Output text file")
    parser.add_argument(
        "--quality",
        choices=("fast", "standard", "deep"),
        default="standard",
    )
    parser.add_argument(
        "--no-cache", action="store_true", help="Skip the per-PDF cache"
    )
    args = parser.parse_args(argv)

    if not os.path.isfile(args.input):
        _emit_result(
            "error", "none", 0, args.output, f"File not found: {args.input}"
        )
        return 1

    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)
    _emit_progress("extract_text", f"Running multi_ocr ({args.quality})")
    result = extract_text(
        args.input, quality=args.quality, use_cache=not args.no_cache
    )

    with open(args.output, "w", encoding="utf-8") as f:
        f.write(result.text)

    if result.text:
        _emit_result(
            "success", result.engine_used, len(result.text), args.output
        )
        return 0
    _emit_result("error", "none", 0, args.output, "all OCR paths produced no text")
    return 2


if __name__ == "__main__":
    sys.exit(_cli_main())
