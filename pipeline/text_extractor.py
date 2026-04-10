#!/usr/bin/env python3
"""
Text extractor for PDF files with fallback chain:
1. pdfplumber (fast, digital PDFs)
2. PaddleOCR (offline OCR for scanned PDFs)
3. Z.AI Vision API (LLM-based extraction)

Usage:
    python text_extractor.py --input file.pdf --output file.txt [--api-key KEY] [--base-url URL] [--model MODEL]

Emits PROGRESS: JSON lines for real-time status updates.
Outputs final JSON result on the last line.
"""

import argparse
import json
import os
import sys
import base64
import io

# ─── Progress helpers ────────────────────────────────────────


def emit_progress(stage: str, message: str, item: int = 0, total: int = 0):
    payload = {"stage": stage, "message": message, "item": item, "total": total}
    print(f"PROGRESS:{json.dumps(payload)}", flush=True)


def emit_result(status: str, method: str, char_count: int, output_path: str, error: str = ""):
    result = {
        "status": status,
        "method": method,
        "char_count": char_count,
        "output": output_path,
        "error": error,
    }
    print(json.dumps(result), flush=True)


# ─── Method 1: pdfplumber ───────────────────────────────────


def extract_with_pdfplumber(pdf_path: str, output_path: str) -> tuple[bool, str, int]:
    """Extract text using pdfplumber. Returns (success, text, char_count)."""
    try:
        import pdfplumber
    except ImportError:
        return False, "pdfplumber not installed", 0

    emit_progress("extract_text", "Opening PDF with pdfplumber...", 0, 0)

    try:
        all_text = []
        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)
            for i, page in enumerate(pdf.pages):
                emit_progress("extract_text", f"Extracting page {i+1}/{total_pages} (pdfplumber)", i + 1, total_pages)
                text = page.extract_text() or ""
                all_text.append(text)

        full_text = "\n\n".join(all_text).strip()

        # Check if text is too sparse (likely scanned PDF)
        if len(full_text) < 50:
            return False, "Text too sparse, likely scanned PDF", len(full_text)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(full_text)

        return True, full_text, len(full_text)

    except Exception as e:
        return False, f"pdfplumber error: {str(e)}", 0


# ─── Method 2: PaddleOCR ────────────────────────────────────


def extract_with_paddleocr(pdf_path: str, output_path: str) -> tuple[bool, str, int]:
    """Extract text using PaddleOCR (pdf2image + paddleocr). Returns (success, text, char_count)."""
    try:
        from pdf2image import convert_from_path
        from paddleocr import PaddleOCR
    except ImportError:
        return False, "PaddleOCR or pdf2image not installed", 0

    emit_progress("extract_text", "Converting PDF to images for OCR...", 0, 0)

    try:
        images = convert_from_path(pdf_path, dpi=300)
        total_pages = len(images)

        ocr = PaddleOCR(use_angle_cls=True, lang="en", show_log=False)
        all_text = []

        for i, img in enumerate(images):
            emit_progress("extract_text", f"OCR page {i+1}/{total_pages} (PaddleOCR)", i + 1, total_pages)

            # Convert PIL image to numpy array
            import numpy as np
            img_array = np.array(img)
            result = ocr.ocr(img_array, cls=True)

            page_lines = []
            if result and result[0]:
                for line in result[0]:
                    if line and len(line) >= 2:
                        page_lines.append(line[1][0])  # Extract text from (bbox, (text, confidence))

            all_text.append("\n".join(page_lines))

        full_text = "\n\n".join(all_text).strip()

        if not full_text:
            return False, "PaddleOCR produced no text", 0

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(full_text)

        return True, full_text, len(full_text)

    except Exception as e:
        return False, f"PaddleOCR error: {str(e)}", 0


# ─── Method 3: Z.AI Vision API ──────────────────────────────


def extract_with_vision_api(
    pdf_path: str, output_path: str, api_key: str, base_url: str, model: str
) -> tuple[bool, str, int]:
    """Extract text by sending page images to Z.AI Vision API. Returns (success, text, char_count)."""
    if not api_key:
        return False, "No API key provided for vision fallback", 0

    try:
        import pdfplumber
    except ImportError:
        return False, "pdfplumber required for rendering pages", 0

    try:
        from anthropic import Anthropic
    except ImportError:
        return False, "anthropic SDK not installed", 0

    emit_progress("extract_text", "Preparing pages for vision API...", 0, 0)

    try:
        client = Anthropic(api_key=api_key, base_url=base_url)
        all_text = []

        with pdfplumber.open(pdf_path) as pdf:
            total_pages = len(pdf.pages)

            for i, page in enumerate(pdf.pages):
                emit_progress(
                    "extract_text",
                    f"Sending page {i+1}/{total_pages} to vision API",
                    i + 1,
                    total_pages,
                )

                # Render page to image
                img = page.to_image(resolution=200)
                buf = io.BytesIO()
                img.save(buf, format="PNG")
                b64_data = base64.b64encode(buf.getvalue()).decode("utf-8")

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
                                        "data": b64_data,
                                    },
                                },
                                {
                                    "type": "text",
                                    "text": "Extract ALL text from this invoice page. Return only the extracted text, preserving the layout as much as possible. Do not add commentary.",
                                },
                            ],
                        }
                    ],
                )

                page_text = response.content[0].text if response.content else ""
                all_text.append(page_text)

        full_text = "\n\n".join(all_text).strip()

        if not full_text:
            return False, "Vision API returned no text", 0

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(full_text)

        return True, full_text, len(full_text)

    except Exception as e:
        return False, f"Vision API error: {str(e)}", 0


# ─── Main ────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Extract text from PDF files")
    parser.add_argument("--input", required=True, help="Input PDF file path")
    parser.add_argument("--output", required=True, help="Output text file path")
    parser.add_argument("--api-key", default="", help="API key for vision fallback")
    parser.add_argument("--base-url", default="https://api.z.ai/api/anthropic", help="API base URL")
    parser.add_argument("--model", default="glm-5", help="Model for vision fallback")
    args = parser.parse_args()

    if not os.path.isfile(args.input):
        emit_result("error", "none", 0, args.output, f"Input file not found: {args.input}")
        sys.exit(1)

    # Ensure output directory exists
    os.makedirs(os.path.dirname(os.path.abspath(args.output)), exist_ok=True)

    # Fallback chain: pdfplumber → PaddleOCR → Vision API
    methods = [
        ("pdfplumber", lambda: extract_with_pdfplumber(args.input, args.output)),
        ("paddleocr", lambda: extract_with_paddleocr(args.input, args.output)),
        (
            "vision_api",
            lambda: extract_with_vision_api(
                args.input, args.output, args.api_key, args.base_url, args.model
            ),
        ),
    ]

    last_error = ""
    for method_name, extract_fn in methods:
        emit_progress("extract_text", f"Trying {method_name}...", 0, 0)
        success, message, char_count = extract_fn()

        if success:
            emit_progress("extract_text", f"Extraction complete ({method_name})", char_count, char_count)
            emit_result("success", method_name, char_count, args.output)
            sys.exit(0)
        else:
            last_error = message
            emit_progress("extract_text", f"{method_name} failed: {message}", 0, 0)

    # All methods failed
    emit_result("error", "none", 0, args.output, f"All extraction methods failed. Last: {last_error}")
    sys.exit(2)


if __name__ == "__main__":
    main()
