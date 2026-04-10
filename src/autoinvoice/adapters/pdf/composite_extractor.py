"""Composite PDF extractor that tries multiple extraction strategies."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Sequence


class CompositePdfExtractor:
    """Tries extraction methods in order: pdfplumber -> PyMuPDF -> Tesseract OCR.

    The composite accepts injected extractors for testability. When none are
    provided it lazily imports and instantiates the three concrete adapters.
    """

    MIN_TEXT_LENGTH = 50  # below this, consider extraction failed

    def __init__(self, extractors: Sequence[Any] | None = None) -> None:
        if extractors is not None:
            self._extractors = list(extractors)
        else:
            from .pdfplumber_extractor import PdfplumberExtractor
            from .pymupdf_extractor import PyMuPDFExtractor
            from .tesseract_extractor import TesseractExtractor

            self._extractors = [
                PdfplumberExtractor(),
                PyMuPDFExtractor(),
                TesseractExtractor(),
            ]

    def extract_text(self, pdf_path: str) -> str:
        """Try each extractor in order; return first result above threshold."""
        for extractor in self._extractors:
            try:
                text: str = extractor.extract_text(pdf_path)
                if len(text.strip()) >= self.MIN_TEXT_LENGTH:
                    return text
            except Exception:
                continue
        return ""

    def extract_with_ocr(self, pdf_path: str) -> str:
        """Try non-OCR first, then OCR as last resort."""
        text = self.extract_text(pdf_path)
        if len(text.strip()) >= self.MIN_TEXT_LENGTH:
            return text

        for extractor in self._extractors:
            try:
                text = str(extractor.extract_with_ocr(pdf_path))
                if len(text.strip()) >= self.MIN_TEXT_LENGTH:
                    return text
            except Exception:
                continue
        return ""
