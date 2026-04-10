"""In-memory fake for PdfExtractorPort."""

from __future__ import annotations


class FakePdfExtractor:
    """Fake PDF extractor that returns pre-configured text by file path."""

    def __init__(self, texts: dict[str, str] | None = None) -> None:
        self._texts = texts or {}

    def extract_text(self, pdf_path: str) -> str:
        """Return pre-configured text for the given path, or raise ValueError."""
        if pdf_path not in self._texts:
            raise ValueError(f"No text configured for path: {pdf_path}")
        return self._texts[pdf_path]

    def extract_with_ocr(self, pdf_path: str) -> str:
        """Return pre-configured text for the given path, or raise ValueError."""
        if pdf_path not in self._texts:
            raise ValueError(f"No text configured for path: {pdf_path}")
        return self._texts[pdf_path]
