"""PDF text extraction adapter using PyMuPDF (fitz)."""

from __future__ import annotations


class PyMuPDFExtractor:
    """Extract text from PDFs using the PyMuPDF library."""

    def extract_text(self, pdf_path: str) -> str:
        """Extract text from all pages of a PDF.

        Args:
            pdf_path: Absolute path to the PDF file.

        Returns:
            Concatenated text from all pages, separated by newlines.
        """
        import fitz

        text_parts: list[str] = []
        doc = fitz.open(pdf_path)
        for page in doc:
            text_parts.append(page.get_text())
        doc.close()
        return "\n".join(text_parts)

    def extract_with_ocr(self, pdf_path: str) -> str:
        """Fall back to standard text extraction (no OCR support)."""
        return self.extract_text(pdf_path)
