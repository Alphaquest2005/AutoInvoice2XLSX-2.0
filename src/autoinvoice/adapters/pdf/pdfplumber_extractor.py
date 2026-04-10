"""PDF text extraction adapter using pdfplumber."""

from __future__ import annotations


class PdfplumberExtractor:
    """Extract text from PDFs with embedded text layers using pdfplumber."""

    def extract_text(self, pdf_path: str) -> str:
        """Extract text from all pages of a PDF.

        Args:
            pdf_path: Absolute path to the PDF file.

        Returns:
            Concatenated text from all pages, separated by newlines.
        """
        import pdfplumber

        text_parts: list[str] = []
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
        return "\n".join(text_parts)

    def extract_with_ocr(self, pdf_path: str) -> str:
        """Fall back to standard text extraction (pdfplumber has no OCR)."""
        return self.extract_text(pdf_path)
