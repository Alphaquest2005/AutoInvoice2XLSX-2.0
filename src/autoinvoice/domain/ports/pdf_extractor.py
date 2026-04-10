"""Port for PDF text extraction."""

from __future__ import annotations

from typing import Protocol


class PdfExtractorPort(Protocol):
    """Interface for extracting text content from PDF files."""

    def extract_text(self, pdf_path: str) -> str:
        """Extract text from a PDF using standard text parsing.

        Args:
            pdf_path: Absolute path to the PDF file.

        Returns:
            Extracted text content.
        """
        ...

    def extract_with_ocr(self, pdf_path: str) -> str:
        """Extract text from a PDF using OCR for scanned documents.

        Args:
            pdf_path: Absolute path to the PDF file.

        Returns:
            OCR-extracted text content.
        """
        ...
