"""PDF text extraction adapter using Tesseract OCR."""

from __future__ import annotations


class TesseractExtractor:
    """Extract text from scanned/image-based PDFs using Tesseract OCR."""

    def extract_text(self, pdf_path: str) -> str:
        """Delegate to OCR extraction (Tesseract is inherently OCR-based)."""
        return self.extract_with_ocr(pdf_path)

    def extract_with_ocr(self, pdf_path: str) -> str:
        """Render each PDF page to an image and run Tesseract OCR.

        Args:
            pdf_path: Absolute path to the PDF file.

        Returns:
            OCR-extracted text from all pages, separated by newlines.
        """
        import io

        import fitz
        import pytesseract
        from PIL import Image

        text_parts: list[str] = []
        doc = fitz.open(pdf_path)
        for page in doc:
            pix = page.get_pixmap(dpi=300)
            img = Image.open(io.BytesIO(pix.tobytes("png")))
            text_parts.append(pytesseract.image_to_string(img))
        doc.close()
        return "\n".join(text_parts)
