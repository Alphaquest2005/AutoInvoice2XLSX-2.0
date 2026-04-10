"""Tests for PDF extractor adapters."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

from autoinvoice.adapters.pdf.composite_extractor import CompositePdfExtractor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class FakePdfExtractor:
    """Fake extractor for testing the composite without mocking."""

    def __init__(self, text: str = "", ocr_text: str | None = None):
        self._text = text
        self._ocr_text = ocr_text if ocr_text is not None else text

    def extract_text(self, pdf_path: str) -> str:
        return self._text

    def extract_with_ocr(self, pdf_path: str) -> str:
        return self._ocr_text


class FailingExtractor:
    """Extractor that always raises."""

    def extract_text(self, pdf_path: str) -> str:
        raise RuntimeError("extraction failed")

    def extract_with_ocr(self, pdf_path: str) -> str:
        raise RuntimeError("ocr failed")


# ---------------------------------------------------------------------------
# TestPdfplumberExtractor
# ---------------------------------------------------------------------------


class TestPdfplumberExtractor:
    def _make_mock_pdfplumber(
        self,
        page_texts: list[str | None],
    ) -> MagicMock:
        mock_lib = MagicMock()
        pages = []
        for text in page_texts:
            p = MagicMock()
            p.extract_text.return_value = text
            pages.append(p)
        mock_pdf = MagicMock()
        mock_pdf.pages = pages
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        mock_lib.open.return_value = mock_pdf
        return mock_lib

    def test_extract_text_returns_string(self) -> None:
        mock_lib = self._make_mock_pdfplumber(["Invoice #1234", "Total: $100.00"])
        with patch.dict(sys.modules, {"pdfplumber": mock_lib}):
            from autoinvoice.adapters.pdf.pdfplumber_extractor import PdfplumberExtractor

            extractor = PdfplumberExtractor()
            result = extractor.extract_text("/fake/invoice.pdf")
        assert result == "Invoice #1234\nTotal: $100.00"

    def test_extract_with_ocr_delegates_to_extract_text(self) -> None:
        mock_lib = self._make_mock_pdfplumber(["Some text"])
        with patch.dict(sys.modules, {"pdfplumber": mock_lib}):
            from autoinvoice.adapters.pdf.pdfplumber_extractor import PdfplumberExtractor

            extractor = PdfplumberExtractor()
            result = extractor.extract_with_ocr("/fake/invoice.pdf")
        assert result == "Some text"

    def test_extract_text_empty_pdf_returns_empty(self) -> None:
        mock_lib = self._make_mock_pdfplumber([None])
        with patch.dict(sys.modules, {"pdfplumber": mock_lib}):
            from autoinvoice.adapters.pdf.pdfplumber_extractor import PdfplumberExtractor

            extractor = PdfplumberExtractor()
            result = extractor.extract_text("/fake/empty.pdf")
        assert result == ""


# ---------------------------------------------------------------------------
# TestPyMuPDFExtractor
# ---------------------------------------------------------------------------


class TestPyMuPDFExtractor:
    def test_extract_text_returns_string(self) -> None:
        mock_page = MagicMock()
        mock_page.get_text.return_value = "PyMuPDF extracted text"

        mock_doc = MagicMock()
        mock_doc.__iter__ = MagicMock(return_value=iter([mock_page]))

        mock_fitz = MagicMock()
        mock_fitz.open.return_value = mock_doc

        with patch.dict(sys.modules, {"fitz": mock_fitz}):
            from autoinvoice.adapters.pdf.pymupdf_extractor import PyMuPDFExtractor

            extractor = PyMuPDFExtractor()
            result = extractor.extract_text("/fake/invoice.pdf")

        assert result == "PyMuPDF extracted text"
        mock_doc.close.assert_called_once()

    def test_extract_text_handles_missing_file(self) -> None:
        mock_fitz = MagicMock()
        mock_fitz.open.side_effect = FileNotFoundError("not found")

        with patch.dict(sys.modules, {"fitz": mock_fitz}):
            from autoinvoice.adapters.pdf.pymupdf_extractor import PyMuPDFExtractor

            extractor = PyMuPDFExtractor()

            with pytest.raises(FileNotFoundError):
                extractor.extract_text("/fake/missing.pdf")


# ---------------------------------------------------------------------------
# TestTesseractExtractor
# ---------------------------------------------------------------------------


class TestTesseractExtractor:
    def test_extract_with_ocr_returns_string(self) -> None:
        mock_pix = MagicMock()
        mock_pix.tobytes.return_value = b"fake-png-bytes"

        mock_page = MagicMock()
        mock_page.get_pixmap.return_value = mock_pix

        mock_doc = MagicMock()
        mock_doc.__iter__ = MagicMock(return_value=iter([mock_page]))

        mock_fitz = MagicMock()
        mock_fitz.open.return_value = mock_doc

        mock_tess = MagicMock()
        mock_tess.image_to_string.return_value = "OCR extracted text"

        mock_pil = MagicMock()
        mock_img = MagicMock()
        mock_pil.Image.open.return_value = mock_img

        with patch.dict(
            sys.modules,
            {
                "fitz": mock_fitz,
                "pytesseract": mock_tess,
                "PIL": mock_pil,
                "PIL.Image": mock_pil.Image,
            },
        ):
            from autoinvoice.adapters.pdf.tesseract_extractor import TesseractExtractor

            extractor = TesseractExtractor()
            result = extractor.extract_with_ocr("/fake/scanned.pdf")

        assert result == "OCR extracted text"
        mock_doc.close.assert_called_once()


# ---------------------------------------------------------------------------
# TestCompositeExtractor
# ---------------------------------------------------------------------------


class TestCompositeExtractor:
    def test_tries_pdfplumber_first(self) -> None:
        long_text = "A" * 60  # above MIN_TEXT_LENGTH
        first = FakePdfExtractor(text=long_text)
        second = FakePdfExtractor(text="should not be used")
        composite = CompositePdfExtractor(extractors=[first, second])

        result = composite.extract_text("/fake/invoice.pdf")
        assert result == long_text

    def test_falls_back_to_pymupdf(self) -> None:
        long_text = "B" * 60
        first = FakePdfExtractor(text="")  # pdfplumber returns empty
        second = FakePdfExtractor(text=long_text)
        third = FakePdfExtractor(text="should not reach")
        composite = CompositePdfExtractor(extractors=[first, second, third])

        result = composite.extract_text("/fake/invoice.pdf")
        assert result == long_text

    def test_falls_back_to_tesseract(self) -> None:
        long_text = "C" * 60
        first = FakePdfExtractor(text="")
        second = FakePdfExtractor(text="short")  # below MIN_TEXT_LENGTH
        third = FakePdfExtractor(text=long_text)
        composite = CompositePdfExtractor(extractors=[first, second, third])

        result = composite.extract_text("/fake/invoice.pdf")
        assert result == long_text

    def test_all_fail_returns_empty(self) -> None:
        first = FakePdfExtractor(text="")
        second = FakePdfExtractor(text="tiny")
        third = FailingExtractor()
        composite = CompositePdfExtractor(extractors=[first, second, third])

        result = composite.extract_text("/fake/invoice.pdf")
        assert result == ""

    def test_extract_with_ocr_uses_ocr_fallback(self) -> None:
        long_ocr = "D" * 60
        first = FakePdfExtractor(text="", ocr_text=long_ocr)
        composite = CompositePdfExtractor(extractors=[first])

        result = composite.extract_with_ocr("/fake/scanned.pdf")
        assert result == long_ocr

    def test_extract_with_ocr_all_fail_returns_empty(self) -> None:
        first = FakePdfExtractor(text="", ocr_text="")
        second = FailingExtractor()
        composite = CompositePdfExtractor(extractors=[first, second])

        result = composite.extract_with_ocr("/fake/scanned.pdf")
        assert result == ""
