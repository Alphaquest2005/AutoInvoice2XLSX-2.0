"""PDF extractor adapters."""

from __future__ import annotations

from .composite_extractor import CompositePdfExtractor
from .pdfplumber_extractor import PdfplumberExtractor
from .pymupdf_extractor import PyMuPDFExtractor
from .tesseract_extractor import TesseractExtractor

__all__ = [
    "CompositePdfExtractor",
    "PdfplumberExtractor",
    "PyMuPDFExtractor",
    "TesseractExtractor",
]
