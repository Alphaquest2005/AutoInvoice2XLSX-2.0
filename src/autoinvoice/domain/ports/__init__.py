"""Domain ports - Protocol interfaces for hexagonal architecture."""

from __future__ import annotations

from autoinvoice.domain.ports.classification_store import ClassificationStorePort
from autoinvoice.domain.ports.code_repository import CodeRepositoryPort
from autoinvoice.domain.ports.config_provider import ConfigProviderPort
from autoinvoice.domain.ports.email_gateway import EmailGatewayPort
from autoinvoice.domain.ports.file_system import FileSystemPort
from autoinvoice.domain.ports.llm_client import LlmClientPort
from autoinvoice.domain.ports.pdf_extractor import PdfExtractorPort
from autoinvoice.domain.ports.xlsx_writer import XlsxWriterPort

__all__ = [
    "ClassificationStorePort",
    "CodeRepositoryPort",
    "ConfigProviderPort",
    "EmailGatewayPort",
    "FileSystemPort",
    "LlmClientPort",
    "PdfExtractorPort",
    "XlsxWriterPort",
]
