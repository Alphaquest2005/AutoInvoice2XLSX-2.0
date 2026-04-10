"""Composition root: wire all adapters to ports."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from autoinvoice.application.process_invoice import ProcessInvoiceUseCase
    from autoinvoice.domain.models.settings import AppSettings
    from autoinvoice.domain.ports.classification_store import ClassificationStorePort
    from autoinvoice.domain.ports.code_repository import CodeRepositoryPort
    from autoinvoice.domain.ports.config_provider import ConfigProviderPort
    from autoinvoice.domain.ports.file_system import FileSystemPort
    from autoinvoice.domain.ports.llm_client import LlmClientPort
    from autoinvoice.domain.ports.pdf_extractor import PdfExtractorPort
    from autoinvoice.domain.ports.xlsx_writer import XlsxWriterPort


@dataclass
class Container:
    """Composition root: wire all adapters to ports."""

    settings: AppSettings

    @cached_property
    def config_provider(self) -> ConfigProviderPort:
        from autoinvoice.adapters.config.yaml_config_provider import YamlConfigProvider

        return YamlConfigProvider(self.settings.base_dir)

    @cached_property
    def file_system(self) -> FileSystemPort:
        from autoinvoice.adapters.storage.filesystem import OsFileSystem

        return OsFileSystem()

    @cached_property
    def pdf_extractor(self) -> PdfExtractorPort:
        from autoinvoice.adapters.pdf.composite_extractor import CompositePdfExtractor

        return CompositePdfExtractor()

    @cached_property
    def llm_client(self) -> LlmClientPort:
        from autoinvoice.adapters.llm.anthropic_client import AnthropicLlmClient

        return AnthropicLlmClient(
            api_key=self.settings.llm_api_key,
            base_url=self.settings.llm_base_url,
            model=self.settings.llm_model,
        )

    @cached_property
    def code_repository(self) -> CodeRepositoryPort:
        from autoinvoice.adapters.storage.sqlite_code_repo import SqliteCodeRepository

        return SqliteCodeRepository(
            db_path=os.path.join(self.settings.base_dir, "data", "cet.db"),
            invalid_codes=self.config_provider.load_invalid_codes(),
        )

    @cached_property
    def xlsx_writer(self) -> XlsxWriterPort:
        from autoinvoice.adapters.xlsx.openpyxl_writer import OpenpyxlXlsxWriter

        return OpenpyxlXlsxWriter()

    @cached_property
    def classification_store(self) -> ClassificationStorePort:
        from autoinvoice.adapters.storage.json_file_store import JsonClassificationStore

        return JsonClassificationStore(self.settings.base_dir)

    def process_invoice_use_case(self) -> ProcessInvoiceUseCase:
        from autoinvoice.application.process_invoice import ProcessInvoiceUseCase

        return ProcessInvoiceUseCase(
            pdf_extractor=self.pdf_extractor,
            config_provider=self.config_provider,
            code_repository=self.code_repository,
            xlsx_writer=self.xlsx_writer,
            file_system=self.file_system,
            classification_store=self.classification_store,
            settings=self.settings,
        )
