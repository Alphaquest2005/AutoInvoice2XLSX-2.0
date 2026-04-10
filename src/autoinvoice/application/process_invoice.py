"""Process Invoice use case - orchestrates the full pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from autoinvoice.domain.models.pipeline import PipelineReport, PipelineStatus, StageResult
from autoinvoice.domain.models.xlsx_spec import SheetSpec
from autoinvoice.domain.services.classifier import classify_items
from autoinvoice.domain.services.grouper import group_by_tariff
from autoinvoice.domain.services.parser import parse_invoice_text

if TYPE_CHECKING:
    from autoinvoice.domain.models.settings import AppSettings
    from autoinvoice.domain.ports.classification_store import ClassificationStorePort
    from autoinvoice.domain.ports.code_repository import CodeRepositoryPort
    from autoinvoice.domain.ports.config_provider import ConfigProviderPort
    from autoinvoice.domain.ports.file_system import FileSystemPort
    from autoinvoice.domain.ports.pdf_extractor import PdfExtractorPort
    from autoinvoice.domain.ports.xlsx_writer import XlsxWriterPort


@dataclass
class ProcessInvoiceUseCase:
    """Orchestrates the full invoice processing pipeline.

    All dependencies are injected via the constructor (ports only).
    """

    pdf_extractor: PdfExtractorPort
    config_provider: ConfigProviderPort
    code_repository: CodeRepositoryPort
    xlsx_writer: XlsxWriterPort
    file_system: FileSystemPort
    classification_store: ClassificationStorePort
    settings: AppSettings

    def execute(self, pdf_path: str, output_path: str) -> PipelineReport:
        """Run the full pipeline: extract -> parse -> classify -> group -> xlsx.

        Args:
            pdf_path: Path to the input PDF invoice.
            output_path: Path for the generated XLSX output.

        Returns:
            A PipelineReport with stage results and overall status.
        """
        stages: list[StageResult] = []
        try:
            # 1. Extract text from PDF
            text = self.pdf_extractor.extract_text(pdf_path)
            stages.append(StageResult(name="extract", status="success"))

            # 2. Parse invoice text into domain model
            invoice = parse_invoice_text(text)
            stages.append(StageResult(name="parse", status="success"))

            # 3. Classify line items
            rules = self.config_provider.load_classification_rules()
            result = classify_items(invoice.items, rules, self.code_repository)
            stages.append(StageResult(name="classify", status="success"))

            # 4. Group by tariff code
            grouped = group_by_tariff(invoice.metadata, result)
            stages.append(StageResult(name="group", status="success"))

            # 5. Generate XLSX output
            self.config_provider.load_column_spec()
            spec = SheetSpec(columns=())  # simplified for now
            self.xlsx_writer.generate([grouped], output_path, spec)
            stages.append(StageResult(name="generate_xlsx", status="success"))

            # 6. Persist classifications for future lookups
            for c in result.classifications:
                self.classification_store.save_classification(
                    c.item.sku, c.tariff_code.code, c.source
                )

            return PipelineReport(stages=tuple(stages), status=PipelineStatus.SUCCESS)

        except Exception as e:
            stages.append(StageResult(name="error", status="error", error=str(e)))
            return PipelineReport(
                stages=tuple(stages),
                status=PipelineStatus.ERROR,
                errors=(str(e),),
            )
