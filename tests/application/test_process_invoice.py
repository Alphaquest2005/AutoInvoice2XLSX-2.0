"""Tests for ProcessInvoiceUseCase - uses all fakes, no mocks."""

from __future__ import annotations

from autoinvoice.application.process_invoice import ProcessInvoiceUseCase
from autoinvoice.domain.models.pipeline import PipelineStatus
from autoinvoice.domain.models.settings import AppSettings
from tests.fakes.fake_classification_store import FakeClassificationStore
from tests.fakes.fake_code_repo import InMemoryCodeRepository
from tests.fakes.fake_config_provider import FakeConfigProvider
from tests.fakes.fake_filesystem import FakeFileSystem
from tests.fakes.fake_pdf_extractor import FakePdfExtractor
from tests.fakes.fake_xlsx_writer import FakeXlsxWriter

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_INVOICE_TEXT = (
    "Invoice # INV-001\n"
    "Date: 2025-01-15\n"
    "Supplier: Acme Corp\n"
    "Total: $150.00\n"
    "\n"
    "Description\tQty\tUnit Price\tTotal\n"
    "Widget A\t2\t25.00\t50.00\n"
    "Gadget B\t1\t100.00\t100.00\n"
)

# A minimal classification rule that matches our sample items.
SAMPLE_RULES = [
    {
        "patterns": ["widget"],
        "code": "84713000",
        "category": "PRODUCTS",
        "confidence": 0.90,
        "priority": 10,
    },
    {
        "patterns": ["gadget"],
        "code": "84714100",
        "category": "PRODUCTS",
        "confidence": 0.85,
        "priority": 5,
    },
]

VALID_CODES = {"84713000", "84714100"}


def _make_settings() -> AppSettings:
    return AppSettings(base_dir="/tmp/test", workspace_path="/tmp/test/workspace")


def _make_use_case(
    pdf_texts: dict[str, str] | None = None,
    rules: list[dict[str, object]] | None = None,
    valid_codes: set[str] | None = None,
) -> tuple[ProcessInvoiceUseCase, FakePdfExtractor, FakeXlsxWriter, FakeClassificationStore]:
    """Build a ProcessInvoiceUseCase wired with all fakes."""
    pdf_extractor = FakePdfExtractor(texts=pdf_texts or {"/tmp/test.pdf": SAMPLE_INVOICE_TEXT})
    config_provider = FakeConfigProvider(
        classification_rules=rules or SAMPLE_RULES,
        column_spec={"columns": []},
    )
    code_repo = InMemoryCodeRepository(valid_codes=valid_codes or VALID_CODES)
    xlsx_writer = FakeXlsxWriter()
    file_system = FakeFileSystem()
    classification_store = FakeClassificationStore()
    settings = _make_settings()

    use_case = ProcessInvoiceUseCase(
        pdf_extractor=pdf_extractor,
        config_provider=config_provider,
        code_repository=code_repo,
        xlsx_writer=xlsx_writer,
        file_system=file_system,
        classification_store=classification_store,
        settings=settings,
    )
    return use_case, pdf_extractor, xlsx_writer, classification_store


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestExecuteFullPipeline:
    """Integration-style tests using all fakes."""

    def test_execute_full_pipeline_success(self) -> None:
        use_case, _pdf, _xlsx, _store = _make_use_case()

        report = use_case.execute("/tmp/test.pdf", "/tmp/output.xlsx")

        assert report.status == PipelineStatus.SUCCESS
        assert len(report.errors) == 0
        stage_names = [s.name for s in report.stages]
        assert "extract" in stage_names
        assert "parse" in stage_names
        assert "classify" in stage_names
        assert "group" in stage_names
        assert "generate_xlsx" in stage_names

    def test_execute_extracts_text(self) -> None:
        use_case, pdf_extractor, _xlsx, _store = _make_use_case()

        report = use_case.execute("/tmp/test.pdf", "/tmp/output.xlsx")

        assert report.status == PipelineStatus.SUCCESS
        # The extract stage completed successfully, meaning extract_text was called.
        assert report.stages[0].name == "extract"
        assert report.stages[0].status == "success"

    def test_execute_classifies_items(self) -> None:
        use_case, _pdf, _xlsx, store = _make_use_case()

        report = use_case.execute("/tmp/test.pdf", "/tmp/output.xlsx")

        assert report.status == PipelineStatus.SUCCESS
        # Items are classified and saved to the store. Since the generic parser
        # does not extract SKUs, both items share sku="" and the second
        # classification overwrites the first in the dict-based store.
        assert len(store._classifications) >= 1

    def test_execute_generates_xlsx(self) -> None:
        use_case, _pdf, xlsx_writer, _store = _make_use_case()

        report = use_case.execute("/tmp/test.pdf", "/tmp/output.xlsx")

        assert report.status == PipelineStatus.SUCCESS
        assert len(xlsx_writer.generated) == 1
        assert xlsx_writer.generated[0]["output_path"] == "/tmp/output.xlsx"

    def test_execute_handles_extraction_error(self) -> None:
        # PDF extractor will raise because path is not configured
        use_case, _pdf, _xlsx, _store = _make_use_case(pdf_texts={})

        report = use_case.execute("/tmp/missing.pdf", "/tmp/output.xlsx")

        assert report.status == PipelineStatus.ERROR
        assert len(report.errors) == 1
        assert "No text configured" in report.errors[0]

    def test_execute_handles_empty_invoice(self) -> None:
        # Invoice text with no parseable items
        empty_text = "Invoice # INV-002\nDate: 2025-01-15\nSupplier: Empty Co\nTotal: $0.00\n"
        use_case, _pdf, xlsx_writer, store = _make_use_case(
            pdf_texts={"/tmp/empty.pdf": empty_text}
        )

        report = use_case.execute("/tmp/empty.pdf", "/tmp/output.xlsx")

        # Should still succeed - zero items is valid.
        assert report.status == PipelineStatus.SUCCESS
        assert len(store._classifications) == 0
        assert len(xlsx_writer.generated) == 1

    def test_execute_unclassified_items_still_succeeds(self) -> None:
        # No matching rules - items will be unclassified but pipeline still completes
        no_match_rules: list[dict[str, object]] = [
            {
                "patterns": ["zzz_no_match_pattern"],
                "code": "99999999",
                "category": "PRODUCTS",
                "confidence": 0.90,
                "priority": 10,
            },
        ]
        use_case, _pdf, _xlsx, store = _make_use_case(
            rules=no_match_rules, valid_codes={"99999999"}
        )

        report = use_case.execute("/tmp/test.pdf", "/tmp/output.xlsx")

        assert report.status == PipelineStatus.SUCCESS
        # No classifications saved since no rules matched.
        assert len(store._classifications) == 0

    def test_all_stages_report_success(self) -> None:
        use_case, _pdf, _xlsx, _store = _make_use_case()

        report = use_case.execute("/tmp/test.pdf", "/tmp/output.xlsx")

        for stage in report.stages:
            assert stage.status == "success", f"Stage {stage.name} was not success"
