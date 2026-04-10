"""CLI entry point for AutoInvoice2XLSX pipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from autoinvoice.composition.container import Container


def main() -> None:
    """Parse arguments, build container, execute pipeline."""
    import argparse
    import json
    import os
    from dataclasses import asdict

    from autoinvoice.composition.container import Container
    from autoinvoice.domain.models.settings import AppSettings

    parser = argparse.ArgumentParser(description="AutoInvoice2XLSX Pipeline")
    parser.add_argument("--input", help="Input PDF path")
    parser.add_argument("--output", help="Output XLSX path")
    parser.add_argument("--base-dir", default=os.getcwd(), help="Project base directory")
    parser.add_argument("--json-output", action="store_true")

    # Validation mode
    parser.add_argument("--validate", metavar="XLSX_PATH", help="Validate an XLSX file")

    # Extraction modes
    parser.add_argument("--extract-text", action="store_true", help="Extract text from PDF")
    parser.add_argument("--extract-ocr", action="store_true", help="Extract text via OCR")

    # LLM overrides
    parser.add_argument("--api-key", help="LLM API key override")
    parser.add_argument("--base-url", help="LLM base URL override")
    parser.add_argument("--model", help="LLM model override")
    parser.add_argument("--ocr-config", help="OCR configuration override")

    args = parser.parse_args()

    # AppSettings is a frozen dataclass — build it once with all overrides.
    settings_kwargs: dict[str, Any] = {
        "base_dir": args.base_dir,
        "workspace_path": os.path.join(args.base_dir, "workspace"),
    }
    if args.api_key:
        settings_kwargs["llm_api_key"] = args.api_key
    if args.base_url:
        settings_kwargs["llm_base_url"] = args.base_url
    if args.model:
        settings_kwargs["llm_model"] = args.model
    settings = AppSettings(**settings_kwargs)

    container = Container(settings=settings)

    # ── Validate mode ──────────────────────────────────────────────────────
    if args.validate:
        validate_result = _run_validate(container, args.validate)
        if args.json_output:
            print(f"REPORT:JSON:{json.dumps(validate_result, default=str)}")
        else:
            print(json.dumps(validate_result, indent=2, default=str))
        return

    # ── Extract text mode ──────────────────────────────────────────────────
    if args.extract_text:
        if not args.input:
            parser.error("--extract-text requires --input")
        extract_result = _run_extract_text(container, args.input, use_ocr=False)
        if args.json_output:
            print(f"REPORT:JSON:{json.dumps(extract_result, default=str)}")
        else:
            print(str(extract_result.get("text", "")))
        return

    # ── Extract OCR mode ───────────────────────────────────────────────────
    if args.extract_ocr:
        if not args.input:
            parser.error("--extract-ocr requires --input")
        extract_result = _run_extract_text(container, args.input, use_ocr=True)
        if args.json_output:
            print(f"REPORT:JSON:{json.dumps(extract_result, default=str)}")
        else:
            print(str(extract_result.get("text", "")))
        return

    # ── Full pipeline mode ─────────────────────────────────────────────────
    if not args.input:
        parser.error("--input is required for pipeline mode")

    use_case = container.process_invoice_use_case()
    output_path = args.output or args.input.replace(".pdf", ".xlsx")
    report = use_case.execute(args.input, output_path)

    if args.json_output:
        report_dict = asdict(report)
        print(f"REPORT:JSON:{json.dumps(report_dict, default=str)}")
    else:
        print(f"Pipeline completed: {report.status}")
        for stage in report.stages:
            print(f"  {stage.name}: {stage.status} ({stage.duration_ms}ms)")


def _run_validate(container: Container, xlsx_path: str) -> dict[str, Any]:
    """Validate an XLSX file and return checks dict."""
    try:
        from autoinvoice.adapters.xlsx.openpyxl_reader import OpenpyxlXlsxReader

        reader = OpenpyxlXlsxReader()
        # Basic validation: check file opens and has expected structure
        data = reader.read_workbook(xlsx_path)
        checks: dict[str, Any] = {
            "file_exists": True,
            "sheets": len(data.get("sheets", [])) if isinstance(data, dict) else 0,
            "valid": True,
        }

        # Try to compute variance if the data has invoice totals
        if hasattr(data, "sheets") or isinstance(data, dict):
            checks["variance_check"] = 0.0  # Placeholder — full validation TBD

        return {"success": True, "checks": checks}
    except Exception as e:
        return {"success": False, "error": str(e), "checks": {"valid": False}}


def _run_extract_text(
    container: Container, input_path: str, use_ocr: bool = False
) -> dict[str, Any]:
    """Extract text from a PDF."""
    try:
        if use_ocr:
            from autoinvoice.adapters.pdf.tesseract_extractor import TesseractExtractor

            extractor: Any = TesseractExtractor()
        else:
            from autoinvoice.adapters.pdf.composite_extractor import CompositePdfExtractor

            extractor = CompositePdfExtractor()

        text = extractor.extract_text(input_path)
        return {"success": True, "text": text, "length": len(text)}
    except Exception as e:
        return {"success": False, "error": str(e), "text": ""}


if __name__ == "__main__":
    main()
