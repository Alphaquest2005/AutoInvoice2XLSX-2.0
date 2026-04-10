"""In-memory fake for XlsxWriterPort."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from autoinvoice.domain.models.grouping import GroupedInvoice
    from autoinvoice.domain.models.xlsx_spec import SheetSpec


class FakeXlsxWriter:
    """Fake XLSX writer that records calls instead of producing files."""

    def __init__(self) -> None:
        self.generated: list[dict[str, Any]] = []
        self.validated: list[str] = []

    def generate(
        self,
        invoices: list[GroupedInvoice],
        output_path: str,
        column_spec: SheetSpec,
    ) -> str:
        """Record the generation call and return the output path."""
        self.generated.append(
            {
                "invoices": invoices,
                "output_path": output_path,
                "column_spec": column_spec,
            }
        )
        return output_path

    def validate(self, xlsx_path: str) -> dict[str, Any]:
        """Record the validation call and return a passing result."""
        self.validated.append(xlsx_path)
        return {"valid": True, "variance": 0.0}
