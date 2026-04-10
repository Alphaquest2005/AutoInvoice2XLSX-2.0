"""Port for XLSX file generation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from autoinvoice.domain.models.grouping import GroupedInvoice
    from autoinvoice.domain.models.xlsx_spec import SheetSpec


class XlsxWriterPort(Protocol):
    """Interface for generating and validating XLSX workbooks."""

    def generate(
        self,
        invoices: list[GroupedInvoice],
        output_path: str,
        column_spec: SheetSpec,
    ) -> str:
        """Generate an XLSX file from grouped invoices.

        Args:
            invoices: Grouped invoices to write.
            output_path: Destination file path.
            column_spec: Column and sheet layout specification.

        Returns:
            Path to the generated XLSX file.
        """
        ...

    def validate(self, xlsx_path: str) -> dict[str, Any]:
        """Validate an XLSX file for structural correctness.

        Args:
            xlsx_path: Path to the XLSX file to validate.

        Returns:
            Validation results with any errors or warnings.
        """
        ...
