"""XLSX specification domain models - column definitions and sheet layout."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class RowType(StrEnum):
    """Type of row in the XLSX output."""

    GROUP_HEADER = "group_header"
    DETAIL = "detail"
    SUBTOTAL = "subtotal"
    GRAND_TOTAL = "grand_total"
    HEADER = "header"
    BLANK = "blank"


@dataclass(frozen=True)
class CellStyle:
    """Style for a single cell."""

    bold: bool = False
    italic: bool = False
    font_size: int = 10
    font_color: str = "000000"
    bg_color: str = ""
    alignment: str = "left"
    number_format: str = ""
    border: bool = False


@dataclass(frozen=True)
class ColumnDef:
    """Single column definition from columns.yaml."""

    name: str
    index: int
    data_type: str = "string"  # string, number, currency, date
    width: int = 12
    header_style: CellStyle = CellStyle()
    group_style: CellStyle = CellStyle()
    detail_style: CellStyle = CellStyle()
    formula: str = ""


@dataclass(frozen=True)
class SheetSpec:
    """Complete sheet specification."""

    columns: tuple[ColumnDef, ...]
    sheet_name: str = "Invoice"
    start_row: int = 1
