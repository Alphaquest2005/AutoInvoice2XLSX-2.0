"""TDD tests for XLSX specification domain models."""

from __future__ import annotations

import pytest

from autoinvoice.domain.models.xlsx_spec import CellStyle, ColumnDef, RowType, SheetSpec


class TestRowType:
    def test_enum_values(self) -> None:
        assert RowType.GROUP_HEADER == "group_header"
        assert RowType.DETAIL == "detail"
        assert RowType.SUBTOTAL == "subtotal"
        assert RowType.GRAND_TOTAL == "grand_total"
        assert RowType.HEADER == "header"
        assert RowType.BLANK == "blank"

    def test_string_comparison(self) -> None:
        assert RowType.DETAIL == "detail"
        assert str(RowType.DETAIL) == "detail"


class TestCellStyle:
    def test_defaults(self) -> None:
        style = CellStyle()
        assert style.bold is False
        assert style.italic is False
        assert style.font_size == 10
        assert style.font_color == "000000"
        assert style.bg_color == ""
        assert style.alignment == "left"
        assert style.number_format == ""
        assert style.border is False

    def test_custom_style(self) -> None:
        style = CellStyle(bold=True, font_size=14, bg_color="FFFF00", alignment="center")
        assert style.bold is True
        assert style.font_size == 14
        assert style.bg_color == "FFFF00"
        assert style.alignment == "center"

    def test_frozen_immutability(self) -> None:
        style = CellStyle()
        with pytest.raises(AttributeError):
            style.bold = True  # type: ignore[misc]


class TestColumnDef:
    def test_create_with_required_fields(self) -> None:
        col = ColumnDef(name="Description", index=12)
        assert col.name == "Description"
        assert col.index == 12

    def test_defaults(self) -> None:
        col = ColumnDef(name="X", index=1)
        assert col.data_type == "string"
        assert col.width == 12
        assert col.formula == ""

    def test_frozen_immutability(self) -> None:
        col = ColumnDef(name="X", index=1)
        with pytest.raises(AttributeError):
            col.name = "Y"  # type: ignore[misc]


class TestSheetSpec:
    def test_create_with_columns(self) -> None:
        cols = (
            ColumnDef(name="Qty", index=1, data_type="number"),
            ColumnDef(name="Desc", index=2),
        )
        spec = SheetSpec(columns=cols)
        assert len(spec.columns) == 2
        assert spec.columns[0].name == "Qty"

    def test_defaults(self) -> None:
        spec = SheetSpec(columns=())
        assert spec.sheet_name == "Invoice"
        assert spec.start_row == 1

    def test_frozen_immutability(self) -> None:
        spec = SheetSpec(columns=())
        with pytest.raises(AttributeError):
            spec.sheet_name = "Other"  # type: ignore[misc]
