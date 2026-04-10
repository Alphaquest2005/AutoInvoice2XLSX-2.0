#!/usr/bin/env python3
"""
Spec Loader — Single Source of Truth for XLSX column and grouping configuration.

Loads config/columns.yaml and config/grouping.yaml once, then provides
a clean API for bl_xlsx_generator.py to query column definitions, styles,
formulas, and totals structure without hardcoding any of these values.

Usage:
    from spec_loader import spec
    # spec is a module-level singleton, loaded once on first import

    headers = spec.headers          # ['Document Type', 'PO Number', ...]
    idx = spec.col_index('F')       # 6
    width = spec.col_width('F')     # 12
    fmt = spec.currency_format      # '#,##0.00'
"""

import os
import logging
from typing import Any, Dict, List, Optional, Tuple

import yaml

logger = logging.getLogger(__name__)

_CONFIG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config')


class SpecLoader:
    """Loads columns.yaml and grouping.yaml, provides column mappings and styles."""

    def __init__(self, config_dir: str = None):
        config_dir = config_dir or _CONFIG_DIR
        self._columns_cfg = self._load_yaml(os.path.join(config_dir, 'columns.yaml'))
        self._grouping_cfg = self._load_yaml(os.path.join(config_dir, 'grouping.yaml'))
        self._build_lookups()

    @staticmethod
    def _load_yaml(path: str) -> dict:
        with open(path, 'r') as f:
            return yaml.safe_load(f)

    def _build_lookups(self):
        """Build fast lookup dicts from the YAML column definitions."""
        cols = self._columns_cfg.get('columns', {})

        # Ordered list of (letter, config) sorted by index
        self._col_list: List[Tuple[str, dict]] = sorted(
            cols.items(), key=lambda x: x[1].get('index', 999)
        )

        # letter -> config
        self._by_letter: Dict[str, dict] = {letter: cfg for letter, cfg in self._col_list}

        # name -> letter (e.g. "TariffCode" -> "F")
        self._name_to_letter: Dict[str, str] = {}
        for letter, cfg in self._col_list:
            name = cfg.get('name', '')
            if name:
                self._name_to_letter[name] = letter

        # Build headers list
        self._headers: List[str] = [cfg.get('header', '') for _, cfg in self._col_list]

        # Build widths dict {index: width}
        self._widths: Dict[int, int] = {}
        for letter, cfg in self._col_list:
            w = cfg.get('width')
            if w:
                self._widths[cfg['index']] = w

        # Build currency column lists
        currency_cfg = self._columns_cfg.get('currency', {})
        self._currency_format = currency_cfg.get('format', '#,##0.00')
        self._currency_all = [self.col_index(c) for c in currency_cfg.get('all_rows', [])]
        self._currency_detail = [self.col_index(c) for c in currency_cfg.get('detail_only', [])]

        # Styles
        styles = self._columns_cfg.get('styles', {})
        self._header_style = styles.get('header', {})
        self._group_style_cfg = styles.get('group', {})
        self._detail_style_cfg = styles.get('detail', {})
        self._border_style = styles.get('border', {})

        # Grouping config
        self._group_row_cfg = self._grouping_cfg.get('group_row', {})
        self._detail_row_cfg = self._grouping_cfg.get('detail_row', {})
        self._first_group_cfg = self._grouping_cfg.get('first_group_per_invoice', {})
        self._totals_cfg = self._grouping_cfg.get('totals_section', {})
        self._ungrouped_totals_cfg = self._grouping_cfg.get('ungrouped_totals_section', {})

        # Category name mappings
        cat_cfg = self._grouping_cfg.get('category_names', {})
        self._category_mappings = cat_cfg.get('mappings', {})
        self._category_default = cat_cfg.get('default', 'PRODUCTS')
        self._category_format = cat_cfg.get('format', '${category_name} (${item_count} items)')

        # Formula templates
        self._formulas = self._columns_cfg.get('formula_templates', {})

    # ─── Column Access ──────────────────────────────────────

    def _resolve_letter(self, letter_or_name: str) -> str:
        """Resolve a column letter or name to a letter."""
        if letter_or_name in self._by_letter:
            return letter_or_name
        if letter_or_name in self._name_to_letter:
            return self._name_to_letter[letter_or_name]
        raise KeyError(f"Unknown column: {letter_or_name}")

    def col_index(self, letter_or_name: str) -> int:
        """Get 1-based column index for a letter or name."""
        letter = self._resolve_letter(letter_or_name)
        return self._by_letter[letter]['index']

    def col_letter(self, index: int) -> str:
        """Get column letter for a 1-based index."""
        for letter, cfg in self._col_list:
            if cfg['index'] == index:
                return letter
        raise KeyError(f"No column with index {index}")

    def col_header(self, letter_or_name: str) -> str:
        """Get the display header for a column."""
        letter = self._resolve_letter(letter_or_name)
        return self._by_letter[letter].get('header', '')

    def col_config(self, letter_or_name: str) -> dict:
        """Get the full config dict for a column."""
        letter = self._resolve_letter(letter_or_name)
        return self._by_letter[letter]

    def col_width(self, letter_or_name: str) -> Optional[int]:
        """Get column width."""
        letter = self._resolve_letter(letter_or_name)
        return self._by_letter[letter].get('width')

    def col_count(self) -> int:
        """Total number of columns."""
        return len(self._col_list)

    # ─── Headers ────────────────────────────────────────────

    @property
    def headers(self) -> List[str]:
        """Ordered list of column header strings (replaces PRODUCTION_HEADERS)."""
        return list(self._headers)

    # ─── Column Widths ──────────────────────────────────────

    @property
    def column_widths(self) -> Dict[int, int]:
        """Dict of {column_index: width} (replaces COLUMN_WIDTHS)."""
        return dict(self._widths)

    # ─── Styles ─────────────────────────────────────────────

    @property
    def header_style(self) -> dict:
        """Header row style config {fill_color, font_color, font_bold, font_size, alignment, wrap_text}."""
        return dict(self._header_style)

    @property
    def group_style(self) -> dict:
        """Group row style from grouping.yaml {fill_color, fill_type, font_bold, font_size}."""
        fmt = self._group_row_cfg.get('formatting', {})
        return dict(fmt) if fmt else dict(self._group_style_cfg)

    @property
    def detail_style(self) -> dict:
        """Detail row style from grouping.yaml {font_bold, font_size}."""
        fmt = self._detail_row_cfg.get('formatting', {})
        return dict(fmt) if fmt else dict(self._detail_style_cfg)

    @property
    def border_style(self) -> str:
        """Border style string (e.g. 'thin')."""
        return self._border_style.get('style', 'thin')

    # ─── Currency ───────────────────────────────────────────

    @property
    def currency_format(self) -> str:
        """Currency number format string (replaces CURRENCY_FMT)."""
        return self._currency_format

    @property
    def currency_columns_all(self) -> List[int]:
        """Column indices that get currency format on all rows (replaces CURRENCY_COLS_ALL)."""
        return list(self._currency_all)

    @property
    def currency_columns_detail(self) -> List[int]:
        """Column indices that get currency format on detail rows only (replaces CURRENCY_COLS_DETAIL)."""
        return list(self._currency_detail)

    # ─── Column Value Specs ─────────────────────────────────

    def _get_col_value_spec(self, letter: str, row_type: str) -> Optional[str]:
        """Get the value spec string for a column and row type.
        row_type is 'group_value', 'detail_value', 'formula', or 'value'.
        """
        cfg = self._by_letter.get(letter, {})
        return cfg.get(row_type)

    def group_value_spec(self, letter: str) -> Optional[str]:
        """What should this column contain on a group row? Returns template string or None."""
        return self._get_col_value_spec(letter, 'group_value')

    def detail_value_spec(self, letter: str) -> Optional[str]:
        """What should this column contain on a detail row? Returns template string or None."""
        return self._get_col_value_spec(letter, 'detail_value')

    def formula_spec(self, letter: str) -> Optional[str]:
        """Get formula template for a column (e.g. '=O{row}*K{row}')."""
        return self._get_col_value_spec(letter, 'formula')

    def static_value(self, letter: str) -> Optional[str]:
        """Get static value for a column (e.g. 'USD')."""
        return self._get_col_value_spec(letter, 'value')

    def col_precision(self, letter: str) -> Optional[str]:
        """Get precision setting for a column (e.g. 'full')."""
        return self._by_letter.get(letter, {}).get('precision')

    def col_default(self, letter: str) -> Any:
        """Get default value for a column."""
        return self._by_letter.get(letter, {}).get('default')

    def col_populate_on(self, letter: str) -> Optional[str]:
        """Get populate_on constraint (e.g. 'first_group_per_invoice')."""
        return self._by_letter.get(letter, {}).get('populate_on')

    # ─── First-Group-Per-Invoice Columns ────────────────────

    def first_group_columns(self) -> Dict[str, dict]:
        """Columns that only populate on the first group row of each invoice."""
        return dict(self._first_group_cfg.get('additional_columns', {}))

    # ─── Detail Row Blank Columns ───────────────────────────

    def detail_blank_columns(self) -> List[str]:
        """Column letters that must be blank on detail rows."""
        return list(self._detail_row_cfg.get('columns_blank', []))

    # ─── Formulas ───────────────────────────────────────────

    def formula_template(self, name: str) -> Optional[str]:
        """Get a named formula template pattern from formula_templates section."""
        tmpl = self._formulas.get(name, {})
        return tmpl.get('pattern') if isinstance(tmpl, dict) else None

    # ─── Totals Section ─────────────────────────────────────

    @property
    def totals_label_column(self) -> int:
        """Column index where subtotal labels go."""
        col = self._totals_cfg.get('label_column', 'L')
        return self.col_index(col)

    @property
    def totals_default_formatting(self) -> dict:
        """Default formatting for totals rows."""
        return dict(self._totals_cfg.get('default_formatting', {}))

    @property
    def totals_rows(self) -> List[dict]:
        """Ordered list of totals row configs from grouping.yaml."""
        return list(self._totals_cfg.get('rows', []))

    @property
    def ungrouped_totals_rows(self) -> List[dict]:
        """Ordered list of ungrouped totals row configs."""
        return list(self._ungrouped_totals_cfg.get('rows', []))

    @property
    def ungrouped_totals_label_column(self) -> int:
        """Column index where ungrouped subtotal labels go."""
        col = self._ungrouped_totals_cfg.get('label_column', 'L')
        return self.col_index(col)

    # ─── Category Names ─────────────────────────────────────

    def category_name(self, tariff_code: str) -> str:
        """Look up category name for a tariff code from grouping.yaml mappings."""
        if not tariff_code:
            return self._category_default
        # Exact match
        if tariff_code in self._category_mappings:
            return self._category_mappings[tariff_code]
        # Try 6-digit prefix
        prefix6 = tariff_code[:6] + '00' if len(tariff_code) >= 6 else ''
        if prefix6 in self._category_mappings:
            return self._category_mappings[prefix6]
        # Try 4-digit prefix
        prefix4 = tariff_code[:4] + '0000' if len(tariff_code) >= 4 else ''
        if prefix4 in self._category_mappings:
            return self._category_mappings[prefix4]
        return self._category_default

    @property
    def category_default(self) -> str:
        return self._category_default

    def format_group_label(self, category: str, item_count: int) -> str:
        """Format a group label using the configured format template."""
        return self._category_format.replace(
            '${category_name}', category
        ).replace(
            '${item_count}', str(item_count)
        )

    # ─── Date Format ────────────────────────────────────────

    @property
    def date_format(self) -> str:
        """Target date format from columns.yaml col D."""
        return self._by_letter.get('D', {}).get('format', 'YYYY-MM-DD')

    # ─── Debug ──────────────────────────────────────────────

    def dump_summary(self):
        """Log a summary of loaded configuration."""
        logger.info(f"[SPEC] columns.yaml v{self._columns_cfg.get('version', '?')}: "
                    f"{self.col_count()} columns, "
                    f"{len(self._widths)} widths, "
                    f"{len(self._currency_all)} currency cols")
        logger.info(f"[SPEC] grouping.yaml v{self._grouping_cfg.get('version', '?')}: "
                    f"{len(self._category_mappings)} category mappings, "
                    f"{len(self.totals_rows)} totals rows")


# Module-level singleton — loaded once on first import
_spec_instance: Optional[SpecLoader] = None


def get_spec(config_dir: str = None) -> SpecLoader:
    """Get or create the singleton SpecLoader instance."""
    global _spec_instance
    if _spec_instance is None:
        _spec_instance = SpecLoader(config_dir)
    return _spec_instance


# Convenience alias
spec = None  # Will be populated on first access


def __getattr__(name):
    """Module-level lazy loading of spec singleton."""
    if name == 'spec':
        global spec
        spec = get_spec()
        return spec
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
