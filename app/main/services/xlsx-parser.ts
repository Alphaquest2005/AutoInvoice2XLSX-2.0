/**
 * XLSX parser service: reads an .xlsx file and returns structured
 * sheet/cell data with styles, merges, and column widths.
 */

import * as XLSX from 'xlsx';
import fs from 'fs';
import type { XlsxData, SheetData, CellData, CellStyle, MergeRange } from '../../shared/types';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Map xlsx.js cell type codes to our CellData type strings. */
const CELL_TYPE_MAP: Record<string, CellData['type']> = {
  s: 'string',
  n: 'number',
  b: 'boolean',
  d: 'date',
  e: 'empty', // error cells surfaced as empty
};

function extractCellStyle(cell: XLSX.CellObject): CellStyle | undefined {
  const style: CellStyle = {};

  if (cell.s) {
    const s = cell.s as Record<string, any>;
    if (s.font?.bold) style.bold = true;
    if (s.font?.italic) style.italic = true;
    if (s.font?.sz) style.fontSize = s.font.sz;
    if (s.font?.color?.rgb) style.fontColor = `#${s.font.color.rgb}`;
    if (s.fill?.fgColor?.rgb) style.bgColor = `#${s.fill.fgColor.rgb}`;
    if (s.alignment?.horizontal) style.alignment = s.alignment.horizontal;
    if (s.numFmt) style.numFmt = s.numFmt;
  }

  // Explicit number format on the cell itself overrides style-level
  if (cell.z) style.numFmt = String(cell.z);

  return Object.keys(style).length > 0 ? style : undefined;
}

function buildCellData(cell: XLSX.CellObject | undefined, address: string, row: number, col: number): CellData {
  if (!cell) {
    return { value: null, type: 'empty', address, row, col };
  }

  const type = CELL_TYPE_MAP[cell.t] ?? 'empty';
  const value = cell.v !== undefined ? cell.v : null;
  const formula = cell.f || undefined;
  const style = extractCellStyle(cell);

  return {
    value: value as CellData['value'],
    type,
    address,
    row,
    col,
    ...(formula ? { formula } : {}),
    ...(style ? { style } : {}),
  };
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Parse an XLSX file from disk and return structured `XlsxData`.
 *
 * Throws on missing file or corrupt workbook.
 */
export function parseXlsxFile(filePath: string): XlsxData {
  if (!fs.existsSync(filePath)) {
    throw new Error(`File not found: ${filePath}`);
  }

  const buffer = fs.readFileSync(filePath);

  let workbook: XLSX.WorkBook;
  try {
    workbook = XLSX.read(buffer, {
      cellFormula: true,
      cellStyles: true,
      cellNF: true,
    });
  } catch (err) {
    throw new Error(`Failed to parse XLSX: ${(err as Error).message}`);
  }

  const sheets: SheetData[] = [];

  for (const sheetName of workbook.SheetNames) {
    const ws = workbook.Sheets[sheetName];
    if (!ws) continue;

    const ref = ws['!ref'] || 'A1';
    const range = XLSX.utils.decode_range(ref);
    const rowCount = range.e.r - range.s.r + 1;
    const colCount = range.e.c - range.s.c + 1;

    // ── Cell data grid ──
    const rows: CellData[][] = [];
    for (let r = range.s.r; r <= range.e.r; r++) {
      const row: CellData[] = [];
      for (let c = range.s.c; c <= range.e.c; c++) {
        const addr = XLSX.utils.encode_cell({ r, c });
        row.push(buildCellData(ws[addr] as XLSX.CellObject | undefined, addr, r, c));
      }
      rows.push(row);
    }

    // ── Column widths ──
    const wsCols = ws['!cols'] || [];
    const colWidths: number[] = [];
    for (let c = 0; c < colCount; c++) {
      const col = wsCols[c];
      colWidths.push(col?.wpx ?? (col?.wch ? col.wch * 8 : 80));
    }

    // ── Merged cells ──
    const merges: MergeRange[] = (ws['!merges'] || []).map((m) => ({
      startRow: m.s.r,
      startCol: m.s.c,
      endRow: m.e.r,
      endCol: m.e.c,
    }));

    sheets.push({
      name: sheetName,
      rows,
      colCount,
      rowCount,
      colWidths,
      merges,
    });
  }

  if (sheets.length === 0) {
    throw new Error('No valid sheets found in workbook');
  }

  return {
    fileName: filePath.split(/[\\/]/).pop() || filePath,
    sheets,
    activeSheet: 0,
  };
}
