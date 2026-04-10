import React, { useRef, useCallback, useMemo } from 'react';
import { CellRenderer } from './CellRenderer';
import { ErrorHighlighter } from './ErrorHighlighter';
import { useXlsxStore, type CellError } from '../../stores/xlsxStore';
import type { CellSelection, SheetData } from '../../../shared/types';

const ROW_HEIGHT = 24;
const HEADER_ROW_HEIGHT = 28;
const ROW_NUM_WIDTH = 40;

export function SpreadsheetGrid() {
  const data = useXlsxStore((s) => s.data);
  const activeSheet = useXlsxStore((s) => s.activeSheet);
  const selection = useXlsxStore((s) => s.selection);
  const setSelection = useXlsxStore((s) => s.setSelection);
  const errors = useXlsxStore((s) => s.errors);

  const containerRef = useRef<HTMLDivElement>(null);
  const isSelecting = useRef(false);
  const selStart = useRef<{ row: number; col: number } | null>(null);

  if (!data) return null;

  const sheet = data.sheets[activeSheet];
  if (!sheet) return null;

  const errorMap = useMemo(() => {
    const map = new Map<string, CellError>();
    errors.forEach((e) => map.set(`${e.row}:${e.col}`, e));
    return map;
  }, [errors]);

  const colWidths = sheet.colWidths.length > 0
    ? sheet.colWidths
    : Array(sheet.colCount).fill(80);

  const handleMouseDown = (row: number, col: number) => {
    isSelecting.current = true;
    selStart.current = { row, col };
    setSelection({ startRow: row, startCol: col, endRow: row, endCol: col });
  };

  const handleMouseMove = (row: number, col: number) => {
    if (!isSelecting.current || !selStart.current) return;
    setSelection({
      startRow: selStart.current.row,
      startCol: selStart.current.col,
      endRow: row,
      endCol: col,
    });
  };

  const handleMouseUp = () => {
    isSelecting.current = false;
  };

  const isInSelection = (row: number, col: number): boolean => {
    if (!selection) return false;
    const minR = Math.min(selection.startRow, selection.endRow);
    const maxR = Math.max(selection.startRow, selection.endRow);
    const minC = Math.min(selection.startCol, selection.endCol);
    const maxC = Math.max(selection.startCol, selection.endCol);
    return row >= minR && row <= maxR && col >= minC && col <= maxC;
  };

  // Column headers (A, B, C...)
  const colHeaders = Array.from({ length: sheet.colCount }, (_, i) => {
    let result = '';
    let n = i;
    do {
      result = String.fromCharCode(65 + (n % 26)) + result;
      n = Math.floor(n / 26) - 1;
    } while (n >= 0);
    return result;
  });

  return (
    <div
      ref={containerRef}
      className="h-full overflow-auto"
      onMouseUp={handleMouseUp}
      onMouseLeave={handleMouseUp}
    >
      <table className="border-collapse select-none" style={{ minWidth: '100%' }}>
        {/* Column headers */}
        <thead className="sticky top-0 z-10">
          <tr>
            {/* Row number header */}
            <th
              className="bg-surface-800 border-b border-r border-surface-600 text-[10px] text-surface-400 font-medium"
              style={{ width: ROW_NUM_WIDTH, minWidth: ROW_NUM_WIDTH, height: HEADER_ROW_HEIGHT }}
            />
            {colHeaders.map((header, ci) => (
              <th
                key={ci}
                className="bg-surface-800 border-b border-r border-surface-600 text-[10px] text-surface-400 font-medium px-1 whitespace-nowrap"
                style={{
                  width: colWidths[ci],
                  minWidth: colWidths[ci],
                  height: HEADER_ROW_HEIGHT,
                }}
              >
                {header}
              </th>
            ))}
          </tr>
        </thead>

        <tbody>
          {sheet.rows.map((row, ri) => (
            <tr key={ri}>
              {/* Row number */}
              <td
                className="sticky left-0 z-[5] bg-surface-800 border-b border-r border-surface-600 text-[10px] text-surface-400 text-center font-medium"
                style={{ width: ROW_NUM_WIDTH, minWidth: ROW_NUM_WIDTH, height: ROW_HEIGHT }}
              >
                {ri + 1}
              </td>

              {row.map((cell, ci) => {
                const error = errorMap.get(`${ri}:${ci}`);
                const selected = isInSelection(ri, ci);

                return (
                  <td
                    key={ci}
                    onMouseDown={() => handleMouseDown(ri, ci)}
                    onMouseMove={() => handleMouseMove(ri, ci)}
                    className={`border-b border-r border-surface-700/50 px-1 text-[11px] cursor-cell whitespace-nowrap overflow-hidden ${
                      selected ? 'cell-selected' : ''
                    } ${error ? (error.severity === 'error' ? 'cell-error' : 'cell-warning') : ''} ${
                      cell.style?.bgColor === '#D9E1F2' || cell.style?.bgColor === '#d9e1f2' ? 'cell-group-row' : ''
                    }`}
                    style={{
                      width: colWidths[ci],
                      minWidth: colWidths[ci],
                      height: ROW_HEIGHT,
                      maxWidth: colWidths[ci],
                      fontWeight: cell.style?.bold ? 'bold' : undefined,
                      fontStyle: cell.style?.italic ? 'italic' : undefined,
                      color: cell.style?.fontColor || undefined,
                      backgroundColor: cell.style?.bgColor && cell.style.bgColor !== '#D9E1F2'
                        ? cell.style.bgColor
                        : undefined,
                      textAlign: cell.style?.alignment || (cell.type === 'number' ? 'right' : 'left'),
                    }}
                  >
                    <CellRenderer cell={cell} />
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
