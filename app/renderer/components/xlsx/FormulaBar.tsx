import React from 'react';
import { useXlsxStore } from '../../stores/xlsxStore';

export function FormulaBar() {
  const selection = useXlsxStore((s) => s.selection);
  const selectedCells = useXlsxStore((s) => s.selectedCells);
  const data = useXlsxStore((s) => s.data);
  const activeSheet = useXlsxStore((s) => s.activeSheet);

  if (!data || !selection) return null;

  // Show first selected cell info
  const cell = selectedCells[0];
  if (!cell) return null;

  const address = cell.address || `${colLetter(selection.startCol)}${selection.startRow + 1}`;
  const formula = cell.formula ? `=${cell.formula}` : '';
  const value = cell.value !== null && cell.value !== undefined ? String(cell.value) : '';

  return (
    <div className="h-7 px-2 flex items-center gap-2 border-b border-surface-700 bg-surface-800/30 text-xs">
      {/* Cell address */}
      <div className="w-16 text-center font-mono text-surface-300 bg-surface-800 border border-surface-600 rounded px-1 py-0.5">
        {address}
      </div>

      {/* Formula / value */}
      <div className="flex-1 font-mono text-surface-200 truncate">
        {formula || value}
      </div>

      {/* Selection info */}
      {selectedCells.length > 1 && (
        <div className="text-surface-500 flex-shrink-0">
          {selectedCells.length} cells selected
        </div>
      )}
    </div>
  );
}

function colLetter(col: number): string {
  let result = '';
  let n = col;
  do {
    result = String.fromCharCode(65 + (n % 26)) + result;
    n = Math.floor(n / 26) - 1;
  } while (n >= 0);
  return result;
}
