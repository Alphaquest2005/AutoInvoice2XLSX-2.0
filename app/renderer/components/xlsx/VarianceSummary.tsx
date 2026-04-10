import React, { useMemo } from 'react';
import { CheckCircle, AlertTriangle } from 'lucide-react';
import { useXlsxStore } from '../../stores/xlsxStore';

interface SummaryRow {
  label: string;
  value: number | null;
  row: number;
  isFormula?: boolean;
}

export function VarianceSummary() {
  const data = useXlsxStore((s) => s.data);
  const activeSheet = useXlsxStore((s) => s.activeSheet);

  const sheetName = data?.sheets[activeSheet]?.name || '';

  const summaryRows = useMemo(() => {
    if (!data) return [];

    const sheet = data.sheets[activeSheet];
    if (!sheet) return [];

    const rows: SummaryRow[] = [];

    // Search from bottom up for summary rows (they're typically at the end)
    // Search last 30 rows to be safe
    for (let r = sheet.rows.length - 1; r >= Math.max(0, sheet.rows.length - 30); r--) {
      const row = sheet.rows[r];
      if (!row) continue;

      // Column L (11) has the label
      const labelCell = row[11];
      if (!labelCell || typeof labelCell.value !== 'string') continue;

      const label = labelCell.value.toUpperCase().trim();

      // Column P (15) has the value - could be number or formula
      const valueCell = row[15];
      let value: number | null = null;
      let isFormula = false;

      if (valueCell) {
        // Check if cell has a formula (SheetJS stores formula in cell.formula without the leading =)
        if (valueCell.formula) {
          isFormula = true;
          // The cell.value might be a cached 0, which is misleading
          value = null; // Can't trust the cached value
        } else if (typeof valueCell.value === 'number') {
          value = valueCell.value;
        } else if (typeof valueCell.value === 'string') {
          const strVal = valueCell.value.trim();
          // Check if it's a formula string (starts with =)
          if (strVal.startsWith('=')) {
            isFormula = true;
            value = null;
          } else {
            // Try to parse numeric string
            const parsed = parseFloat(strVal.replace(/[$,]/g, ''));
            if (!isNaN(parsed)) {
              value = parsed;
            }
          }
        }
      }

      if (label.includes('VARIANCE CHECK')) {
        rows.push({ label: 'VARIANCE CHECK', value, row: r, isFormula });
      } else if (label.includes('GROUP VERIFICATION')) {
        rows.push({ label: 'GROUP VERIFICATION', value, row: r, isFormula });
      } else if (label.includes('NET TOTAL')) {
        rows.push({ label: 'NET TOTAL', value, row: r, isFormula });
      }
    }

    // Reverse to show in natural order (GROUP VERIFICATION, NET TOTAL, VARIANCE CHECK)
    return rows.reverse();
  }, [data, activeSheet]);

  // Always show the bar, even if no summary rows found
  if (summaryRows.length === 0) {
    return (
      <div className="px-3 py-1.5 border-t border-surface-700 bg-surface-800/50 flex items-center gap-4 text-xs">
        <span className="text-surface-400">
          {sheetName ? `${sheetName}: No variance data found` : 'No spreadsheet loaded'}
        </span>
      </div>
    );
  }

  const hasFormulas = summaryRows.some((r) => r.isFormula);
  const hasError = summaryRows.some(
    (r) => (r.label === 'VARIANCE CHECK' || r.label === 'GROUP VERIFICATION') &&
           r.value !== null && Math.abs(r.value) > 0.001
  );

  return (
    <div className={`px-3 py-1.5 border-t flex items-center gap-4 text-xs ${
      hasFormulas
        ? 'bg-yellow-900/30 border-yellow-700/50'
        : hasError
          ? 'bg-red-900/30 border-red-700/50'
          : 'bg-green-900/20 border-surface-700'
    }`}>
      <div className="flex items-center gap-1.5">
        {hasFormulas ? (
          <AlertTriangle size={14} className="text-yellow-400" />
        ) : hasError ? (
          <AlertTriangle size={14} className="text-red-400" />
        ) : (
          <CheckCircle size={14} className="text-green-400" />
        )}
        <span className={hasFormulas ? 'text-yellow-300' : hasError ? 'text-red-300' : 'text-green-300'}>
          {sheetName}:
        </span>
      </div>

      {hasFormulas && (
        <span className="text-yellow-400 italic">
          Contains formulas - re-run pipeline for accurate values
        </span>
      )}

      {summaryRows.map((row, i) => {
        const isCheckRow = row.label === 'VARIANCE CHECK' || row.label === 'GROUP VERIFICATION';
        const isError = isCheckRow && row.value !== null && Math.abs(row.value) > 0.001;
        const isOk = isCheckRow && row.value !== null && Math.abs(row.value) <= 0.001;

        return (
          <div key={i} className="flex items-center gap-1">
            <span className="text-surface-400">{row.label}:</span>
            <span className={`font-mono ${
              row.isFormula ? 'text-yellow-400 italic' :
              isError ? 'text-red-400 font-bold' :
              isOk ? 'text-green-400' :
              'text-surface-200'
            }`}>
              {row.isFormula
                ? '(formula)'
                : row.value !== null
                  ? `$${row.value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
                  : 'N/A'
              }
            </span>
          </div>
        );
      })}
    </div>
  );
}
