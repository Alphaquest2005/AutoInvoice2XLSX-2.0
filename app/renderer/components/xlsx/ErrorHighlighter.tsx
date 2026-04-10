import React from 'react';
import { AlertTriangle, XCircle } from 'lucide-react';
import { useXlsxStore, type CellError } from '../../stores/xlsxStore';

export function ErrorHighlighter() {
  const errors = useXlsxStore((s) => s.errors);
  const setSelection = useXlsxStore((s) => s.setSelection);
  const toggleAnnotation = useXlsxStore((s) => s.toggleAnnotation);

  if (errors.length === 0) return null;

  const grouped = errors.reduce<Record<string, CellError[]>>((acc, e) => {
    const key = e.type;
    if (!acc[key]) acc[key] = [];
    acc[key].push(e);
    return acc;
  }, {});

  return (
    <div className="p-2 border-t border-surface-700 bg-surface-800/50">
      <div className="text-xs font-medium text-red-400 mb-1.5 flex items-center gap-1">
        <AlertTriangle size={12} />
        {errors.length} Error{errors.length !== 1 ? 's' : ''} Found
      </div>

      <div className="space-y-1 max-h-32 overflow-y-auto">
        {Object.entries(grouped).map(([type, errs]) => (
          <div key={type}>
            <div className="text-[10px] text-surface-400 uppercase font-medium mb-0.5">
              {type.replace('_', ' ')}
            </div>
            {errs.slice(0, 5).map((e) => (
              <button
                key={`${e.row}:${e.col}`}
                onClick={() => {
                  setSelection({ startRow: e.row, startCol: e.col, endRow: e.row, endCol: e.col });
                  toggleAnnotation();
                }}
                className="flex items-center gap-1.5 text-[11px] text-surface-300 hover:text-surface-100 hover:bg-surface-700/50 rounded px-1 py-0.5 w-full text-left"
              >
                <XCircle size={10} className="text-red-400 flex-shrink-0" />
                <span className="font-mono">{e.address}</span>
                <span className="text-surface-500 truncate">
                  {e.value !== null ? String(e.value) : ''}
                </span>
              </button>
            ))}
            {errs.length > 5 && (
              <span className="text-[10px] text-surface-500 px-1">...and {errs.length - 5} more</span>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
