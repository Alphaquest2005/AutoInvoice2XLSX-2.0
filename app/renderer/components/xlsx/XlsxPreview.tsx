import React from 'react';
import { Table2, AlertTriangle, FileSpreadsheet } from 'lucide-react';
import { SpreadsheetGrid } from './SpreadsheetGrid';
import { FormulaBar } from './FormulaBar';
import { SheetTabs } from './SheetTabs';
import { AnnotationPanel } from './AnnotationPanel';
import { VarianceSummary } from './VarianceSummary';
import { useXlsxStore } from '../../stores/xlsxStore';

export function XlsxPreview() {
  const data = useXlsxStore((s) => s.data);
  const filePath = useXlsxStore((s) => s.filePath);
  const errors = useXlsxStore((s) => s.errors);
  const annotationOpen = useXlsxStore((s) => s.annotationOpen);

  const handleOpenInExcel = async () => {
    if (!filePath) return;
    const result = await window.api.openInExcel(filePath);
    if (!result.success) {
      console.error('Failed to open in Excel:', result.error);
    }
  };

  if (!data) {
    return (
      <div className="h-full flex flex-col items-center justify-center bg-surface-900 text-surface-400 p-6">
        <Table2 size={40} className="mb-4 opacity-40" />
        <p className="text-sm text-center">No spreadsheet loaded</p>
        <p className="text-xs text-surface-500 mt-1 text-center">
          Open an XLSX file from the file browser or drop one here
        </p>
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col bg-surface-900">
      {/* Header */}
      <div className="h-9 px-3 flex items-center justify-between border-b border-surface-700 bg-surface-800/50">
        <span className="text-xs font-medium text-surface-300 truncate">{data.fileName}</span>
        <div className="flex items-center gap-3">
          {errors.length > 0 && (
            <span className="flex items-center gap-1 text-xs text-red-400">
              <AlertTriangle size={12} />
              {errors.length} error{errors.length !== 1 ? 's' : ''}
            </span>
          )}
          <button
            onClick={handleOpenInExcel}
            className="flex items-center gap-1.5 px-2 py-1 text-xs text-surface-300 hover:text-surface-100 hover:bg-surface-700 rounded transition-colors"
            title="Open in Excel"
          >
            <FileSpreadsheet size={14} />
            Open in Excel
          </button>
        </div>
      </div>

      {/* Formula bar */}
      <FormulaBar />

      {/* Spreadsheet grid */}
      <div className="flex-1 overflow-hidden">
        <SpreadsheetGrid />
      </div>

      {/* Annotation panel */}
      {annotationOpen && <AnnotationPanel />}

      {/* Variance summary (always visible at bottom) */}
      <VarianceSummary />

      {/* Sheet tabs */}
      <SheetTabs />
    </div>
  );
}
