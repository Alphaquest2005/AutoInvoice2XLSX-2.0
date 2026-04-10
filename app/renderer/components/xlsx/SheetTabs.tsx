import React from 'react';
import { MessageSquare } from 'lucide-react';
import { useXlsxStore } from '../../stores/xlsxStore';

export function SheetTabs() {
  const data = useXlsxStore((s) => s.data);
  const activeSheet = useXlsxStore((s) => s.activeSheet);
  const setActiveSheet = useXlsxStore((s) => s.setActiveSheet);
  const selection = useXlsxStore((s) => s.selection);
  const annotationOpen = useXlsxStore((s) => s.annotationOpen);
  const toggleAnnotation = useXlsxStore((s) => s.toggleAnnotation);

  if (!data) return null;

  return (
    <div className="h-7 flex items-center border-t border-surface-700 bg-surface-800/50 px-1 gap-0.5">
      {/* Sheet tabs */}
      {data.sheets.map((sheet, i) => (
        <button
          key={i}
          onClick={() => setActiveSheet(i)}
          className={`px-3 py-1 text-[11px] rounded-t transition-colors ${
            i === activeSheet
              ? 'bg-surface-700 text-surface-100 font-medium'
              : 'text-surface-400 hover:text-surface-200 hover:bg-surface-700/50'
          }`}
        >
          {sheet.name}
        </button>
      ))}

      {/* Spacer */}
      <div className="flex-1" />

      {/* Annotate button */}
      {selection && (
        <button
          onClick={toggleAnnotation}
          className={`px-2 py-1 text-[11px] rounded flex items-center gap-1 transition-colors ${
            annotationOpen
              ? 'bg-accent text-white'
              : 'text-surface-400 hover:text-surface-100 hover:bg-surface-700'
          }`}
          title="Annotate selection (send to chat)"
        >
          <MessageSquare size={11} />
          Annotate
        </button>
      )}
    </div>
  );
}
