import React from 'react';
import { FileText, ExternalLink } from 'lucide-react';
import { usePreviewStore } from '../../stores/previewStore';

export function TxtPreview() {
  const textContent = usePreviewStore((s) => s.textContent);
  const previewPath = usePreviewStore((s) => s.previewPath);

  const displayName = previewPath?.replace(/\\/g, '/').split('/').pop() || 'Unknown';

  const handleOpenExternal = () => {
    if (previewPath) {
      window.api.openExternal(previewPath);
    }
  };

  return (
    <div className="h-full flex flex-col bg-surface-900">
      {/* Header */}
      <div className="h-9 px-3 flex items-center justify-between border-b border-surface-700 bg-surface-800/50">
        <span className="flex items-center gap-1.5 text-xs font-medium text-surface-300 truncate">
          <FileText size={14} className="text-surface-400 flex-shrink-0" />
          {displayName}
        </span>
        <button
          onClick={handleOpenExternal}
          className="flex items-center gap-1.5 px-2 py-1 text-xs text-surface-300 hover:text-surface-100 hover:bg-surface-700 rounded transition-colors"
          title="Open externally"
        >
          <ExternalLink size={14} />
          Open
        </button>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-auto p-4">
        <pre className="text-xs text-surface-200 font-mono whitespace-pre-wrap break-words leading-relaxed">
          {textContent}
        </pre>
      </div>
    </div>
  );
}
