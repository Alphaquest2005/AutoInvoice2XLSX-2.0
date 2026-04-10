import React from 'react';
import { Eye, Loader } from 'lucide-react';
import { usePreviewStore } from '../../stores/previewStore';
import { XlsxPreview } from '../xlsx/XlsxPreview';
import { PdfPreview } from './PdfPreview';
import { TxtPreview } from './TxtPreview';

export function PreviewPane() {
  const previewType = usePreviewStore((s) => s.previewType);
  const loading = usePreviewStore((s) => s.loading);

  if (loading) {
    return (
      <div className="h-full flex flex-col items-center justify-center bg-surface-900 text-surface-400">
        <Loader size={24} className="mb-3 animate-spin opacity-50" />
        <p className="text-xs">Loading preview...</p>
      </div>
    );
  }

  switch (previewType) {
    case 'xlsx':
      return <XlsxPreview />;
    case 'pdf':
      return <PdfPreview />;
    case 'txt':
      return <TxtPreview />;
    default:
      return (
        <div className="h-full flex flex-col items-center justify-center bg-surface-900 text-surface-400 p-6">
          <Eye size={40} className="mb-4 opacity-40" />
          <p className="text-sm text-center">No file selected</p>
          <p className="text-xs text-surface-500 mt-1 text-center">
            Click a file to preview PDF, XLSX, or text files
          </p>
        </div>
      );
  }
}
