import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { FileText, ExternalLink, ArrowUp, ArrowDown, Shuffle } from 'lucide-react';
import { usePreviewStore } from '../../stores/previewStore';
import { useChatStore } from '../../stores/chatStore';

export function PdfPreview() {
  const pdfBase64 = usePreviewStore((s) => s.pdfBase64);
  const previewPath = usePreviewStore((s) => s.previewPath);

  const [pageCount, setPageCount] = useState<number | null>(null);
  const [pageOrder, setPageOrder] = useState<number[]>([]);
  const [reordering, setReordering] = useState(false);
  const [showReorder, setShowReorder] = useState(false);

  const displayName = previewPath?.replace(/\\/g, '/').split('/').pop() || 'Unknown';

  const dataUrl = useMemo(() => {
    if (!pdfBase64) return null;
    return `data:application/pdf;base64,${pdfBase64}`;
  }, [pdfBase64]);

  // Fetch page count when path changes
  useEffect(() => {
    setShowReorder(false);
    setPageCount(null);
    setPageOrder([]);
    if (!previewPath) return;
    window.api.getPdfPageCount(previewPath).then((res) => {
      if (res.success && res.page_count) {
        setPageCount(res.page_count);
        setPageOrder(Array.from({ length: res.page_count }, (_, i) => i));
      }
    });
  }, [previewPath]);

  const handleOpenExternal = () => {
    if (previewPath) {
      window.api.openExternal(previewPath);
    }
  };

  const movePage = useCallback((fromIdx: number, direction: -1 | 1) => {
    const toIdx = fromIdx + direction;
    if (toIdx < 0 || toIdx >= pageOrder.length) return;
    setPageOrder((prev) => {
      const next = [...prev];
      [next[fromIdx], next[toIdx]] = [next[toIdx], next[fromIdx]];
      return next;
    });
  }, [pageOrder.length]);

  const applyReorder = useCallback(async () => {
    if (!previewPath || !pageOrder.length) return;

    // Check if order actually changed
    const isOriginal = pageOrder.every((v, i) => v === i);
    if (isOriginal) {
      setShowReorder(false);
      return;
    }

    setReordering(true);
    try {
      const result = await window.api.reorderPdf(previewPath, pageOrder);
      if (result.success) {
        // Reload the PDF preview
        await usePreviewStore.getState().openFile(previewPath);
        // Reset to identity order since the file is now reordered
        setPageOrder(Array.from({ length: pageOrder.length }, (_, i) => i));
        setShowReorder(false);
        useChatStore.getState().addSystemMessage(
          `Reordered pages in "${displayName}"`
        );
      } else {
        console.error('[pdf] Reorder failed:', result.error);
        useChatStore.getState().addSystemMessage(
          `Failed to reorder pages: ${result.error}`
        );
      }
    } catch (err) {
      console.error('[pdf] Reorder error:', err);
    } finally {
      setReordering(false);
    }
  }, [previewPath, pageOrder, displayName]);

  const resetOrder = useCallback(() => {
    if (pageCount) {
      setPageOrder(Array.from({ length: pageCount }, (_, i) => i));
    }
  }, [pageCount]);

  const isOrderChanged = pageOrder.length > 0 && !pageOrder.every((v, i) => v === i);

  return (
    <div className="h-full flex flex-col bg-surface-900">
      {/* Header */}
      <div className="h-9 px-3 flex items-center justify-between border-b border-surface-700 bg-surface-800/50">
        <span className="flex items-center gap-1.5 text-xs font-medium text-surface-300 truncate">
          <FileText size={14} className="text-red-400 flex-shrink-0" />
          {displayName}
          {pageCount !== null && (
            <span className="text-surface-500 ml-1">({pageCount} pg)</span>
          )}
        </span>
        <div className="flex items-center gap-1">
          {pageCount !== null && pageCount > 1 && (
            <button
              onClick={() => setShowReorder(!showReorder)}
              className={`flex items-center gap-1 px-2 py-1 text-xs rounded transition-colors ${
                showReorder
                  ? 'text-blue-300 bg-blue-500/20'
                  : 'text-surface-300 hover:text-surface-100 hover:bg-surface-700'
              }`}
              title="Reorder pages"
            >
              <Shuffle size={14} />
              Reorder
            </button>
          )}
          <button
            onClick={handleOpenExternal}
            className="flex items-center gap-1 px-2 py-1 text-xs text-surface-300 hover:text-surface-100 hover:bg-surface-700 rounded transition-colors"
            title="Open externally"
          >
            <ExternalLink size={14} />
            Open
          </button>
        </div>
      </div>

      {/* Page Reorder Panel */}
      {showReorder && pageOrder.length > 1 && (
        <div className="border-b border-surface-700 bg-surface-800/80 px-3 py-2">
          <div className="flex items-center gap-2 mb-2">
            <span className="text-xs font-medium text-surface-300">Page Order</span>
            <div className="flex-1" />
            {isOrderChanged && (
              <>
                <button
                  onClick={resetOrder}
                  className="px-2 py-0.5 text-xs text-surface-400 hover:text-surface-200 hover:bg-surface-700 rounded transition-colors"
                >
                  Reset
                </button>
                <button
                  onClick={applyReorder}
                  disabled={reordering}
                  className="px-2 py-0.5 text-xs text-white bg-blue-600 hover:bg-blue-500 rounded transition-colors disabled:opacity-50"
                >
                  {reordering ? 'Saving...' : 'Apply'}
                </button>
              </>
            )}
          </div>
          <div className="flex flex-col gap-1">
            {pageOrder.map((origPage, idx) => (
              <div
                key={idx}
                className={`flex items-center gap-2 px-2 py-1 rounded text-xs ${
                  origPage !== idx
                    ? 'bg-blue-500/10 text-blue-300'
                    : 'text-surface-300'
                }`}
              >
                <span className="w-16 text-surface-500">
                  Position {idx + 1}:
                </span>
                <span className="flex-1 font-medium">
                  Page {origPage + 1}
                </span>
                <button
                  onClick={() => movePage(idx, -1)}
                  disabled={idx === 0}
                  className="p-0.5 hover:bg-surface-600 rounded disabled:opacity-20 transition-colors"
                  title="Move up"
                >
                  <ArrowUp size={12} />
                </button>
                <button
                  onClick={() => movePage(idx, 1)}
                  disabled={idx === pageOrder.length - 1}
                  className="p-0.5 hover:bg-surface-600 rounded disabled:opacity-20 transition-colors"
                  title="Move down"
                >
                  <ArrowDown size={12} />
                </button>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* PDF Embed */}
      <div className="flex-1 overflow-hidden">
        {dataUrl ? (
          <iframe
            src={dataUrl}
            className="w-full h-full border-0"
            title={displayName}
          />
        ) : (
          <div className="h-full flex items-center justify-center text-surface-400 text-sm">
            Unable to load PDF
          </div>
        )}
      </div>
    </div>
  );
}
