import React, { useState } from 'react';
import { X, Send, MessageSquare } from 'lucide-react';
import { useXlsxStore } from '../../stores/xlsxStore';
import { useChatStore } from '../../stores/chatStore';

export function AnnotationPanel() {
  const [text, setText] = useState('');
  const selectedCells = useXlsxStore((s) => s.selectedCells);
  const selection = useXlsxStore((s) => s.selection);
  const closeAnnotation = useXlsxStore((s) => s.closeAnnotation);
  const filePath = useXlsxStore((s) => s.filePath);
  const sendMessage = useChatStore((s) => s.sendMessage);
  const activeConversationId = useChatStore((s) => s.activeConversationId);
  const createConversation = useChatStore((s) => s.createConversation);

  const handleSend = async () => {
    if (!text.trim() || selectedCells.length === 0) return;

    // Build context message
    const cellRefs = selectedCells.map((c) => c.address).join(', ');
    const cellValues = selectedCells
      .filter((c) => c.value !== null)
      .map((c) => `${c.address}: ${c.formula ? `=${c.formula}` : c.value}`)
      .slice(0, 20)
      .join('\n');

    const selRange = selection
      ? `${colLetter(Math.min(selection.startCol, selection.endCol))}${Math.min(selection.startRow, selection.endRow) + 1}:${colLetter(Math.max(selection.startCol, selection.endCol))}${Math.max(selection.startRow, selection.endRow) + 1}`
      : cellRefs;

    const contextMsg = [
      `[Spreadsheet annotation on ${filePath || 'current file'}]`,
      `Selected cells: ${selRange}`,
      `Cell data:`,
      cellValues,
      ``,
      `User feedback: ${text}`,
    ].join('\n');

    // Ensure there's an active conversation
    if (!activeConversationId) {
      await createConversation();
    }

    sendMessage(contextMsg);
    setText('');
    closeAnnotation();
  };

  return (
    <div className="border-t border-surface-700 bg-surface-800 p-3">
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-1.5 text-xs text-surface-300">
          <MessageSquare size={12} />
          <span>Annotate Selection ({selectedCells.length} cells)</span>
        </div>
        <button onClick={closeAnnotation} className="text-surface-400 hover:text-surface-100">
          <X size={14} />
        </button>
      </div>

      {/* Selected cells preview */}
      <div className="mb-2 max-h-16 overflow-y-auto text-[10px] font-mono text-surface-500 bg-surface-900 rounded p-1.5">
        {selectedCells.slice(0, 10).map((c) => (
          <div key={c.address}>
            {c.address}: {c.formula ? `=${c.formula}` : String(c.value ?? '')}
          </div>
        ))}
        {selectedCells.length > 10 && <div>...and {selectedCells.length - 10} more</div>}
      </div>

      {/* Input */}
      <div className="flex gap-2">
        <input
          type="text"
          value={text}
          onChange={(e) => setText(e.target.value)}
          onKeyDown={(e) => e.key === 'Enter' && handleSend()}
          placeholder="e.g., Fix this tariff code, These should be grouped..."
          className="flex-1 px-2 py-1.5 text-xs bg-surface-900 border border-surface-600 rounded text-surface-100 placeholder-surface-500 focus:outline-none focus:border-accent"
        />
        <button
          onClick={handleSend}
          disabled={!text.trim()}
          className="px-3 py-1.5 text-xs bg-accent hover:bg-accent-hover disabled:bg-surface-700 text-white rounded transition-colors flex items-center gap-1"
        >
          <Send size={11} />
          Send to Agent
        </button>
      </div>
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
