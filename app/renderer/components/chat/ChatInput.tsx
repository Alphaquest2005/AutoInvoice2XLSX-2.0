import React, { useState, useRef, useCallback, useEffect } from 'react';
import { Send, Paperclip, Loader2, Clock } from 'lucide-react';
import { useChatStore } from '../../stores/chatStore';

interface QueuedMessage {
  content: string;
  attachments?: string[];
}

export function ChatInput() {
  const [text, setText] = useState('');
  const [attachments, setAttachments] = useState<string[]>([]);
  const [queued, setQueued] = useState<QueuedMessage | null>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const sendMessage = useChatStore((s) => s.sendMessage);
  const isStreaming = useChatStore((s) => s.isStreaming);
  const activeConversationId = useChatStore((s) => s.activeConversationId);

  // Send queued message when streaming finishes
  useEffect(() => {
    if (!isStreaming && queued) {
      sendMessage(queued.content, queued.attachments);
      setQueued(null);
    }
  }, [isStreaming, queued, sendMessage]);

  const handleSend = useCallback(() => {
    const trimmed = text.trim();
    if (!trimmed || !activeConversationId) return;

    if (isStreaming) {
      // Queue message to send after current stream completes
      setQueued({ content: trimmed, attachments: attachments.length > 0 ? attachments : undefined });
    } else {
      sendMessage(trimmed, attachments.length > 0 ? attachments : undefined);
    }
    setText('');
    setAttachments([]);
  }, [text, isStreaming, activeConversationId, attachments, sendMessage]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  const handleAttach = async () => {
    if (!window.api) return;
    const file = await window.api.openFileDialog();
    if (file) {
      setAttachments((prev) => [...prev, file]);
    }
  };

  const autoResize = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    const ta = e.target;
    ta.style.height = 'auto';
    ta.style.height = Math.min(ta.scrollHeight, 150) + 'px';
    setText(ta.value);
  };

  return (
    <div className="border-t border-surface-700 bg-surface-800/50 p-2">
      {/* Queued message indicator */}
      {queued && (
        <div className="flex items-center gap-1.5 px-2 py-1 mb-1 text-xs text-amber-400 bg-amber-400/10 rounded">
          <Clock size={12} />
          Message queued — will send when response completes
        </div>
      )}

      {/* Attachments */}
      {attachments.length > 0 && (
        <div className="flex flex-wrap gap-1 mb-2 px-1">
          {attachments.map((a, i) => (
            <span
              key={i}
              className="inline-flex items-center gap-1 px-2 py-0.5 text-xs bg-surface-700 rounded text-surface-300"
            >
              {a.split(/[\\/]/).pop()}
              <button
                onClick={() => setAttachments((prev) => prev.filter((_, j) => j !== i))}
                className="text-surface-500 hover:text-surface-200"
              >
                x
              </button>
            </span>
          ))}
        </div>
      )}

      <div className="flex items-end gap-1.5">
        <button
          onClick={handleAttach}
          className="p-1.5 text-surface-400 hover:text-surface-200 transition-colors"
          title="Attach file"
        >
          <Paperclip size={16} />
        </button>

        <textarea
          ref={textareaRef}
          value={text}
          onChange={autoResize}
          onKeyDown={handleKeyDown}
          rows={1}
          placeholder={isStreaming ? 'Type a message... (will send after response)' : 'Type a message... (Enter to send, Shift+Enter for newline)'}
          disabled={!activeConversationId}
          className="flex-1 resize-none bg-surface-900 border border-surface-600 rounded-lg px-3 py-2 text-sm text-surface-100 placeholder-surface-500 focus:outline-none focus:border-accent disabled:opacity-50"
        />

        <button
          onClick={handleSend}
          disabled={!text.trim() || !activeConversationId}
          className={`p-1.5 transition-colors ${
            isStreaming && text.trim()
              ? 'text-amber-400 hover:text-amber-300'
              : 'text-accent hover:text-accent-hover disabled:text-surface-600'
          }`}
          title={isStreaming ? 'Queue message (Enter)' : 'Send (Enter)'}
        >
          {isStreaming ? (
            text.trim() ? <Clock size={18} /> : <Loader2 size={18} className="animate-spin" />
          ) : (
            <Send size={18} />
          )}
        </button>
      </div>
    </div>
  );
}
