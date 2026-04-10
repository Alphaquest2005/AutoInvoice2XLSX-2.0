import React, { useRef, useEffect } from 'react';
import { MessageBubble } from './MessageBubble';
import { useChatStore } from '../../stores/chatStore';

export function MessageList() {
  const messages = useChatStore((s) => s.messages);
  const streamingContent = useChatStore((s) => s.streamingContent);
  const isStreaming = useChatStore((s) => s.isStreaming);
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, streamingContent]);

  return (
    <div className="h-full overflow-y-auto px-3 py-3 space-y-3">
      {messages.length === 0 && !isStreaming && (
        <div className="flex items-center justify-center h-full text-surface-500 text-sm">
          <p>Send a message or drop a PDF to get started</p>
        </div>
      )}

      {messages.map((msg) => (
        <MessageBubble key={msg.id} message={msg} />
      ))}

      {/* Streaming message */}
      {isStreaming && streamingContent && (
        <MessageBubble
          message={{
            id: 'streaming',
            conversationId: '',
            role: 'assistant',
            content: streamingContent,
            createdAt: new Date().toISOString(),
          }}
          isStreaming
        />
      )}

      <div ref={bottomRef} />
    </div>
  );
}
