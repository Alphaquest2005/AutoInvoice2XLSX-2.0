import React, { useState, useEffect } from 'react';
import { MessageList } from './MessageList';
import { ChatInput } from './ChatInput';
import { ToolIndicator } from './ToolIndicator';
import { useChatStore } from '../../stores/chatStore';
import { usePipelineStore } from '../../stores/pipelineStore';
import { useConsoleStore } from '../../stores/consoleStore';
import { ConsolePanel } from '../console/ConsolePanel';
import { MessageSquare } from 'lucide-react';

function TabButton({ label, active, onClick, badge }: {
  label: string; active: boolean; onClick: () => void; badge?: boolean;
}) {
  return (
    <button
      onClick={onClick}
      className={`text-xs font-medium transition-colors relative pb-[9px] ${
        active ? 'text-surface-100' : 'text-surface-500 hover:text-surface-300'
      }`}
    >
      {label}
      {active && (
        <div className="absolute bottom-0 left-0 right-0 h-[2px] bg-accent" />
      )}
      {badge && !active && (
        <span className="absolute -top-0.5 -right-2 w-1.5 h-1.5 rounded-full bg-accent" />
      )}
    </button>
  );
}

export function ChatPane() {
  const activeConversationId = useChatStore((s) => s.activeConversationId);
  const isStreaming = useChatStore((s) => s.isStreaming);
  const createConversation = useChatStore((s) => s.createConversation);
  const pipelineState = usePipelineStore((s) => s.state);
  const hasUnread = useConsoleStore((s) => s.hasUnread);

  const [activeTab, setActiveTab] = useState<'chat' | 'console'>('console');

  // Auto-switch to console when pipeline starts
  useEffect(() => {
    if (pipelineState === 'running') {
      setActiveTab('console');
    }
  }, [pipelineState]);

  // Clear unread when switching to console
  useEffect(() => {
    if (activeTab === 'console') {
      useConsoleStore.getState().clearUnread();
    }
  }, [activeTab]);

  if (!activeConversationId) {
    return (
      <div className="h-full flex flex-col bg-surface-900">
        {/* Tab bar */}
        <div className="h-9 px-3 flex items-center gap-4 border-b border-surface-700 bg-surface-800/50 shrink-0">
          <TabButton label="Chat" active={activeTab === 'chat'} onClick={() => setActiveTab('chat')} />
          <TabButton
            label="Console"
            active={activeTab === 'console'}
            onClick={() => setActiveTab('console')}
            badge={hasUnread && activeTab !== 'console'}
          />
        </div>

        <div className="flex-1 overflow-hidden min-h-0">
          {activeTab === 'console' ? (
            <ConsolePanel />
          ) : (
            <div className="h-full flex flex-col items-center justify-center text-surface-400 p-6">
              <MessageSquare size={40} className="mb-4 opacity-40" />
              <p className="text-sm mb-4 text-center">No conversation selected</p>
              <button
                onClick={() => createConversation()}
                className="px-4 py-2 text-sm bg-accent hover:bg-accent-hover text-white rounded transition-colors"
              >
                New Conversation
              </button>
            </div>
          )}
        </div>
      </div>
    );
  }

  return (
    <div className="h-full flex flex-col bg-surface-900">
      {/* Tab bar */}
      <div className="h-9 px-3 flex items-center gap-4 border-b border-surface-700 bg-surface-800/50 shrink-0">
        <TabButton label="Chat" active={activeTab === 'chat'} onClick={() => setActiveTab('chat')} />
        <TabButton
          label="Console"
          active={activeTab === 'console'}
          onClick={() => setActiveTab('console')}
          badge={hasUnread && activeTab !== 'console'}
        />
      </div>

      {activeTab === 'console' ? (
        <div className="flex-1 overflow-hidden min-h-0">
          <ConsolePanel />
        </div>
      ) : (
        <>
          {/* Messages */}
          <div className="flex-1 overflow-hidden">
            <MessageList />
          </div>

          {/* Tool indicator */}
          {isStreaming && <ToolIndicator />}

          {/* Input */}
          <ChatInput />
        </>
      )}
    </div>
  );
}
