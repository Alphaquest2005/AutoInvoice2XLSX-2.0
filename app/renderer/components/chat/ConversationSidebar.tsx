import React, { useState } from 'react';
import { X, MessageSquare, Trash2, Search } from 'lucide-react';
import { useChatStore } from '../../stores/chatStore';

export function ConversationSidebar() {
  const conversations = useChatStore((s) => s.conversations);
  const activeConversationId = useChatStore((s) => s.activeConversationId);
  const selectConversation = useChatStore((s) => s.selectConversation);
  const deleteConversation = useChatStore((s) => s.deleteConversation);
  const toggleSidebar = useChatStore((s) => s.toggleSidebar);
  const [search, setSearch] = useState('');

  const filtered = conversations.filter((c) =>
    c.title.toLowerCase().includes(search.toLowerCase())
  );

  return (
    <div className="w-64 max-w-[80vw] h-full bg-surface-800 border-r border-surface-700 flex flex-col flex-shrink-0 absolute left-0 top-0 z-30 shadow-xl sm:relative sm:shadow-none">
      {/* Header */}
      <div className="h-9 px-3 flex items-center justify-between border-b border-surface-700">
        <span className="text-xs font-medium text-surface-300">History</span>
        <button onClick={toggleSidebar} className="text-surface-400 hover:text-surface-100">
          <X size={14} />
        </button>
      </div>

      {/* Search */}
      <div className="px-2 py-2">
        <div className="relative">
          <Search size={12} className="absolute left-2.5 top-1/2 -translate-y-1/2 text-surface-500" />
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search conversations..."
            className="w-full pl-7 pr-2 py-1.5 text-xs bg-surface-900 border border-surface-600 rounded text-surface-200 placeholder-surface-500 focus:outline-none focus:border-accent"
          />
        </div>
      </div>

      {/* List */}
      <div className="flex-1 overflow-y-auto">
        {filtered.map((conv) => (
          <div
            key={conv.id}
            onClick={() => {
              selectConversation(conv.id);
              toggleSidebar();
            }}
            className={`group px-3 py-2 cursor-pointer flex items-start gap-2 hover:bg-surface-700/50 transition-colors ${
              conv.id === activeConversationId ? 'bg-surface-700/70' : ''
            }`}
          >
            <MessageSquare size={13} className="text-surface-500 mt-0.5 flex-shrink-0" />
            <div className="flex-1 min-w-0">
              <p className="text-xs text-surface-200 truncate">{conv.title}</p>
              <p className="text-[10px] text-surface-500 mt-0.5">
                {new Date(conv.updatedAt).toLocaleDateString()}
              </p>
              {conv.invoiceNumbers.length > 0 && (
                <div className="flex flex-wrap gap-1 mt-1">
                  {conv.invoiceNumbers.slice(0, 2).map((n) => (
                    <span key={n} className="text-[9px] px-1 py-0.5 bg-surface-600 rounded text-surface-300">
                      {n}
                    </span>
                  ))}
                </div>
              )}
            </div>
            <button
              onClick={(e) => {
                e.stopPropagation();
                deleteConversation(conv.id);
              }}
              className="opacity-0 group-hover:opacity-100 text-surface-500 hover:text-red-400 transition-opacity"
            >
              <Trash2 size={12} />
            </button>
          </div>
        ))}
      </div>
    </div>
  );
}
