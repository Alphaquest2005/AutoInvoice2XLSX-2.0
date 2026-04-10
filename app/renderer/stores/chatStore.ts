import { create } from 'zustand';
import type { Conversation, Message, StreamingChunk, ToolUse, ToolResult } from '../../shared/types';

declare global {
  interface Window {
    api: import('../../shared/types').IpcApi;
  }
}

interface ChatState {
  conversations: Conversation[];
  activeConversationId: string | null;
  messages: Message[];
  streamingContent: string;
  streamingToolUse: ToolUse | null;
  isStreaming: boolean;
  sidebarOpen: boolean;

  loadConversations: () => Promise<void>;
  selectConversation: (id: string) => Promise<void>;
  createConversation: () => Promise<void>;
  deleteConversation: (id: string) => Promise<void>;
  sendMessage: (content: string, attachments?: string[]) => void;
  addSystemMessage: (content: string) => Promise<void>;
  handleStreamChunk: (chunk: StreamingChunk) => void;
  toggleSidebar: () => void;
}

export const useChatStore = create<ChatState>((set, get) => ({
  conversations: [],
  activeConversationId: null,
  messages: [],
  streamingContent: '',
  streamingToolUse: null,
  isStreaming: false,
  sidebarOpen: false,

  loadConversations: async () => {
    if (!window.api) return;
    const conversations = await window.api.getConversations();
    const { activeConversationId } = get();
    // Auto-select the most recent conversation if none is active
    if (!activeConversationId && conversations.length > 0) {
      const messages = await window.api.getMessages(conversations[0].id);
      set({ conversations, activeConversationId: conversations[0].id, messages });
    } else {
      set({ conversations });
    }
  },

  selectConversation: async (id: string) => {
    if (!window.api) return;
    const messages = await window.api.getMessages(id);
    set({ activeConversationId: id, messages });
  },

  createConversation: async () => {
    if (!window.api) return;
    const conv = await window.api.createConversation();
    set((state) => ({
      conversations: [conv, ...state.conversations],
      activeConversationId: conv.id,
      messages: [],
    }));
  },

  deleteConversation: async (id: string) => {
    if (!window.api) return;
    await window.api.deleteConversation(id);
    set((state) => ({
      conversations: state.conversations.filter((c) => c.id !== id),
      activeConversationId: state.activeConversationId === id ? null : state.activeConversationId,
      messages: state.activeConversationId === id ? [] : state.messages,
    }));
  },

  sendMessage: (content: string, attachments?: string[]) => {
    if (!window.api) return;
    const { activeConversationId } = get();
    if (!activeConversationId) return;

    // Add user message to local state immediately
    const userMsg: Message = {
      id: Date.now().toString(),
      conversationId: activeConversationId,
      role: 'user',
      content,
      createdAt: new Date().toISOString(),
    };

    set((state) => ({
      messages: [...state.messages, userMsg],
      isStreaming: true,
      streamingContent: '',
      streamingToolUse: null,
    }));

    // Notify main process LLM is busy (for graceful shutdown)
    window.api?.notifyLlmBusy(true);

    window.api.sendMessage(activeConversationId, content, attachments);
  },

  addSystemMessage: async (content: string) => {
    if (!window.api) return;

    let { activeConversationId } = get();

    // Create a conversation if none exists
    if (!activeConversationId) {
      await get().createConversation();
      activeConversationId = get().activeConversationId;
    }

    if (!activeConversationId) return;

    // Persist to main-process DB so LLM sees it in context
    await window.api.addSystemMessage(activeConversationId, content);

    const sysMsg: Message = {
      id: Date.now().toString(),
      conversationId: activeConversationId,
      role: 'system',
      content,
      createdAt: new Date().toISOString(),
    };

    set((state) => ({
      messages: [...state.messages, sysMsg],
    }));
  },

  handleStreamChunk: (chunk: StreamingChunk) => {
    switch (chunk.type) {
      case 'text':
        set((state) => ({
          streamingContent: state.streamingContent + (chunk.text || ''),
        }));
        break;

      case 'tool_use_start': {
        const toolUse = chunk.toolUse;
        set({ streamingToolUse: toolUse || null });
        // Show tool call in chat so user can see LLM actions
        if (toolUse) {
          const { activeConversationId } = get();
          if (activeConversationId) {
            const inputSummary = Object.entries(toolUse.input)
              .map(([k, v]) => {
                const vs = typeof v === 'string' ? v : JSON.stringify(v);
                if (k === 'content' && vs.length > 100) return `${k}: ${vs.length} chars`;
                return `${k}: ${vs.length > 100 ? vs.slice(0, 100) + '...' : vs}`;
              })
              .join(', ');
            const toolMsg: Message = {
              id: `tool-call-${Date.now()}`,
              conversationId: activeConversationId,
              role: 'system',
              content: `[Tool Call] ${toolUse.name}(${inputSummary})`,
              createdAt: new Date().toISOString(),
            };
            set((state) => ({ messages: [...state.messages, toolMsg] }));
          }
        }
        break;
      }

      case 'tool_use_end': {
        const toolResult = chunk.toolResult;
        set({ streamingToolUse: null });
        // Show tool result in chat
        if (toolResult) {
          const { activeConversationId } = get();
          if (activeConversationId) {
            const content = toolResult.content || '';
            const preview = content.length > 500 ? content.slice(0, 500) + '...' : content;
            const resultMsg: Message = {
              id: `tool-result-${Date.now()}`,
              conversationId: activeConversationId,
              role: 'system',
              content: `[Tool Result] ${toolResult.isError ? 'ERROR: ' : ''}${preview}`,
              createdAt: new Date().toISOString(),
            };
            set((state) => ({ messages: [...state.messages, resultMsg] }));
          }
        }
        break;
      }

      case 'message_end': {
        const { streamingContent, activeConversationId } = get();
        if (streamingContent && activeConversationId) {
          const assistantMsg: Message = {
            id: Date.now().toString(),
            conversationId: activeConversationId,
            role: 'assistant',
            content: streamingContent,
            createdAt: new Date().toISOString(),
          };
          set((state) => ({
            messages: [...state.messages, assistantMsg],
            streamingContent: '',
            isStreaming: false,
            streamingToolUse: null,
          }));
        } else {
          set({ isStreaming: false, streamingContent: '', streamingToolUse: null });
        }
        // Notify main process LLM is idle (for graceful shutdown)
        window.api?.notifyLlmBusy(false);
        // Reload conversations to get updated titles
        get().loadConversations();
        break;
      }

      case 'error':
        set({
          isStreaming: false,
          streamingContent: '',
          streamingToolUse: null,
        });
        // Notify main process LLM is idle (for graceful shutdown)
        window.api?.notifyLlmBusy(false);
        break;
    }
  },

  toggleSidebar: () => set((state) => ({ sidebarOpen: !state.sidebarOpen })),
}));
