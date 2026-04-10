import { useEffect } from 'react';
import { useChatStore } from '../stores/chatStore';

export function useChat() {
  const {
    conversations,
    activeConversationId,
    messages,
    streamingContent,
    isStreaming,
    loadConversations,
    selectConversation,
    createConversation,
    deleteConversation,
    sendMessage,
  } = useChatStore();

  return {
    conversations,
    activeConversationId,
    messages,
    streamingContent,
    isStreaming,
    loadConversations,
    selectConversation,
    createConversation,
    deleteConversation,
    sendMessage,
  };
}
