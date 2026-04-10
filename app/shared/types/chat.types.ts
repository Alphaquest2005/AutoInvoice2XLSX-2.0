export interface Conversation {
  id: string;
  title: string;
  createdAt: string;
  updatedAt: string;
  tags: string[];
  invoiceNumbers: string[];
}

export interface Message {
  id: string;
  conversationId: string;
  role: 'user' | 'assistant' | 'system';
  content: string;
  toolUse?: ToolUse[];
  toolResult?: ToolResult[];
  createdAt: string;
}

export interface ToolUse {
  id: string;
  name: string;
  input: Record<string, unknown>;
}

export interface ToolResult {
  toolUseId: string;
  content: string;
  isError?: boolean;
}

export interface StreamingChunk {
  type: 'text' | 'tool_use_start' | 'tool_use_input' | 'tool_use_end' | 'message_end' | 'error';
  text?: string;
  toolUse?: ToolUse;
  toolResult?: ToolResult;
}
