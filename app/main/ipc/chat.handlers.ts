/**
 * IPC handlers for chat / LLM conversation management.
 * Channels: chat:getConversations, chat:getMessages, chat:createConversation,
 *           chat:deleteConversation, chat:addSystemMessage, chat:sendMessage
 */

import { ipcMain } from 'electron';
import fs from 'fs';
import path from 'path';
import type { HandlerDependencies } from './index';
import {
  getConversations,
  getMessages,
  createConversation,
  deleteConversation,
  addMessage,
} from '../stores/chat.store';
import { LlmClient } from '../services/llm-client';
import { loadSettings } from '../utils/settings';
import { loadPipelineConfig } from '../services/config-loader';
import { getAgentTools, executeAgentTool } from '../services/agent-tools';
import type { ToolUse } from '../../shared/types';

// ─── Module-scoped lazy singleton ────────────────────────────────────────────

let llmClient: LlmClient | null = null;

function getLlmClient(): LlmClient {
  if (!llmClient) {
    llmClient = new LlmClient();
  }
  return llmClient;
}

// ─── System prompt ───────────────────────────────────────────────────────────

function buildSystemPrompt(): string {
  const pipelineConfig = loadPipelineConfig();
  const pipelineName = pipelineConfig?.pipeline?.name ?? 'AutoInvoice2XLSX';
  const pipelineVersion = pipelineConfig?.pipeline?.version ?? '2.0';

  return [
    `You are an expert CARICOM customs invoice processing assistant for ${pipelineName} v${pipelineVersion}.`,
    'You help users convert PDF invoices into properly formatted XLSX files for ASYCUDA World import.',
    '',
    'You have access to tools for:',
    '- Running the full extraction/processing pipeline or individual stages',
    '- Reading, writing, and editing files in the workspace',
    '- Querying and updating classification rules',
    '- Validating XLSX output against CARICOM requirements',
    '- Looking up tariff codes and CET entries',
    '- Splitting multi-invoice PDFs',
    '- Searching chat history for previous conversations',
    '',
    'Always be precise with file paths. When running the pipeline, report progress and results clearly.',
    'If a tool call fails, explain the error and suggest a fix rather than silently retrying.',
  ].join('\n');
}

// ─── Handler registration ────────────────────────────────────────────────────

export function registerChatHandlers(deps: HandlerDependencies): void {
  ipcMain.handle('chat:getConversations', async () => {
    try {
      return getConversations();
    } catch (err) {
      console.error('[chat:getConversations] Error:', err);
      return [];
    }
  });

  ipcMain.handle('chat:getMessages', async (_e, conversationId: string) => {
    try {
      return getMessages(conversationId);
    } catch (err) {
      console.error('[chat:getMessages] Error:', err);
      return [];
    }
  });

  ipcMain.handle('chat:createConversation', async (_e, title?: string) => {
    try {
      return createConversation(title);
    } catch (err) {
      console.error('[chat:createConversation] Error:', err);
      throw err;
    }
  });

  ipcMain.handle('chat:deleteConversation', async (_e, id: string) => {
    try {
      deleteConversation(id);
    } catch (err) {
      console.error('[chat:deleteConversation] Error:', err);
      throw err;
    }
  });

  ipcMain.handle('chat:addSystemMessage', async (_e, conversationId: string, content: string) => {
    try {
      addMessage(conversationId, 'system', content);
    } catch (err) {
      console.error('[chat:addSystemMessage] Error:', err);
      throw err;
    }
  });

  ipcMain.on(
    'chat:sendMessage',
    async (_event, conversationId: string, content: string, attachments?: string[]) => {
      const win = deps.getMainWindow();
      if (!win) return;

      const send = (chunk: Record<string, unknown>) => {
        win.webContents.send('chat:streamChunk', chunk);
      };

      try {
        // 1. Initialise client (lazy)
        const client = getLlmClient();

        // 2. Build system prompt
        const systemPrompt = buildSystemPrompt();

        // 3. Save user message to store
        addMessage(conversationId, 'user', content);

        // 4. Build message history for the LLM
        //    Map system messages to user role with [System Event] prefix (matches v1)
        //    Merge consecutive same-role messages
        const storedMessages = getMessages(conversationId);
        const messageHistory: { role: 'user' | 'assistant'; content: string }[] = [];
        for (const m of storedMessages) {
          const role = (m.role === 'system' ? 'user' : m.role) as 'user' | 'assistant';
          const msgContent = m.role === 'system' ? `[System Event] ${m.content}` : m.content;
          if (messageHistory.length > 0 && messageHistory[messageHistory.length - 1].role === role) {
            messageHistory[messageHistory.length - 1].content += '\n' + msgContent;
          } else {
            messageHistory.push({ role, content: msgContent });
          }
        }

        // 5. Attach file content if provided (matches v1)
        if (attachments?.length && messageHistory.length > 0) {
          const attachContent = attachments
            .map((f) => {
              try {
                const fileContent = fs.readFileSync(f, 'utf-8');
                return `\n\n[Attached file: ${path.basename(f)}]\n\`\`\`\n${fileContent.slice(0, 10000)}\n\`\`\``;
              } catch {
                return `\n\n[Could not read: ${path.basename(f)}]`;
              }
            })
            .join('');
          messageHistory[messageHistory.length - 1].content += attachContent;
        }

        // 5. Get available agent tools
        const tools = getAgentTools();

        // 6. Accumulate full assistant response for storage
        let fullResponse = '';

        // 7. Stream the LLM response
        await client.streamMessage(systemPrompt, messageHistory, tools, {
          onText: (text: string) => {
            fullResponse += text;
            send({ type: 'text', text });
          },

          onToolUse: async (toolUse: ToolUse) => {
            // Notify renderer that a tool is being used
            send({
              type: 'tool_use_start',
              toolUse: { id: toolUse.id, name: toolUse.name, input: toolUse.input },
            });

            // Execute the tool
            const result = await executeAgentTool(toolUse.name, toolUse.input);

            // Auto-load XLSX if the tool produced one
            if (typeof result === 'object' && result !== null) {
              const r = result as Record<string, unknown>;
              if (r.status === 'success' && typeof r.output === 'string' && r.output.endsWith('.xlsx')) {
                const fsCheck = require('fs');
                if (fsCheck.existsSync(r.output)) {
                  win.webContents.send('xlsx:autoLoad', r.output);
                }
              }
            }

            // Notify renderer of tool result
            const resultStr = typeof result === 'string' ? result : JSON.stringify(result);
            send({
              type: 'tool_use_end',
              toolResult: {
                toolUseId: toolUse.id,
                content: resultStr.length > 500 ? resultStr.slice(0, 500) + '...' : resultStr,
                isError: typeof result === 'object' && result !== null && 'error' in (result as Record<string, unknown>),
              },
            });

            return result;
          },

          onEnd: () => {
            // Save full assistant response to store
            if (fullResponse.trim()) {
              addMessage(conversationId, 'assistant', fullResponse);
            }
            send({ type: 'message_end' });
          },

          onError: (error: string) => {
            console.error('[chat:sendMessage] LLM error:', error);
            send({ type: 'error', text: error });
          },
        });
      } catch (err) {
        const msg = err instanceof Error ? err.message : 'Unknown error in chat handler';
        console.error('[chat:sendMessage] Error:', msg);
        send({ type: 'error', text: msg });
      }
    },
  );
}
