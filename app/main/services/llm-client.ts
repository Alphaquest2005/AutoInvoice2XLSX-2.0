/**
 * LLM Client — Agentic tool-use loop with context management.
 *
 * Ported from v1 ZaiClient with the following improvements:
 * - Loads settings from SSOT (with constructor overrides)
 * - Uses shared types (StreamingChunk, ToolUse, ToolResult, Message)
 * - Extracted constants, reduced nesting, improved type safety
 */

import Anthropic from '@anthropic-ai/sdk';
import { loadSettings } from '../utils/settings';
import type { ToolUse, ToolResult, StreamingChunk } from '../../shared/types';

// ─── Constants ──────────────────────────────────────────────────────────────

/** Maximum characters kept per tool result before truncation. */
const TOOL_RESULT_MAX_CHARS = 20_000;

/** Abort the agentic loop after this many consecutive failures of the same tool. */
const MAX_CONSECUTIVE_FAILURES = 5;

/** Hard-stop if the exact same tool+input signature repeats this many times. */
const MAX_REPEATED_CALLS = 5;

/** Tools where identical re-invocation is intentional (e.g. re-run after config edits). */
const REPEAT_EXEMPT_TOOLS = new Set(['run_pipeline', 'run_folder_pipeline']);

/** Target context window — leaves headroom below the model's hard limit. */
const MAX_CONTEXT_TOKENS = 100_000;

/** Begin pruning when utilisation exceeds this fraction of MAX_CONTEXT_TOKENS. */
const CONTEXT_PRUNE_THRESHOLD = 0.70;

/** Number of most-recent tool results kept verbatim during pruning. */
const SLIDING_WINDOW_SIZE = 12;

/** Placeholder injected when an old tool result is pruned. */
const MASKED_RESULT = '[Previous result pruned - call tool again if needed]';

/** Warn about tool-call efficiency after this many calls. */
const EFFICIENCY_WARNING_THRESHOLD = 8;

/** Maximum tokens the model may emit per turn. */
const MAX_OUTPUT_TOKENS = 8192;

/** Number of recent call signatures retained for repetition detection. */
const RECENT_SIGNATURE_WINDOW = 10;

// ─── Types ──────────────────────────────────────────────────────────────────

export interface ToolDef {
  name: string;
  description: string;
  input_schema: Record<string, unknown>;
}

interface ContextStats {
  totalTokens: number;
  messageCount: number;
  toolResultCount: number;
  maskedCount: number;
  systemTokens: number;
  assistantTokens: number;
  userTokens: number;
  largestMessage: { index: number; tokens: number; type: string };
}

interface LlmClientOptions {
  apiKey?: string;
  baseUrl?: string;
  model?: string;
}

export interface StreamCallbacks {
  onText: (text: string) => void;
  onToolUse: (toolUse: ToolUse) => Promise<unknown>;
  onEnd: () => void;
  onError: (error: string) => void;
}

// ─── Token Estimation ───────────────────────────────────────────────────────

/**
 * Rough token estimate — ~4 chars per token for English/code.
 */
export function estimateTokens(content: unknown): number {
  if (content == null) return 0;
  const str = typeof content === 'string' ? content : JSON.stringify(content);
  return Math.ceil(str.length / 4);
}

// ─── Context Diagnostics ────────────────────────────────────────────────────

export function getContextStats(
  systemPrompt: string,
  messages: Anthropic.MessageParam[],
): ContextStats {
  const systemTokens = estimateTokens(systemPrompt);
  let assistantTokens = 0;
  let userTokens = 0;
  let toolResultCount = 0;
  let maskedCount = 0;
  let largestMessage = { index: -1, tokens: 0, type: '' };

  for (let i = 0; i < messages.length; i++) {
    const msg = messages[i];
    const tokens = estimateTokens(msg.content);

    if (msg.role === 'assistant') {
      assistantTokens += tokens;
      if (Array.isArray(msg.content)) {
        for (const block of msg.content as any[]) {
          if (block.type === 'tool_use') toolResultCount++;
        }
      }
    } else {
      userTokens += tokens;
      if (Array.isArray(msg.content)) {
        for (const block of msg.content as any[]) {
          if (block.type === 'tool_result' && block.content === MASKED_RESULT) {
            maskedCount++;
          }
        }
      }
    }

    if (tokens > largestMessage.tokens) {
      largestMessage = { index: i, tokens, type: msg.role };
    }
  }

  return {
    totalTokens: systemTokens + assistantTokens + userTokens,
    messageCount: messages.length,
    toolResultCount,
    maskedCount,
    systemTokens,
    assistantTokens,
    userTokens,
    largestMessage,
  };
}

function logContextDiagnostics(
  iteration: number,
  stats: ContextStats,
  action: 'check' | 'prune' | 'post-prune',
): void {
  const pct = ((stats.totalTokens / MAX_CONTEXT_TOKENS) * 100).toFixed(1);
  const tag = '[llm-context]';

  switch (action) {
    case 'check':
      console.log(
        `${tag} iter=${iteration} tokens=${stats.totalTokens} (${pct}% of ${MAX_CONTEXT_TOKENS}) ` +
          `msgs=${stats.messageCount} tools=${stats.toolResultCount} masked=${stats.maskedCount}`,
      );
      break;

    case 'prune': {
      const threshPct = (CONTEXT_PRUNE_THRESHOLD * 100).toFixed(0);
      const threshTokens = Math.floor(MAX_CONTEXT_TOKENS * CONTEXT_PRUNE_THRESHOLD);
      console.warn(
        `${tag} PRUNING at iter=${iteration} — tokens=${stats.totalTokens} exceeds threshold (${threshPct}% = ${threshTokens})`,
      );
      console.warn(
        `${tag} breakdown: system=${stats.systemTokens} assistant=${stats.assistantTokens} user=${stats.userTokens}`,
      );
      if (stats.largestMessage.index >= 0) {
        console.warn(
          `${tag} largest msg: index=${stats.largestMessage.index} tokens=${stats.largestMessage.tokens} type=${stats.largestMessage.type}`,
        );
      }
      break;
    }

    case 'post-prune':
      console.log(
        `${tag} after pruning: tokens=${stats.totalTokens} (${pct}%) masked=${stats.maskedCount}/${stats.toolResultCount}`,
      );
      break;
  }
}

// ─── Observation Masking (Prune Old Tool Results) ───────────────────────────

/**
 * Replace old tool results with short placeholders, keeping the last
 * `keepLastN` results intact so the model retains recent context.
 */
export function pruneToolResults(
  messages: Anthropic.MessageParam[],
  keepLastN: number = SLIDING_WINDOW_SIZE,
): Anthropic.MessageParam[] {
  const toolResultIndices: number[] = [];

  for (let i = 0; i < messages.length; i++) {
    const msg = messages[i];
    if (msg.role === 'user' && Array.isArray(msg.content)) {
      if ((msg.content as any[]).some((b) => b.type === 'tool_result')) {
        toolResultIndices.push(i);
      }
    }
  }

  const cutoff = Math.max(0, toolResultIndices.length - keepLastN);
  const toMask = new Set(toolResultIndices.slice(0, cutoff));

  if (toMask.size === 0) return messages;

  console.log(`[llm-context] masking ${toMask.size} old tool results, keeping last ${keepLastN}`);

  return messages.map((msg, idx) => {
    if (!toMask.has(idx)) return msg;

    const maskedContent = (msg.content as any[]).map((block) =>
      block.type === 'tool_result'
        ? { type: 'tool_result' as const, tool_use_id: block.tool_use_id, content: MASKED_RESULT }
        : block,
    );

    return { role: msg.role as 'user', content: maskedContent };
  });
}

/**
 * Check context size and prune when approaching the limit.
 */
export function manageContext(
  systemPrompt: string,
  messages: Anthropic.MessageParam[],
  iteration: number,
): Anthropic.MessageParam[] {
  const stats = getContextStats(systemPrompt, messages);
  logContextDiagnostics(iteration, stats, 'check');

  const threshold = MAX_CONTEXT_TOKENS * CONTEXT_PRUNE_THRESHOLD;
  if (stats.totalTokens <= threshold) return messages;

  logContextDiagnostics(iteration, stats, 'prune');

  const pruned = pruneToolResults(messages, SLIDING_WINDOW_SIZE);
  const postStats = getContextStats(systemPrompt, pruned);
  logContextDiagnostics(iteration, postStats, 'post-prune');

  if (postStats.totalTokens > threshold) {
    console.warn(
      `[llm-context] WARNING: still at ${postStats.totalTokens} tokens after pruning. ` +
        `Consider reducing SLIDING_WINDOW_SIZE or TOOL_RESULT_MAX_CHARS.`,
    );
  }

  return pruned;
}

// ─── Tool Result Truncation ─────────────────────────────────────────────────

function truncateToolResult(result: unknown): string {
  const str = typeof result === 'string' ? result : JSON.stringify(result);
  if (str.length <= TOOL_RESULT_MAX_CHARS) return str;

  // For objects with a long `content` field (e.g. read_file), truncate content specifically
  if (typeof result === 'object' && result !== null) {
    const obj = result as Record<string, unknown>;
    if (typeof obj.content === 'string' && obj.content.length > TOOL_RESULT_MAX_CHARS - 500) {
      return JSON.stringify({
        ...obj,
        content: (obj.content as string).slice(0, TOOL_RESULT_MAX_CHARS - 500),
        _result_truncated: true,
        _original_content_length: (obj.content as string).length,
        _truncation_note:
          'Content was truncated to fit context limits. Use read_file with offset/limit for specific sections.',
      });
    }
  }

  // Fallback: raw string truncation
  return JSON.stringify({
    _result_truncated: true,
    _original_length: str.length,
    _truncation_note: 'Result was too large and has been truncated.',
    partial_result: str.slice(0, TOOL_RESULT_MAX_CHARS - 200),
  });
}

// ─── Malformed JSON Recovery ────────────────────────────────────────────────

/**
 * When the model streams a very large tool input (e.g. write_file with hundreds
 * of lines) the accumulated JSON may be malformed. Attempt to extract known
 * fields so the tool handler can at least report a meaningful error.
 */
function recoverToolInput(raw: string): Record<string, unknown> {
  const extracted: Record<string, unknown> = {};

  // Extract simple string fields
  const stringFields = ['path', 'input_file', 'output_file', 'stage', 'query', 'action', 'rule_id'];
  for (const field of stringFields) {
    const match = raw.match(new RegExp(`"${field}"\\s*:\\s*"([^"]*)"`));
    if (match) extracted[field] = match[1];
  }

  // Extract long "content" field
  const contentStart = raw.indexOf('"content"');
  if (contentStart !== -1) {
    const valueStart = raw.indexOf(':"', contentStart) + 2;
    if (valueStart > 1) {
      let value = raw.slice(valueStart);
      if (value.endsWith('"}')) value = value.slice(0, -2);
      else if (value.endsWith('"')) value = value.slice(0, -1);
      try {
        extracted.content = JSON.parse('"' + value + '"');
      } catch {
        extracted.content = value
          .replace(/\\n/g, '\n')
          .replace(/\\t/g, '\t')
          .replace(/\\"/g, '"')
          .replace(/\\\\/g, '\\');
      }
    }
  }

  if (Object.keys(extracted).length > 0) {
    extracted._truncated = true;
    console.log(`[llm] Recovered fields from malformed JSON: ${Object.keys(extracted).join(', ')}`);
    return extracted;
  }

  return { raw, _truncated: true };
}

// ─── LlmClient ──────────────────────────────────────────────────────────────

export class LlmClient {
  private client: Anthropic;
  private model: string;

  constructor(options: LlmClientOptions = {}) {
    const settings = loadSettings();
    const apiKey = options.apiKey ?? settings.apiKey;
    const baseURL = options.baseUrl ?? settings.baseUrl;
    this.model = options.model ?? settings.model;

    this.client = new Anthropic({ apiKey, baseURL });
  }

  /**
   * Run the agentic streaming loop.
   *
   * The model streams text and tool-use blocks. When tool use is requested the
   * callback executes the tool and the result is fed back. The loop continues
   * until the model stops requesting tools, or a safety rail triggers.
   */
  async streamMessage(
    systemPrompt: string,
    messages: Array<{ role: 'user' | 'assistant'; content: string }>,
    tools: ToolDef[],
    callbacks: StreamCallbacks,
  ): Promise<void> {
    const { onText, onToolUse, onEnd, onError } = callbacks;

    try {
      let currentMessages: Anthropic.MessageParam[] = messages.map((m) => ({
        role: m.role,
        content: m.content,
      }));

      let continueLoop = true;
      let iteration = 0;
      let consecutiveFailures = 0;
      let lastFailedTool = '';

      // Repetition detection
      const recentSignatures: string[] = [];

      // Efficiency tracking
      const filesRead = new Map<string, number>();
      const editAttempts = new Map<string, number>();
      let totalToolCalls = 0;

      while (continueLoop) {
        continueLoop = false;
        iteration++;
        console.log(`[llm] Agentic loop iteration ${iteration}`);

        // Context management — prune if approaching limit
        currentMessages = manageContext(systemPrompt, currentMessages, iteration);

        const toolUseBlocks = await this.streamOneTurn(
          systemPrompt,
          currentMessages,
          tools,
          onText,
        );

        if (toolUseBlocks.length === 0) break;

        continueLoop = true;

        // ── Repetition detection ──────────────────────────────────────────
        const allExempt = toolUseBlocks.every((t) => REPEAT_EXEMPT_TOOLS.has(t.name));
        const signature = toolUseBlocks
          .map((t) => `${t.name}:${JSON.stringify(t.input)}`)
          .sort()
          .join('|');

        const repeatCount = recentSignatures.filter((s) => s === signature).length;
        recentSignatures.push(signature);
        if (recentSignatures.length > RECENT_SIGNATURE_WINDOW) recentSignatures.shift();

        if (!allExempt && repeatCount >= MAX_REPEATED_CALLS) {
          console.warn(`[llm] Same tool call repeated ${repeatCount + 1} times, stopping loop`);
          onText(
            `\n\n[Stopped: detected repeated tool calls. The same request was made ${repeatCount + 1} times. Try rephrasing or a different approach.]`,
          );
          break;
        }

        // Inject strategy-change hint on early repeats (before hard-stop)
        if (!allExempt && repeatCount >= 2) {
          console.warn(`[llm] Repeat #${repeatCount + 1} detected, injecting strategy change hint`);
          currentMessages.push({
            role: 'user',
            content:
              `STRATEGY CHANGE REQUIRED: You have made the same tool call ${repeatCount + 1} times with identical inputs. ` +
              `Your current approach is not working. Try a completely different strategy:\n` +
              `- If reading a file keeps failing, try a different file path or use list_files first\n` +
              `- If editing keeps failing, re-read the file first and match the exact text\n` +
              `- If the pipeline keeps failing after format spec changes, re-read the PDF text to verify your regex patterns match the actual content\n` +
              `- If stuck, explain what's going wrong and stop`,
          });
        }

        // ── Append assistant message with tool_use blocks ─────────────────
        currentMessages.push({
          role: 'assistant',
          content: toolUseBlocks.map((t) => ({
            type: 'tool_use' as const,
            id: t.id,
            name: t.name,
            input: t.input,
          })),
        });

        // ── Execute tools and collect results ─────────────────────────────
        const { toolResults, hadFailure } = await this.executeTools(
          toolUseBlocks,
          onToolUse,
          { filesRead, editAttempts, totalToolCalls },
        );

        totalToolCalls += toolUseBlocks.length;

        // Inject efficiency warning if warranted
        const efficiencyWarning = this.checkEfficiency(
          toolUseBlocks,
          filesRead,
          editAttempts,
          totalToolCalls + toolUseBlocks.length,
        );
        if (efficiencyWarning) {
          toolResults.push({
            type: 'tool_result' as const,
            tool_use_id: 'efficiency-warning',
            content: efficiencyWarning,
          });
        }

        currentMessages.push({ role: 'user', content: toolResults });

        // ── Consecutive failure tracking ──────────────────────────────────
        if (hadFailure.tool) {
          if (hadFailure.tool === lastFailedTool) {
            consecutiveFailures++;
          } else {
            consecutiveFailures = 1;
            lastFailedTool = hadFailure.tool;
          }
          console.warn(`[llm] Tool ${hadFailure.tool} failed (consecutive: ${consecutiveFailures})`);
        } else {
          consecutiveFailures = 0;
          lastFailedTool = '';
        }

        if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
          console.warn(
            `[llm] ${consecutiveFailures} consecutive failures of ${lastFailedTool}, aborting loop`,
          );
          onText(
            `\n\n[Stopped: ${lastFailedTool} failed ${consecutiveFailures} times consecutively. Try a different approach.]`,
          );
          break;
        }
      }

      onEnd();
    } catch (error: unknown) {
      const msg = error instanceof Error ? error.message : 'LLM API error';
      onError(msg);
    }
  }

  // ── Private helpers ─────────────────────────────────────────────────────────

  /**
   * Stream a single model turn, collecting text deltas and tool-use blocks.
   * Returns the parsed tool-use blocks (empty array if the model produced none).
   */
  private async streamOneTurn(
    systemPrompt: string,
    messages: Anthropic.MessageParam[],
    tools: ToolDef[],
    onText: (text: string) => void,
  ): Promise<ToolUse[]> {
    const stream = this.client.messages.stream({
      model: this.model,
      max_tokens: MAX_OUTPUT_TOKENS,
      system: systemPrompt,
      messages,
      tools: tools as Anthropic.Tool[],
    });

    const toolUseBlocks: ToolUse[] = [];
    let currentToolId = '';
    let currentToolName = '';
    let currentToolInput = '';

    for await (const event of stream) {
      if (event.type === 'content_block_start') {
        const block = (event as any).content_block;
        if (block?.type === 'tool_use') {
          currentToolId = block.id;
          currentToolName = block.name;
          currentToolInput = '';
        }
      } else if (event.type === 'content_block_delta') {
        const delta = (event as any).delta;
        if (delta?.type === 'text_delta') {
          onText(delta.text);
        } else if (delta?.type === 'input_json_delta') {
          currentToolInput += delta.partial_json;
        }
      } else if (event.type === 'content_block_stop') {
        if (currentToolId) {
          const input = this.parseToolInput(currentToolName, currentToolInput);
          toolUseBlocks.push({ id: currentToolId, name: currentToolName, input });
          currentToolId = '';
          currentToolName = '';
          currentToolInput = '';
        }
      }
    }

    return toolUseBlocks;
  }

  /**
   * Parse accumulated JSON for a tool input, falling back to field extraction
   * when the JSON is malformed (common with very large write_file payloads).
   */
  private parseToolInput(toolName: string, raw: string): Record<string, unknown> {
    if (!raw) return {};
    try {
      return JSON.parse(raw);
    } catch (err) {
      console.error(
        `[llm] JSON parse failed for ${toolName}: ${(err as Error).message}`,
      );
      console.error(`[llm] Raw input length: ${raw.length}, first 200: ${raw.slice(0, 200)}`);
      return recoverToolInput(raw);
    }
  }

  /**
   * Execute tool-use blocks and collect Anthropic-formatted tool_result entries.
   */
  private async executeTools(
    toolUseBlocks: ToolUse[],
    onToolUse: (toolUse: ToolUse) => Promise<unknown>,
    _tracking: {
      filesRead: Map<string, number>;
      editAttempts: Map<string, number>;
      totalToolCalls: number;
    },
  ): Promise<{
    toolResults: any[];
    hadFailure: { tool: string } | { tool: '' };
  }> {
    const toolResults: any[] = [];
    let failedTool = '';

    for (const tool of toolUseBlocks) {
      const result = await onToolUse(tool);
      const resultStr = truncateToolResult(result);

      // Track reads/edits for efficiency warnings
      if (tool.name === 'read_file') {
        const path = (tool.input as any).path || '';
        _tracking.filesRead.set(path, (_tracking.filesRead.get(path) || 0) + 1);
      }
      if (tool.name === 'edit_file') {
        const path = (tool.input as any).path || '';
        _tracking.editAttempts.set(path, (_tracking.editAttempts.get(path) || 0) + 1);
      }

      // Detect errors
      const isError =
        typeof result === 'object' && result !== null && 'error' in (result as Record<string, unknown>);
      if (isError) failedTool = tool.name;

      toolResults.push({
        type: 'tool_result' as const,
        tool_use_id: tool.id,
        content: resultStr,
      });
    }

    return {
      toolResults,
      hadFailure: failedTool ? { tool: failedTool } : { tool: '' as const },
    };
  }

  /**
   * Return an efficiency warning string if warranted, or empty string.
   */
  private checkEfficiency(
    toolUseBlocks: ToolUse[],
    filesRead: Map<string, number>,
    editAttempts: Map<string, number>,
    totalToolCalls: number,
  ): string {
    // Check duplicate reads
    for (const tool of toolUseBlocks) {
      if (tool.name === 'read_file') {
        const path = (tool.input as any).path || '';
        const count = filesRead.get(path) || 0;
        if (count > 1) {
          console.warn(`[llm-efficiency] Duplicate read of ${path} (${count} times)`);
          return `EFFICIENCY WARNING: You already read "${path}" earlier. Do not read the same file multiple times.`;
        }
      }

      if (tool.name === 'edit_file') {
        const path = (tool.input as any).path || '';
        const count = editAttempts.get(path) || 0;
        if (count > 1) {
          console.warn(`[llm-efficiency] Repeated edit_file failure on ${path} (${count} attempts)`);
          return `EFFICIENCY WARNING: edit_file failed ${count} times on "${path}". STOP retrying with different whitespace. Re-read the EXACT lines carefully and copy the text EXACTLY as shown (without line number prefixes).`;
        }
      }
    }

    // General call-count warning
    if (totalToolCalls >= EFFICIENCY_WARNING_THRESHOLD) {
      console.warn(`[llm-efficiency] ${totalToolCalls} tool calls — approaching efficiency limit`);
      return `EFFICIENCY WARNING: You have made ${totalToolCalls} tool calls. Target is 3-5 per fix. Be more focused: read once, fix once, test once.`;
    }

    return '';
  }
}
