/**
 * Web Search Service
 *
 * Provides web search via Z.AI MCP (Model Context Protocol) server.
 * Uses direct HTTP JSON-RPC calls — no MCP SDK needed.
 *
 * Endpoint: https://api.z.ai/api/mcp/web_search_prime/mcp
 * Auth: Bearer token (Z.AI API key)
 * Tool: webSearchPrime
 */

// ─── Types ──────────────────────────────────────────────────────

const MCP_URL = 'https://api.z.ai/api/mcp/web_search_prime/mcp' as const;

const CLIENT_INFO = {
  name: 'AutoInvoice2XLSX',
  version: '2.0.0',
} as const;

export interface WebSearchResult {
  query: string;
  results: string;
  error?: string;
}

interface McpContentEntry {
  type: string;
  text?: string;
}

interface McpJsonRpcResponse {
  result?: {
    content?: McpContentEntry[];
  } | string;
  error?: {
    message?: string;
  };
}

// ─── Public API ─────────────────────────────────────────────────

/**
 * Search the web via Z.AI's MCP search endpoint.
 * Tries a direct JSON-RPC POST first; falls back to SSE-initialized session if needed.
 */
export async function webSearch(query: string, apiKey: string): Promise<WebSearchResult> {
  if (!query?.trim()) {
    return { query, results: '', error: 'Empty search query' };
  }
  if (!apiKey) {
    return { query, results: '', error: 'No API key configured' };
  }

  try {
    const response = await fetch(MCP_URL, {
      method: 'POST',
      headers: buildHeaders(apiKey),
      body: buildToolCallBody(query),
    });

    // If endpoint requires session initialization, try that flow
    if (response.status === 405 || response.status === 400) {
      console.log('[web-search] Direct call returned', response.status, '— trying session init');
      return await webSearchWithSession(query, apiKey);
    }

    if (!response.ok) {
      const body = await response.text();
      return { query, results: '', error: `HTTP ${response.status}: ${body.slice(0, 200)}` };
    }

    const json: McpJsonRpcResponse = await response.json();

    if (json.error) {
      return { query, results: '', error: json.error.message ?? JSON.stringify(json.error) };
    }

    return { query, results: extractResultText(json) };
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : 'Web search failed';
    return { query, results: '', error: message };
  }
}

// ─── Session-Based Fallback ─────────────────────────────────────

/**
 * Fallback: initialize an MCP session first, then make the tool call.
 * Some MCP servers require an initialize handshake before accepting tool calls.
 */
async function webSearchWithSession(query: string, apiKey: string): Promise<WebSearchResult> {
  const headers = buildHeaders(apiKey);

  // Step 1: Initialize session
  const initResponse = await fetch(MCP_URL, {
    method: 'POST',
    headers,
    body: JSON.stringify({
      jsonrpc: '2.0',
      id: 0,
      method: 'initialize',
      params: {
        protocolVersion: '2024-11-05',
        capabilities: {},
        clientInfo: CLIENT_INFO,
      },
    }),
  });

  if (!initResponse.ok) {
    const body = await initResponse.text();
    return { query, results: '', error: `Session init failed: HTTP ${initResponse.status}: ${body.slice(0, 200)}` };
  }

  // Capture session ID from response header if present
  const sessionId = initResponse.headers.get('mcp-session-id');
  const toolHeaders: Record<string, string> = { ...headers };
  if (sessionId) {
    toolHeaders['mcp-session-id'] = sessionId;
  }

  // Step 2: Send initialized notification
  await fetch(MCP_URL, {
    method: 'POST',
    headers: toolHeaders,
    body: JSON.stringify({
      jsonrpc: '2.0',
      method: 'notifications/initialized',
    }),
  });

  // Step 3: Call the search tool
  const toolResponse = await fetch(MCP_URL, {
    method: 'POST',
    headers: toolHeaders,
    body: buildToolCallBody(query),
  });

  if (!toolResponse.ok) {
    const body = await toolResponse.text();
    return { query, results: '', error: `Tool call failed: HTTP ${toolResponse.status}: ${body.slice(0, 200)}` };
  }

  const json: McpJsonRpcResponse = await toolResponse.json();

  if (json.error) {
    return { query, results: '', error: json.error.message ?? JSON.stringify(json.error) };
  }

  return { query, results: extractResultText(json) };
}

// ─── Helpers ────────────────────────────────────────────────────

function buildHeaders(apiKey: string): Record<string, string> {
  return {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${apiKey}`,
  };
}

function buildToolCallBody(query: string): string {
  return JSON.stringify({
    jsonrpc: '2.0',
    id: 1,
    method: 'tools/call',
    params: {
      name: 'webSearchPrime',
      arguments: { query },
    },
  });
}

/**
 * Extract readable text from an MCP JSON-RPC tool result.
 */
function extractResultText(json: McpJsonRpcResponse): string {
  // Standard MCP tool result format: { result: { content: [{ type: "text", text: "..." }] } }
  if (typeof json.result === 'object' && json.result?.content && Array.isArray(json.result.content)) {
    return json.result.content
      .map((c: McpContentEntry) => c.text ?? JSON.stringify(c))
      .join('\n');
  }

  // Direct result string
  if (typeof json.result === 'string') {
    return json.result;
  }

  // Fallback: stringify the whole result
  return JSON.stringify(json.result, null, 2);
}
