/**
 * Chat store – SQLite-backed with in-memory fallback.
 *
 * Manages conversations, messages, recent files, search, and auto-tagging.
 * Uses WAL mode for concurrent read performance.
 */

import type { Conversation, Message, RecentFile } from '../../shared/types/index';
import { chatDbPath } from '../utils/paths';
import path from 'path';
import fs from 'fs';

// ---------------------------------------------------------------------------
// Internal state
// ---------------------------------------------------------------------------

let db: InstanceType<typeof import('better-sqlite3')> | null = null;

/** In-memory fallback stores when SQLite is unavailable. */
const memConversations: Conversation[] = [];
const memMessages: Message[] = [];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function generateId(): string {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 9);
}

function now(): string {
  return new Date().toISOString();
}

// ---------------------------------------------------------------------------
// Initialisation
// ---------------------------------------------------------------------------

export function initChatStore(): void {
  const dbFile = chatDbPath();

  try {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const Database = require('better-sqlite3');
    fs.mkdirSync(path.dirname(dbFile), { recursive: true });

    db = new Database(dbFile);
    db!.pragma('journal_mode = WAL');

    db!.exec(`
      CREATE TABLE IF NOT EXISTS conversations (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL DEFAULT 'New Conversation',
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        tags TEXT DEFAULT '[]',
        invoice_numbers TEXT DEFAULT '[]'
      );

      CREATE TABLE IF NOT EXISTS messages (
        id TEXT PRIMARY KEY,
        conversation_id TEXT NOT NULL,
        role TEXT NOT NULL CHECK(role IN ('user', 'assistant', 'system')),
        content TEXT NOT NULL,
        tool_use TEXT,
        tool_result TEXT,
        created_at TEXT NOT NULL,
        FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
      );

      CREATE TABLE IF NOT EXISTS recent_files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        path TEXT NOT NULL,
        name TEXT NOT NULL,
        opened_at TEXT NOT NULL,
        type TEXT NOT NULL
      );

      CREATE INDEX IF NOT EXISTS idx_messages_conversation ON messages(conversation_id);
      CREATE INDEX IF NOT EXISTS idx_conversations_updated ON conversations(updated_at);
      CREATE INDEX IF NOT EXISTS idx_recent_files_opened ON recent_files(opened_at);
    `);

    console.log('Chat store initialised (SQLite)');
  } catch (err) {
    console.warn(
      'Failed to initialise SQLite chat store – running without persistence:',
      (err as Error).message,
    );
    db = null;
  }
}

/** Expose the raw db handle for advanced callers (e.g. migrations). */
export function getChatDb() {
  return db;
}

// ---------------------------------------------------------------------------
// Conversations CRUD
// ---------------------------------------------------------------------------

export function createConversation(title?: string): Conversation {
  const id = generateId();
  const ts = now();
  const conv: Conversation = {
    id,
    title: title ?? 'New Conversation',
    createdAt: ts,
    updatedAt: ts,
    tags: [],
    invoiceNumbers: [],
  };

  if (db) {
    db.prepare(
      'INSERT INTO conversations (id, title, created_at, updated_at) VALUES (?, ?, ?, ?)',
    ).run(id, conv.title, ts, ts);
  } else {
    memConversations.unshift(conv);
  }

  return conv;
}

export function getConversations(): Conversation[] {
  if (!db) return memConversations;

  const rows = db
    .prepare('SELECT * FROM conversations ORDER BY updated_at DESC')
    .all() as any[];

  return rows.map((r) => ({
    id: r.id,
    title: r.title,
    createdAt: r.created_at,
    updatedAt: r.updated_at,
    tags: JSON.parse(r.tags || '[]'),
    invoiceNumbers: JSON.parse(r.invoice_numbers || '[]'),
  }));
}

export function deleteConversation(id: string): void {
  if (db) {
    db.prepare('DELETE FROM messages WHERE conversation_id = ?').run(id);
    db.prepare('DELETE FROM conversations WHERE id = ?').run(id);
  } else {
    const idx = memConversations.findIndex((c) => c.id === id);
    if (idx >= 0) memConversations.splice(idx, 1);
    // Walk backwards to safely splice while iterating
    for (let i = memMessages.length - 1; i >= 0; i--) {
      if (memMessages[i].conversationId === id) memMessages.splice(i, 1);
    }
  }
}

// ---------------------------------------------------------------------------
// Messages CRUD
// ---------------------------------------------------------------------------

export function getMessages(conversationId: string): Message[] {
  if (!db) return memMessages.filter((m) => m.conversationId === conversationId);

  const rows = db
    .prepare('SELECT * FROM messages WHERE conversation_id = ? ORDER BY created_at ASC')
    .all(conversationId) as any[];

  return rows.map((r) => ({
    id: r.id,
    conversationId: r.conversation_id,
    role: r.role,
    content: r.content,
    toolUse: r.tool_use ? JSON.parse(r.tool_use) : undefined,
    toolResult: r.tool_result ? JSON.parse(r.tool_result) : undefined,
    createdAt: r.created_at,
  }));
}

export function addMessage(
  conversationId: string,
  role: 'user' | 'assistant' | 'system',
  content: string,
  toolUse?: unknown,
  toolResult?: unknown,
): Message {
  const id = generateId();
  const ts = now();

  const msg: Message = {
    id,
    conversationId,
    role,
    content,
    toolUse: toolUse as Message['toolUse'],
    toolResult: toolResult as Message['toolResult'],
    createdAt: ts,
  };

  if (db) {
    db.prepare(
      'INSERT INTO messages (id, conversation_id, role, content, tool_use, tool_result, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)',
    ).run(
      id,
      conversationId,
      role,
      content,
      toolUse ? JSON.stringify(toolUse) : null,
      toolResult ? JSON.stringify(toolResult) : null,
      ts,
    );

    // Touch conversation timestamp
    db.prepare('UPDATE conversations SET updated_at = ? WHERE id = ?').run(ts, conversationId);

    if (role === 'user') {
      autoTitleConversation(conversationId, content);
      autoTagInvoiceNumbers(conversationId, content);
    }
  } else {
    memMessages.push(msg);
    const conv = memConversations.find((c) => c.id === conversationId);
    if (conv) {
      conv.updatedAt = ts;
      if (role === 'user' && conv.title === 'New Conversation') {
        conv.title = truncate(content, 60);
      }
    }
  }

  return msg;
}

// ---------------------------------------------------------------------------
// Auto-tagging helpers
// ---------------------------------------------------------------------------

const MAX_TITLE_LEN = 60;
const INVOICE_PATTERN = /\b(INV[-\s]?\d+|#\d{4,})\b/gi;

function truncate(text: string, maxLen: number): string {
  return text.length > maxLen ? text.slice(0, maxLen) + '...' : text;
}

function autoTitleConversation(conversationId: string, content: string): void {
  if (!db) return;
  const conv = db
    .prepare('SELECT title FROM conversations WHERE id = ?')
    .get(conversationId) as { title: string } | undefined;

  if (conv?.title === 'New Conversation') {
    const autoTitle = truncate(content, MAX_TITLE_LEN);
    db.prepare('UPDATE conversations SET title = ? WHERE id = ?').run(autoTitle, conversationId);
  }
}

function autoTagInvoiceNumbers(conversationId: string, content: string): void {
  if (!db) return;
  const matches = content.match(INVOICE_PATTERN);
  if (!matches) return;

  const row = db
    .prepare('SELECT invoice_numbers FROM conversations WHERE id = ?')
    .get(conversationId) as { invoice_numbers: string } | undefined;

  const existing: Set<string> = new Set(JSON.parse(row?.invoice_numbers || '[]'));
  matches.forEach((n) => existing.add(n));

  db.prepare('UPDATE conversations SET invoice_numbers = ? WHERE id = ?').run(
    JSON.stringify([...existing]),
    conversationId,
  );
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

export interface ChatSearchResult {
  conversations: {
    id: string;
    title: string;
    updatedAt: string;
    messageCount: number;
  }[];
  messages: {
    conversationId: string;
    conversationTitle: string;
    role: string;
    content: string;
    createdAt: string;
  }[];
}

export function searchChatHistory(
  query?: string,
  conversationId?: string,
  limit = 50,
): ChatSearchResult {
  if (!db) return { conversations: [], messages: [] };

  // Specific conversation requested
  if (conversationId) {
    return searchByConversation(conversationId, limit);
  }

  // Text search across all conversations
  if (query) {
    return searchByQuery(query, limit);
  }

  // Default: list recent conversations with counts
  return listRecentConversations(limit);
}

function searchByConversation(conversationId: string, limit: number): ChatSearchResult {
  const conv = db!
    .prepare('SELECT id, title, updated_at FROM conversations WHERE id = ?')
    .get(conversationId) as any;

  if (!conv) return { conversations: [], messages: [] };

  const msgs = db!
    .prepare(
      'SELECT role, content, created_at FROM messages WHERE conversation_id = ? ORDER BY created_at ASC LIMIT ?',
    )
    .all(conversationId, limit) as any[];

  return {
    conversations: [
      { id: conv.id, title: conv.title, updatedAt: conv.updated_at, messageCount: msgs.length },
    ],
    messages: msgs.map((m: any) => ({
      conversationId: conv.id,
      conversationTitle: conv.title,
      role: m.role,
      content: m.content,
      createdAt: m.created_at,
    })),
  };
}

function searchByQuery(query: string, limit: number): ChatSearchResult {
  const msgs = db!
    .prepare(
      `SELECT m.conversation_id, c.title, m.role, m.content, m.created_at
       FROM messages m JOIN conversations c ON m.conversation_id = c.id
       WHERE m.content LIKE ? AND m.role IN ('user', 'assistant')
       ORDER BY m.created_at DESC LIMIT ?`,
    )
    .all(`%${query}%`, limit) as any[];

  const convMap = new Map<
    string,
    { id: string; title: string; updatedAt: string; messageCount: number }
  >();

  for (const m of msgs) {
    if (!convMap.has(m.conversation_id)) {
      convMap.set(m.conversation_id, {
        id: m.conversation_id,
        title: m.title,
        updatedAt: m.created_at,
        messageCount: 0,
      });
    }
    convMap.get(m.conversation_id)!.messageCount++;
  }

  return {
    conversations: [...convMap.values()],
    messages: msgs.map((m: any) => ({
      conversationId: m.conversation_id,
      conversationTitle: m.title,
      role: m.role,
      content: truncate(m.content, 500),
      createdAt: m.created_at,
    })),
  };
}

function listRecentConversations(limit: number): ChatSearchResult {
  const rows = db!
    .prepare(
      `SELECT c.id, c.title, c.updated_at,
              (SELECT COUNT(*) FROM messages WHERE conversation_id = c.id) AS msg_count
       FROM conversations c ORDER BY c.updated_at DESC LIMIT ?`,
    )
    .all(limit) as any[];

  return {
    conversations: rows.map((c: any) => ({
      id: c.id,
      title: c.title,
      updatedAt: c.updated_at,
      messageCount: c.msg_count,
    })),
    messages: [],
  };
}

// ---------------------------------------------------------------------------
// Recent files
// ---------------------------------------------------------------------------

const MAX_RECENT_FILES = 20;

export function addRecentFile(filePath: string, name: string, type: string): void {
  if (!db) return;
  const ts = now();

  // Upsert: remove existing entry for this path, then insert
  db.prepare('DELETE FROM recent_files WHERE path = ?').run(filePath);
  db.prepare('INSERT INTO recent_files (path, name, opened_at, type) VALUES (?, ?, ?, ?)').run(
    filePath,
    name,
    ts,
    type,
  );

  // Prune old entries
  db.prepare(
    `DELETE FROM recent_files WHERE id NOT IN
     (SELECT id FROM recent_files ORDER BY opened_at DESC LIMIT ?)`,
  ).run(MAX_RECENT_FILES);
}

export function getRecentFiles(): RecentFile[] {
  if (!db) return [];
  const rows = db
    .prepare('SELECT path, name, opened_at, type FROM recent_files ORDER BY opened_at DESC LIMIT ?')
    .all(MAX_RECENT_FILES) as any[];

  return rows.map((r) => ({
    path: r.path,
    name: r.name,
    openedAt: r.opened_at,
    type: r.type,
  }));
}
