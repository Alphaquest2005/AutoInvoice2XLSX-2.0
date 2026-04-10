/**
 * Client Settings Store
 * Manages email-processing client configurations and processed email tracking
 * using SQLite (better-sqlite3, WAL mode).
 *
 * Falls back to null if the native module fails to load.
 */

import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import { clientsDbPath } from '../utils/paths';
import type {
  ClientSettings,
  ProcessedEmail,
} from '../../shared/types';

// Local type alias extracted from ProcessedEmail['status']
type EmailProcessingStatus = ProcessedEmail['status'];

// ---------------------------------------------------------------------------
// Module state
// ---------------------------------------------------------------------------

let db: Database.Database | null = null;

const TAG = '[client-store]';

// ---------------------------------------------------------------------------
// ID generation
// ---------------------------------------------------------------------------

function generateId(): string {
  return Date.now().toString(36) + Math.random().toString(36).slice(2, 9);
}

// ---------------------------------------------------------------------------
// Initialisation & migrations
// ---------------------------------------------------------------------------

export function initClientStore(): void {
  const dbFile = clientsDbPath();
  console.log(TAG, 'Initializing client database at:', dbFile);

  try {
    fs.mkdirSync(path.dirname(dbFile), { recursive: true });
    db = new Database(dbFile);
    db.pragma('journal_mode = WAL');

    createSchema(db);
    runMigrations(db);

    console.log(TAG, 'Initialized with SQLite');
  } catch (err) {
    console.error(TAG, 'Failed to initialize:', (err as Error).message);
    db = null;
  }
}

function createSchema(database: Database.Database): void {
  database.exec(`
    CREATE TABLE IF NOT EXISTS clients (
      id                          TEXT PRIMARY KEY,
      name                        TEXT NOT NULL,
      enabled                     INTEGER DEFAULT 1,
      created_at                  TEXT NOT NULL,
      updated_at                  TEXT NOT NULL,
      -- IMAP (incoming) settings
      incoming_address            TEXT NOT NULL,
      incoming_server             TEXT NOT NULL,
      incoming_port               INTEGER DEFAULT 993,
      incoming_password           TEXT NOT NULL,
      incoming_ssl                INTEGER DEFAULT 1,
      -- SMTP (outgoing) settings
      outgoing_address            TEXT NOT NULL,
      outgoing_server             TEXT NOT NULL,
      outgoing_port               INTEGER DEFAULT 465,
      outgoing_password           TEXT NOT NULL,
      outgoing_ssl                INTEGER DEFAULT 1,
      -- Processing settings
      watch_folder                TEXT NOT NULL DEFAULT '',
      output_recipients           TEXT DEFAULT '[]',
      developer_email             TEXT DEFAULT '',
      auto_process                INTEGER DEFAULT 1,
      mark_as_read_after_processing INTEGER DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS processed_emails (
      id                INTEGER PRIMARY KEY AUTOINCREMENT,
      client_id         TEXT NOT NULL,
      message_id        TEXT NOT NULL,
      subject           TEXT,
      from_address      TEXT,
      received_at       TEXT,
      processed_at      TEXT,
      status            TEXT DEFAULT 'saving',
      input_dir         TEXT,
      output_files      TEXT DEFAULT '[]',
      waybill_number    TEXT,
      invoice_number    TEXT,
      email_sent        INTEGER DEFAULT 0,
      error             TEXT,
      retry_count       INTEGER DEFAULT 0,
      linked_record_id  TEXT,
      doc_types         TEXT,
      created_at        TEXT NOT NULL,
      updated_at        TEXT NOT NULL,
      FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_processed_client  ON processed_emails(client_id);
    CREATE INDEX IF NOT EXISTS idx_processed_status  ON processed_emails(status);
    CREATE INDEX IF NOT EXISTS idx_processed_message ON processed_emails(message_id);
  `);
}

function runMigrations(database: Database.Database): void {
  const peColumns = database.prepare("PRAGMA table_info(processed_emails)").all() as { name: string }[];
  const peColNames = new Set(peColumns.map(c => c.name));

  // Migration: add doc_types and linked_record_id for email linking
  if (!peColNames.has('doc_types')) {
    database.exec('ALTER TABLE processed_emails ADD COLUMN doc_types TEXT');
  }
  if (!peColNames.has('linked_record_id')) {
    database.exec('ALTER TABLE processed_emails ADD COLUMN linked_record_id TEXT');
    console.log(TAG, 'Added doc_types + linked_record_id columns for email linking');
  }

  // Migration: add retry_count + migrate old status values to state machine states
  if (!peColNames.has('retry_count')) {
    database.exec('ALTER TABLE processed_emails ADD COLUMN retry_count INTEGER DEFAULT 0');
    database.exec(`UPDATE processed_emails SET status = 'saving' WHERE status = 'processing' AND input_dir IS NULL`);
    database.exec(`UPDATE processed_emails SET status = 'pipeline_running' WHERE status = 'processing' AND input_dir IS NOT NULL`);
    database.exec(`UPDATE processed_emails SET status = 'needs_review' WHERE status = 'pending' AND error LIKE '%ariance%'`);
    database.exec(`UPDATE processed_emails SET status = 'files_ready' WHERE status = 'pending'`);
    database.exec(`UPDATE processed_emails SET status = 'completed' WHERE status = 'success'`);
    console.log(TAG, 'Migrated status values to state machine states');
  }

  // Migration: add processed_at if missing
  if (!peColNames.has('processed_at')) {
    database.exec('ALTER TABLE processed_emails ADD COLUMN processed_at TEXT');
  }

  // Migration: rename old columns if they exist (bl_number -> waybill_number, etc.)
  if (peColNames.has('bl_number') && !peColNames.has('waybill_number')) {
    database.exec('ALTER TABLE processed_emails ADD COLUMN waybill_number TEXT');
    database.exec('UPDATE processed_emails SET waybill_number = bl_number');
    console.log(TAG, 'Migrated bl_number -> waybill_number');
  }
  if (!peColNames.has('waybill_number') && !peColNames.has('bl_number')) {
    database.exec('ALTER TABLE processed_emails ADD COLUMN waybill_number TEXT');
  }
  if (peColNames.has('invoice_count') && !peColNames.has('invoice_number')) {
    database.exec('ALTER TABLE processed_emails ADD COLUMN invoice_number TEXT');
    console.log(TAG, 'Added invoice_number column');
  }
  if (!peColNames.has('invoice_number') && !peColNames.has('invoice_count')) {
    database.exec('ALTER TABLE processed_emails ADD COLUMN invoice_number TEXT');
  }

  // Migration: add email_sent / input_dir if missing (v1 may or may not have them)
  if (!peColNames.has('email_sent')) {
    database.exec('ALTER TABLE processed_emails ADD COLUMN email_sent INTEGER DEFAULT 0');
  }
  if (!peColNames.has('input_dir')) {
    database.exec('ALTER TABLE processed_emails ADD COLUMN input_dir TEXT');
  }

  // Migration: add created_at / updated_at if upgrading from v1 schema
  if (!peColNames.has('created_at')) {
    database.exec("ALTER TABLE processed_emails ADD COLUMN created_at TEXT NOT NULL DEFAULT ''");
    database.exec("UPDATE processed_emails SET created_at = COALESCE(processed_at, received_at, datetime('now'))");
    console.log(TAG, 'Added created_at column to processed_emails');
  }
  if (!peColNames.has('updated_at')) {
    database.exec("ALTER TABLE processed_emails ADD COLUMN updated_at TEXT NOT NULL DEFAULT ''");
    database.exec("UPDATE processed_emails SET updated_at = COALESCE(processed_at, received_at, datetime('now'))");
    console.log(TAG, 'Added updated_at column to processed_emails');
  }

  // Migration: convert id from TEXT PRIMARY KEY to INTEGER PRIMARY KEY AUTOINCREMENT
  // v1 used TEXT PRIMARY KEY with generated string IDs. v2 uses INTEGER AUTOINCREMENT.
  // Without this migration, INSERT without id value stores NULL (SQLite allows NULL in
  // non-INTEGER primary keys), and all subsequent WHERE id = ? lookups fail.
  const idColInfo = (database.prepare("PRAGMA table_info(processed_emails)").all() as {
    name: string; type: string;
  }[]).find(c => c.name === 'id');

  if (idColInfo && idColInfo.type !== 'INTEGER') {
    console.log(TAG, 'Migrating processed_emails id from TEXT to INTEGER AUTOINCREMENT...');
    database.exec(`
      CREATE TABLE processed_emails_v2 (
        id                INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id         TEXT NOT NULL,
        message_id        TEXT NOT NULL,
        subject           TEXT,
        from_address      TEXT,
        received_at       TEXT,
        processed_at      TEXT,
        status            TEXT DEFAULT 'saving',
        input_dir         TEXT,
        output_files      TEXT DEFAULT '[]',
        waybill_number    TEXT,
        invoice_number    TEXT,
        email_sent        INTEGER DEFAULT 0,
        error             TEXT,
        retry_count       INTEGER DEFAULT 0,
        linked_record_id  TEXT,
        doc_types         TEXT,
        created_at        TEXT NOT NULL DEFAULT '',
        updated_at        TEXT NOT NULL DEFAULT '',
        FOREIGN KEY (client_id) REFERENCES clients(id) ON DELETE CASCADE
      );

      INSERT INTO processed_emails_v2 (
        client_id, message_id, subject, from_address, received_at,
        processed_at, status, input_dir, output_files,
        waybill_number, invoice_number, email_sent, error,
        retry_count, linked_record_id, doc_types,
        created_at, updated_at
      )
      SELECT
        client_id, message_id, subject, from_address, received_at,
        processed_at, status, input_dir, output_files,
        waybill_number, invoice_number, email_sent, error,
        retry_count, linked_record_id, doc_types,
        created_at, updated_at
      FROM processed_emails;

      DROP TABLE processed_emails;
      ALTER TABLE processed_emails_v2 RENAME TO processed_emails;

      CREATE INDEX IF NOT EXISTS idx_processed_client  ON processed_emails(client_id);
      CREATE INDEX IF NOT EXISTS idx_processed_status  ON processed_emails(status);
      CREATE INDEX IF NOT EXISTS idx_processed_message ON processed_emails(message_id);
    `);
    console.log(TAG, 'Migrated processed_emails to INTEGER AUTOINCREMENT id');
  } else {
    // Clean up any NULL-id rows from prior broken inserts
    const nullRows = database.prepare("SELECT COUNT(*) as cnt FROM processed_emails WHERE id IS NULL").get() as { cnt: number };
    if (nullRows.cnt > 0) {
      database.exec("DELETE FROM processed_emails WHERE id IS NULL");
      console.log(TAG, `Cleaned up ${nullRows.cnt} processed_emails rows with NULL id`);
    }
  }

  // Client table migrations
  const cColumns = database.prepare("PRAGMA table_info(clients)").all() as { name: string }[];
  const cColNames = new Set(cColumns.map(c => c.name));

  if (!cColNames.has('output_recipients')) {
    database.exec("ALTER TABLE clients ADD COLUMN output_recipients TEXT DEFAULT '[]'");
  }
  if (!cColNames.has('developer_email')) {
    database.exec("ALTER TABLE clients ADD COLUMN developer_email TEXT DEFAULT ''");
  }
  if (!cColNames.has('mark_as_read_after_processing')) {
    database.exec('ALTER TABLE clients ADD COLUMN mark_as_read_after_processing INTEGER DEFAULT 1');
  }
}

// ---------------------------------------------------------------------------
// Accessor
// ---------------------------------------------------------------------------

export function getClientStore(): Database.Database | null {
  return db;
}

// ---------------------------------------------------------------------------
// Client CRUD
// ---------------------------------------------------------------------------

export function createClient(
  settings: Omit<ClientSettings, 'id' | 'createdAt' | 'updatedAt'>,
): ClientSettings {
  const id = generateId();
  const now = new Date().toISOString();

  const client: ClientSettings = {
    id,
    createdAt: now,
    updatedAt: now,
    ...settings,
  };

  if (db) {
    db.prepare(`
      INSERT INTO clients (
        id, name, enabled, created_at, updated_at,
        incoming_address, incoming_server, incoming_port, incoming_password, incoming_ssl,
        outgoing_address, outgoing_server, outgoing_port, outgoing_password, outgoing_ssl,
        watch_folder, output_recipients, developer_email,
        auto_process, mark_as_read_after_processing
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      id, client.name, client.enabled ? 1 : 0, now, now,
      client.incomingEmail.address, client.incomingEmail.server,
      client.incomingEmail.port, client.incomingEmail.password,
      client.incomingEmail.ssl ? 1 : 0,
      client.outgoingEmail.address, client.outgoingEmail.server,
      client.outgoingEmail.port, client.outgoingEmail.password,
      client.outgoingEmail.ssl ? 1 : 0,
      client.watchFolder ?? '',
      JSON.stringify(client.outputRecipients ?? []),
      client.developerEmail ?? '',
      client.autoProcess ? 1 : 0,
      client.markAsReadAfterProcessing ? 1 : 0,
    );
  }

  return client;
}

export function getClients(): ClientSettings[] {
  if (!db) return [];
  const rows = db.prepare('SELECT * FROM clients ORDER BY name ASC').all() as Record<string, unknown>[];
  return rows.map(rowToClient);
}

export function getClient(id: string): ClientSettings | null {
  if (!db) return null;
  const row = db.prepare('SELECT * FROM clients WHERE id = ?').get(id) as Record<string, unknown> | undefined;
  return row ? rowToClient(row) : null;
}

export function getClientByEmail(email: string): ClientSettings | null {
  if (!db) return null;
  const row = db.prepare('SELECT * FROM clients WHERE incoming_address = ?').get(email) as Record<string, unknown> | undefined;
  return row ? rowToClient(row) : null;
}

export function updateClient(
  id: string,
  updates: Partial<ClientSettings>,
): ClientSettings | null {
  if (!db) return null;

  const existing = getClient(id);
  if (!existing) return null;

  const now = new Date().toISOString();
  const updated: ClientSettings = { ...existing, ...updates, updatedAt: now };

  db.prepare(`
    UPDATE clients SET
      name = ?, enabled = ?, updated_at = ?,
      incoming_address = ?, incoming_server = ?, incoming_port = ?,
      incoming_password = ?, incoming_ssl = ?,
      outgoing_address = ?, outgoing_server = ?, outgoing_port = ?,
      outgoing_password = ?, outgoing_ssl = ?,
      watch_folder = ?, output_recipients = ?, developer_email = ?,
      auto_process = ?, mark_as_read_after_processing = ?
    WHERE id = ?
  `).run(
    updated.name, updated.enabled ? 1 : 0, now,
    updated.incomingEmail.address, updated.incomingEmail.server,
    updated.incomingEmail.port, updated.incomingEmail.password,
    updated.incomingEmail.ssl ? 1 : 0,
    updated.outgoingEmail.address, updated.outgoingEmail.server,
    updated.outgoingEmail.port, updated.outgoingEmail.password,
    updated.outgoingEmail.ssl ? 1 : 0,
    updated.watchFolder ?? '',
    JSON.stringify(updated.outputRecipients ?? []),
    updated.developerEmail ?? '',
    updated.autoProcess ? 1 : 0,
    updated.markAsReadAfterProcessing ? 1 : 0,
    id,
  );

  return updated;
}

export function deleteClient(id: string): boolean {
  if (!db) return false;
  const result = db.prepare('DELETE FROM clients WHERE id = ?').run(id);
  return result.changes > 0;
}

// ---------------------------------------------------------------------------
// Processed email tracking
// ---------------------------------------------------------------------------

export function recordProcessedEmail(
  email: Omit<ProcessedEmail, 'id'>,
): ProcessedEmail {
  const now = new Date().toISOString();

  if (!db) {
    return { id: '', ...email };
  }

  const result = db.prepare(`
    INSERT INTO processed_emails (
      client_id, message_id, subject, from_address, received_at,
      processed_at, status, input_dir, output_files,
      waybill_number, invoice_number, email_sent, error,
      retry_count, linked_record_id, doc_types,
      created_at, updated_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).run(
    email.clientId, email.messageId, email.subject, email.from,
    email.receivedAt, email.processedAt ?? null,
    email.status,
    email.inputDir ?? null,
    JSON.stringify(email.outputFiles ?? []),
    email.waybillNumber ?? null, email.invoiceNumber ?? null,
    email.emailSent ? 1 : 0, email.error ?? null,
    email.retryCount ?? 0, email.linkedRecordId ?? null,
    email.docTypes ? JSON.stringify(email.docTypes) : null,
    now, now,
  );

  return { id: String(result.lastInsertRowid), ...email };
}

export function updateProcessedEmail(
  id: string,
  updates: Partial<ProcessedEmail>,
): boolean {
  if (!db) return false;

  const setClauses: string[] = [];
  const values: unknown[] = [];

  const fieldMap: Record<string, { column: string; transform?: (v: unknown) => unknown }> = {
    status:         { column: 'status' },
    waybillNumber:  { column: 'waybill_number' },
    invoiceNumber:  { column: 'invoice_number' },
    outputFiles:    { column: 'output_files', transform: v => JSON.stringify(v) },
    error:          { column: 'error' },
    emailSent:      { column: 'email_sent', transform: v => (v ? 1 : 0) },
    inputDir:       { column: 'input_dir' },
    processedAt:    { column: 'processed_at' },
    docTypes:       { column: 'doc_types', transform: v => JSON.stringify(v) },
    retryCount:     { column: 'retry_count' },
    linkedRecordId: { column: 'linked_record_id' },
  };

  for (const [key, { column, transform }] of Object.entries(fieldMap)) {
    const value = (updates as Record<string, unknown>)[key];
    if (value !== undefined) {
      setClauses.push(`${column} = ?`);
      values.push(transform ? transform(value) : value);
    }
  }

  if (setClauses.length === 0) return false;

  setClauses.push('updated_at = ?');
  values.push(new Date().toISOString());
  values.push(id);

  const result = db.prepare(
    `UPDATE processed_emails SET ${setClauses.join(', ')} WHERE id = ?`,
  ).run(...values);
  return result.changes > 0;
}

export function getProcessedEmails(clientId?: string, limit = 100): ProcessedEmail[] {
  if (!db) return [];

  const query = clientId
    ? 'SELECT * FROM processed_emails WHERE client_id = ? ORDER BY created_at DESC LIMIT ?'
    : 'SELECT * FROM processed_emails ORDER BY created_at DESC LIMIT ?';

  const rows = clientId
    ? (db.prepare(query).all(clientId, limit) as Record<string, unknown>[])
    : (db.prepare(query).all(limit) as Record<string, unknown>[]);

  return rows.map(rowToProcessedEmail);
}

export function isEmailProcessed(messageId: string): boolean {
  if (!db) return false;
  const row = db.prepare('SELECT id FROM processed_emails WHERE message_id = ?').get(messageId);
  return !!row;
}

export function findProcessedEmailByInputDir(inputDir: string): ProcessedEmail | null {
  if (!db) return null;
  const row = db.prepare(
    'SELECT * FROM processed_emails WHERE input_dir = ? ORDER BY created_at DESC LIMIT 1',
  ).get(inputDir) as Record<string, unknown> | undefined;
  return row ? rowToProcessedEmail(row) : null;
}

export function findProcessedEmailByShipmentDir(shipmentDir: string): ProcessedEmail | null {
  if (!db) return null;
  // The shipment dir path appears in the output_files JSON array
  const row = db.prepare(
    "SELECT * FROM processed_emails WHERE output_files LIKE ? ORDER BY created_at DESC LIMIT 1",
  ).get(`%${shipmentDir.replace(/\\/g, '/')}%`) as Record<string, unknown> | undefined;
  if (row) return rowToProcessedEmail(row);
  // Also try with backslashes (Windows paths)
  const row2 = db.prepare(
    "SELECT * FROM processed_emails WHERE output_files LIKE ? ORDER BY created_at DESC LIMIT 1",
  ).get(`%${shipmentDir.replace(/\//g, '\\')}%`) as Record<string, unknown> | undefined;
  return row2 ? rowToProcessedEmail(row2) : null;
}

// ---------------------------------------------------------------------------
// State machine: resume helpers
// ---------------------------------------------------------------------------

const RESUMABLE_STATES: EmailProcessingStatus[] = [
  'saving', 'files_ready', 'pipeline_running', 'pipeline_done', 'email_sending',
];

export function getResumableEmails(): ProcessedEmail[] {
  if (!db) return [];

  const placeholders = RESUMABLE_STATES.map(() => '?').join(', ');
  const rows = db.prepare(`
    SELECT * FROM processed_emails
    WHERE status IN (${placeholders})
      AND retry_count < 3
    ORDER BY created_at ASC
  `).all(...RESUMABLE_STATES) as Record<string, unknown>[];

  return rows.map(rowToProcessedEmail);
}

export function incrementRetryCount(id: string): void {
  if (!db) return;
  db.prepare('UPDATE processed_emails SET retry_count = retry_count + 1 WHERE id = ?').run(id);
}

// ---------------------------------------------------------------------------
// Email linking
// ---------------------------------------------------------------------------

export function updateEmailDocTypes(id: string, docTypes: string[]): void {
  if (!db) return;
  db.prepare('UPDATE processed_emails SET doc_types = ? WHERE id = ?')
    .run(JSON.stringify(docTypes), id);
}

export function linkEmailRecords(id1: string, id2: string): void {
  if (!db) return;
  db.prepare('UPDATE processed_emails SET linked_record_id = ? WHERE id = ?').run(id2, id1);
  db.prepare('UPDATE processed_emails SET linked_record_id = ? WHERE id = ?').run(id1, id2);
}

/**
 * Find recent unlinked emails with specific doc types for a client.
 * Used to find invoice-only emails when a BL arrives (and vice versa).
 */
export function findRecentUnlinkedEmails(
  clientId: string,
  hasDocType: string,
  excludeDocType?: string,
  windowDays = 14,
): ProcessedEmail[] {
  if (!db) return [];

  const cutoff = new Date(Date.now() - windowDays * 86_400_000).toISOString();

  let sql = `
    SELECT * FROM processed_emails
    WHERE client_id = ?
      AND linked_record_id IS NULL
      AND received_at > ?
      AND (
        (doc_types IS NOT NULL AND doc_types LIKE ?)
        OR (doc_types IS NULL AND input_dir IS NOT NULL)
      )
  `;
  const params: unknown[] = [clientId, cutoff, `%"${hasDocType}"%`];

  if (excludeDocType) {
    sql += ' AND (doc_types IS NULL OR doc_types NOT LIKE ?)';
    params.push(`%"${excludeDocType}"%`);
  }

  sql += ' ORDER BY received_at DESC';

  const rows = db.prepare(sql).all(...params) as Record<string, unknown>[];
  return rows.map(rowToProcessedEmail);
}

// ---------------------------------------------------------------------------
// Row mappers
// ---------------------------------------------------------------------------

function rowToClient(r: Record<string, unknown>): ClientSettings {
  return {
    id: r.id as string,
    name: r.name as string,
    enabled: r.enabled === 1,
    createdAt: r.created_at as string,
    updatedAt: r.updated_at as string,
    incomingEmail: {
      address: r.incoming_address as string,
      server: r.incoming_server as string,
      port: r.incoming_port as number,
      password: r.incoming_password as string,
      ssl: r.incoming_ssl === 1,
    },
    outgoingEmail: {
      address: r.outgoing_address as string,
      server: r.outgoing_server as string,
      port: r.outgoing_port as number,
      password: r.outgoing_password as string,
      ssl: r.outgoing_ssl === 1,
    },
    watchFolder: (r.watch_folder as string) ?? '',
    outputRecipients: JSON.parse((r.output_recipients as string) || '[]'),
    developerEmail: (r.developer_email as string) ?? '',
    autoProcess: r.auto_process === 1,
    markAsReadAfterProcessing: r.mark_as_read_after_processing === 1,
  };
}

function rowToProcessedEmail(r: Record<string, unknown>): ProcessedEmail {
  return {
    id: String(r.id),
    clientId: r.client_id as string,
    messageId: r.message_id as string,
    subject: r.subject as string,
    from: r.from_address as string,
    receivedAt: r.received_at as string,
    processedAt: (r.processed_at as string) ?? '',
    status: r.status as EmailProcessingStatus,
    inputDir: (r.input_dir as string) || undefined,
    outputFiles: JSON.parse((r.output_files as string) || '[]'),
    waybillNumber: (r.waybill_number as string) || undefined,
    invoiceNumber: (r.invoice_number as string) || undefined,
    emailSent: r.email_sent === 1,
    error: (r.error as string) || undefined,
    retryCount: (r.retry_count as number) ?? 0,
    linkedRecordId: r.linked_record_id != null ? String(r.linked_record_id) : undefined,
    docTypes: r.doc_types ? JSON.parse(r.doc_types as string) : undefined,
  };
}
