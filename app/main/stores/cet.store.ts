/**
 * CARICOM CET (Common External Tariff) SQLite store.
 * Stores HS codes, descriptions, duty rates, aliases, ASYCUDA classifications,
 * and classification corrections for fast tariff lookups.
 *
 * Uses better-sqlite3 with WAL mode; falls back to null (in-memory disabled)
 * if the native module fails to load.
 */

import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import { cetDbPath } from '../utils/paths';
import type {
  CetStats,
  AsycudaClassification,
  AsycudaCorrection,
} from '../../shared/types';

// ---------------------------------------------------------------------------
// Module state
// ---------------------------------------------------------------------------

let db: Database.Database | null = null;

const TAG = '[cet-store]';

// ---------------------------------------------------------------------------
// Initialisation & migrations
// ---------------------------------------------------------------------------

export function initCetStore(): void {
  const dbFile = cetDbPath();
  console.log(TAG, 'Initializing CET database at:', dbFile);

  try {
    fs.mkdirSync(path.dirname(dbFile), { recursive: true });
    db = new Database(dbFile);
    db.pragma('journal_mode = WAL');

    runMigrations(db);
    createSchema(db);

    console.log(TAG, 'CET database initialized successfully');
  } catch (err) {
    console.error(TAG, 'FAILED to initialize CET database!');
    console.error(TAG, 'Error:', (err as Error).message);
    db = null;
  }
}

function runMigrations(database: Database.Database): void {
  const cetCodesInfo = database.prepare("PRAGMA table_info(cet_codes)").all() as { name: string }[];
  const hasEnabledColumn = cetCodesInfo.some(col => col.name === 'enabled');

  if (!hasEnabledColumn && cetCodesInfo.length > 0) {
    console.log(TAG, 'Migrating: adding enabled column to existing tables');
    try {
      database.exec('ALTER TABLE cet_codes ADD COLUMN enabled INTEGER DEFAULT 1');
      database.exec('ALTER TABLE cet_aliases ADD COLUMN enabled INTEGER DEFAULT 1');
      console.log(TAG, 'Migration complete');
    } catch (migErr) {
      console.log(TAG, 'Migration columns may already exist:', (migErr as Error).message);
    }
  }
}

function createSchema(database: Database.Database): void {
  database.exec(`
    CREATE TABLE IF NOT EXISTS cet_codes (
      hs_code     TEXT PRIMARY KEY,
      description TEXT NOT NULL,
      duty_rate   TEXT,
      chapter     INTEGER,
      heading     TEXT,
      section     TEXT,
      unit        TEXT,
      notes       TEXT,
      source      TEXT DEFAULT 'rules',
      enabled     INTEGER DEFAULT 1,
      updated_at  TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS cet_aliases (
      id      INTEGER PRIMARY KEY AUTOINCREMENT,
      hs_code TEXT NOT NULL,
      alias   TEXT NOT NULL,
      source  TEXT DEFAULT 'rules',
      enabled INTEGER DEFAULT 1,
      FOREIGN KEY (hs_code) REFERENCES cet_codes(hs_code)
    );

    CREATE TABLE IF NOT EXISTS asycuda_imports (
      id                  INTEGER PRIMARY KEY AUTOINCREMENT,
      file_path           TEXT NOT NULL,
      declaration_type    TEXT,
      registration_number TEXT,
      registration_date   TEXT,
      items_count         INTEGER,
      imported_at         TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS asycuda_classifications (
      id                     INTEGER PRIMARY KEY AUTOINCREMENT,
      sku                    TEXT NOT NULL,
      hs_code                TEXT NOT NULL,
      description            TEXT,
      commercial_description TEXT,
      country_of_origin      TEXT,
      import_id              INTEGER,
      source_file            TEXT,
      created_at             TEXT NOT NULL,
      FOREIGN KEY (import_id) REFERENCES asycuda_imports(id)
    );

    CREATE TABLE IF NOT EXISTS classification_corrections (
      id                INTEGER PRIMARY KEY AUTOINCREMENT,
      sku               TEXT NOT NULL,
      old_hs_code       TEXT NOT NULL,
      new_hs_code       TEXT NOT NULL,
      reason            TEXT,
      asycuda_import_id INTEGER,
      corrected_at      TEXT NOT NULL,
      FOREIGN KEY (asycuda_import_id) REFERENCES asycuda_imports(id)
    );

    CREATE INDEX IF NOT EXISTS idx_cet_chapter      ON cet_codes(chapter);
    CREATE INDEX IF NOT EXISTS idx_cet_heading       ON cet_codes(heading);
    CREATE INDEX IF NOT EXISTS idx_cet_enabled       ON cet_codes(enabled);
    CREATE INDEX IF NOT EXISTS idx_aliases_code      ON cet_aliases(hs_code);
    CREATE INDEX IF NOT EXISTS idx_aliases_text      ON cet_aliases(alias);
    CREATE INDEX IF NOT EXISTS idx_asycuda_sku       ON asycuda_classifications(sku);
    CREATE INDEX IF NOT EXISTS idx_asycuda_hs        ON asycuda_classifications(hs_code);
    CREATE INDEX IF NOT EXISTS idx_corrections_sku   ON classification_corrections(sku);
  `);
}

// ---------------------------------------------------------------------------
// Accessor
// ---------------------------------------------------------------------------

export function getCetDb(): Database.Database | null {
  return db;
}

// ---------------------------------------------------------------------------
// Seed from rules JSON (first-launch only)
// ---------------------------------------------------------------------------

export function seedFromRules(rulesPath: string, invalidCodesPath: string): void {
  if (!db) return;

  const count = db.prepare('SELECT COUNT(*) as c FROM cet_codes').get() as { c: number };
  if (count.c > 0) {
    console.log(TAG, `Database already has ${count.c} codes, skipping seed`);
    return;
  }

  const now = new Date().toISOString();

  const insertCode = db.prepare(`
    INSERT OR IGNORE INTO cet_codes
      (hs_code, description, duty_rate, chapter, heading, notes, source, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, 'rules', ?)
  `);

  const insertAlias = db.prepare(
    "INSERT INTO cet_aliases (hs_code, alias, source) VALUES (?, ?, 'rules')"
  );

  const transaction = db.transaction(() => {
    seedClassificationRules(rulesPath, insertCode, insertAlias, now);
    seedInvalidCodes(invalidCodesPath, insertCode, now);
  });

  transaction();

  const finalCount = db.prepare('SELECT COUNT(*) as c FROM cet_codes').get() as { c: number };
  const aliasCount = db.prepare('SELECT COUNT(*) as c FROM cet_aliases').get() as { c: number };
  console.log(TAG, `Seeded ${finalCount.c} codes with ${aliasCount.c} aliases`);
}

function seedClassificationRules(
  rulesFile: string,
  insertCode: Database.Statement,
  insertAlias: Database.Statement,
  now: string,
): void {
  try {
    const rulesData = JSON.parse(fs.readFileSync(rulesFile, 'utf-8'));
    const rules = rulesData.rules || rulesData;
    if (!Array.isArray(rules)) return;

    for (const rule of rules) {
      const code: string | undefined = rule.code;
      if (!code || code === 'UNKNOWN') continue;

      const chapter = parseInt(code.substring(0, 2), 10);
      const heading = code.substring(0, 4);

      insertCode.run(
        code,
        rule.category || rule.description || 'Unknown',
        null,
        chapter,
        heading,
        rule.notes || null,
        now,
      );

      for (const pattern of rule.patterns || []) {
        insertAlias.run(code, (pattern as string).toUpperCase());
      }
      if (rule.category) {
        insertAlias.run(code, (rule.category as string).toUpperCase());
      }
    }
  } catch (err) {
    console.warn(TAG, 'Could not read classification rules:', (err as Error).message);
  }
}

function seedInvalidCodes(
  invalidCodesFile: string,
  insertCode: Database.Statement,
  now: string,
): void {
  try {
    const invalidData = JSON.parse(fs.readFileSync(invalidCodesFile, 'utf-8'));

    for (const [invalidCode, mapping] of Object.entries(invalidData)) {
      if (invalidCode === '_comment') continue;
      const m = mapping as { correct_code?: string; reason?: string };
      if (!m.correct_code) continue;

      const chapter = parseInt(m.correct_code.substring(0, 2), 10);
      const heading = m.correct_code.substring(0, 4);

      insertCode.run(
        m.correct_code,
        m.reason || 'Corrected code',
        null,
        chapter,
        heading,
        `Auto-fix from ${invalidCode}: ${m.reason || ''}`,
        now,
      );
    }
  } catch (err) {
    console.warn(TAG, 'Could not read invalid codes:', (err as Error).message);
  }
}

// ---------------------------------------------------------------------------
// Tariff lookup (fuzzy description + exact code + chapter filter)
// ---------------------------------------------------------------------------

export interface TariffLookupResult {
  found?: boolean;
  result?: Record<string, unknown>;
  count?: number;
  results?: Record<string, unknown>[];
  message?: string;
  error?: string;
}

export function lookupTariff(
  query: string,
  options?: { code?: string; chapter?: number },
): TariffLookupResult {
  if (!db) return { error: 'CET database not initialized' };

  // Exact code lookup
  if (options?.code) {
    const row = db.prepare(`
      SELECT c.*, GROUP_CONCAT(a.alias, ', ') as aliases
      FROM cet_codes c
      LEFT JOIN cet_aliases a ON c.hs_code = a.hs_code
      WHERE c.hs_code = ?
      GROUP BY c.hs_code
    `).get(options.code) as Record<string, unknown> | undefined;

    return row
      ? { found: true, result: row }
      : { found: false, message: `No CET entry for code ${options.code}` };
  }

  // Fuzzy description / alias search
  if (!query || query.trim().length === 0) {
    return { error: 'Provide a query or code to search' };
  }

  const tokens = query.toUpperCase().split(/\s+/).filter(t => t.length > 2);
  if (tokens.length === 0) {
    return { count: 0, results: [], message: 'Query too short for meaningful search' };
  }

  const conditions = tokens.map(
    () => '(c.description LIKE ? COLLATE NOCASE OR a.alias LIKE ? COLLATE NOCASE)',
  );
  const params: string[] = [];
  for (const token of tokens) {
    params.push(`%${token}%`, `%${token}%`);
  }

  let sql = `
    SELECT c.hs_code, c.description, c.duty_rate, c.chapter, c.heading,
           c.unit, c.notes, GROUP_CONCAT(DISTINCT a.alias) as aliases
    FROM cet_codes c
    LEFT JOIN cet_aliases a ON c.hs_code = a.hs_code
    WHERE (${conditions.join(' OR ')})
  `;

  if (options?.chapter) {
    sql += ' AND c.chapter = ?';
    params.push(String(options.chapter));
  }

  sql += ' GROUP BY c.hs_code ORDER BY c.hs_code LIMIT 15';

  const rows = db.prepare(sql).all(...params) as Record<string, unknown>[];
  return { count: rows.length, results: rows };
}

// ---------------------------------------------------------------------------
// Add / update a CET entry
// ---------------------------------------------------------------------------

export interface CetEntryInput {
  hs_code: string;
  description: string;
  duty_rate?: string;
  unit?: string;
  notes?: string;
  source?: string;
  aliases?: string[];
}

export function addCetEntry(entry: CetEntryInput): { success: boolean; hs_code: string } | { error: string } {
  if (!db) return { error: 'CET database not initialized' };

  const now = new Date().toISOString();
  const chapter = parseInt(entry.hs_code.substring(0, 2), 10);
  const heading = entry.hs_code.substring(0, 4);
  const source = entry.source || 'manual';

  db.prepare(`
    INSERT INTO cet_codes (hs_code, description, duty_rate, chapter, heading, unit, notes, source, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(hs_code) DO UPDATE SET
      description = COALESCE(excluded.description, description),
      duty_rate   = COALESCE(excluded.duty_rate, duty_rate),
      unit        = COALESCE(excluded.unit, unit),
      notes       = COALESCE(excluded.notes, notes),
      source      = excluded.source,
      updated_at  = excluded.updated_at
  `).run(
    entry.hs_code, entry.description, entry.duty_rate ?? null,
    chapter, heading, entry.unit ?? null, entry.notes ?? null,
    source, now,
  );

  if (entry.aliases?.length) {
    const insertAlias = db.prepare(
      'INSERT INTO cet_aliases (hs_code, alias, source) VALUES (?, ?, ?)',
    );
    for (const alias of entry.aliases) {
      insertAlias.run(entry.hs_code, alias.toUpperCase(), source);
    }
  }

  return { success: true, hs_code: entry.hs_code };
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

export function getCetStats(): CetStats | { error: string } {
  if (!db) return { error: 'CET database not initialized' };

  const codes = (db.prepare('SELECT COUNT(*) as c FROM cet_codes').get() as { c: number }).c;
  const aliases = (db.prepare('SELECT COUNT(*) as c FROM cet_aliases').get() as { c: number }).c;
  const asycuda_classifications = (db.prepare('SELECT COUNT(*) as c FROM asycuda_classifications').get() as { c: number }).c;
  const corrections = (db.prepare('SELECT COUNT(*) as c FROM classification_corrections').get() as { c: number }).c;

  const chapterRows = db.prepare('SELECT DISTINCT chapter FROM cet_codes ORDER BY chapter').all() as { chapter: number }[];
  const chapters = chapterRows.map(r => r.chapter);
  const chapter_count = chapters.length;

  return {
    codes,
    aliases,
    chapters,
    chapter_count,
    asycuda_classifications,
    corrections,
  };
}

// ---------------------------------------------------------------------------
// ASYCUDA XML import
// ---------------------------------------------------------------------------

export interface AsycudaImportData {
  file_path: string;
  declaration_type?: string;
  registration_number?: string;
  registration_date?: string;
  items: Array<{
    sku_reference?: string;
    commodity_code: string;
    precision?: string;
    description?: string;
    commercial_description?: string;
    country_of_origin?: string;
  }>;
}

export interface AsycudaImportStoreResult {
  importId: number | bigint;
  imported: number;
  skipped: number;
  corrected: number;
  corrections: AsycudaCorrection[];
}

export function importAsycudaClassifications(
  data: AsycudaImportData,
): AsycudaImportStoreResult | { error: string } {
  if (!db) return { error: 'CET database not initialized' };

  const now = new Date().toISOString();
  const corrections: AsycudaCorrection[] = [];
  let imported = 0;
  let skipped = 0;
  let corrected = 0;

  const transaction = db.transaction(() => {
    // Record the import
    const importResult = db!.prepare(`
      INSERT INTO asycuda_imports
        (file_path, declaration_type, registration_number, registration_date, items_count, imported_at)
      VALUES (?, ?, ?, ?, ?, ?)
    `).run(
      data.file_path,
      data.declaration_type ?? null,
      data.registration_number ?? null,
      data.registration_date ?? null,
      data.items.length,
      now,
    );
    const importId = importResult.lastInsertRowid;

    const insertClassification = db!.prepare(`
      INSERT INTO asycuda_classifications
        (sku, hs_code, description, commercial_description, country_of_origin, import_id, source_file, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);
    const findExisting = db!.prepare(
      'SELECT hs_code FROM asycuda_classifications WHERE sku = ? ORDER BY created_at DESC LIMIT 1',
    );
    const insertCorrection = db!.prepare(`
      INSERT INTO classification_corrections
        (sku, old_hs_code, new_hs_code, reason, asycuda_import_id, corrected_at)
      VALUES (?, ?, ?, ?, ?, ?)
    `);
    const disableAlias = db!.prepare(
      'UPDATE cet_aliases SET enabled = 0 WHERE alias LIKE ? AND hs_code = ?',
    );

    for (const item of data.items) {
      if (!item.sku_reference?.trim()) {
        skipped++;
        continue;
      }

      const sku = item.sku_reference.trim().toUpperCase();
      const fullHsCode = item.precision
        ? `${item.commodity_code}${item.precision}`
        : item.commodity_code;

      // Detect classification change => record correction
      const existing = findExisting.get(sku) as { hs_code: string } | undefined;
      if (existing && existing.hs_code !== fullHsCode) {
        insertCorrection.run(
          sku, existing.hs_code, fullHsCode,
          `ASYCUDA import from ${data.file_path}`, importId, now,
        );
        disableAlias.run(`%${sku}%`, existing.hs_code);
        corrections.push({ sku, old_code: existing.hs_code, new_code: fullHsCode });
        corrected++;
      }

      insertClassification.run(
        sku, fullHsCode,
        item.description ?? null,
        item.commercial_description ?? null,
        item.country_of_origin ?? null,
        importId, data.file_path, now,
      );

      // Ensure HS code exists in cet_codes
      const chapter = parseInt(item.commodity_code.substring(0, 2), 10);
      const heading = item.commodity_code.substring(0, 4);
      db!.prepare(`
        INSERT INTO cet_codes (hs_code, description, chapter, heading, source, enabled, updated_at)
        VALUES (?, ?, ?, ?, 'asycuda', 1, ?)
        ON CONFLICT(hs_code) DO UPDATE SET
          updated_at = excluded.updated_at,
          source = CASE WHEN source = 'rules' THEN 'asycuda' ELSE source END
      `).run(fullHsCode, item.description || 'ASYCUDA Import', chapter, heading, now);

      imported++;
    }

    return { importId, imported, skipped, corrected, corrections };
  });

  return transaction();
}

// ---------------------------------------------------------------------------
// SKU classification lookup (ASYCUDA-first, then aliases)
// ---------------------------------------------------------------------------

export function getSkuClassification(sku: string): AsycudaClassification {
  const empty: AsycudaClassification = { found: false };
  if (!db) return empty;

  const skuUpper = sku.toUpperCase();

  // Prefer ASYCUDA (most authoritative)
  const asycuda = db.prepare(`
    SELECT hs_code, description, commercial_description, country_of_origin, source_file, created_at
    FROM asycuda_classifications
    WHERE sku = ?
    ORDER BY created_at DESC LIMIT 1
  `).get(skuUpper) as {
    hs_code: string; description: string; commercial_description: string;
    country_of_origin: string; source_file: string; created_at: string;
  } | undefined;

  if (asycuda) {
    return {
      found: true,
      source: 'asycuda',
      hs_code: asycuda.hs_code,
      description: asycuda.description,
      commercial_description: asycuda.commercial_description,
      country_of_origin: asycuda.country_of_origin,
      source_file: asycuda.source_file,
      created_at: asycuda.created_at,
    };
  }

  // Fall back to alias lookup
  const alias = db.prepare(`
    SELECT a.hs_code, c.description
    FROM cet_aliases a
    JOIN cet_codes c ON a.hs_code = c.hs_code
    WHERE a.alias LIKE ? AND a.enabled = 1 AND c.enabled = 1
    ORDER BY a.id DESC LIMIT 1
  `).get(`%${skuUpper}%`) as { hs_code: string; description: string } | undefined;

  if (alias) {
    return {
      found: true,
      source: 'rules',
      hs_code: alias.hs_code,
      description: alias.description,
    };
  }

  return empty;
}

// ---------------------------------------------------------------------------
// Corrections history for a SKU
// ---------------------------------------------------------------------------

export function getSkuCorrections(sku: string): { sku: string; corrections: AsycudaCorrection[] } {
  if (!db) return { sku, corrections: [] };

  const rows = db.prepare(`
    SELECT old_hs_code, new_hs_code, reason, corrected_at
    FROM classification_corrections
    WHERE sku = ?
    ORDER BY corrected_at DESC
  `).all(sku.toUpperCase()) as { old_hs_code: string; new_hs_code: string; reason: string; corrected_at: string }[];

  return {
    sku,
    corrections: rows.map(r => ({
      sku: sku.toUpperCase(),
      old_code: r.old_hs_code,
      new_code: r.new_hs_code,
    })),
  };
}

// ---------------------------------------------------------------------------
// ASYCUDA imports list
// ---------------------------------------------------------------------------

export function getAsycudaImports(): { imports: AsycudaImportRow[] } {
  if (!db) return { imports: [] };

  const imports = db.prepare(`
    SELECT id, file_path, declaration_type, registration_number,
           registration_date, items_count, imported_at
    FROM asycuda_imports
    ORDER BY imported_at DESC LIMIT 100
  `).all() as AsycudaImportRow[];

  return { imports };
}

interface AsycudaImportRow {
  id: number;
  file_path: string;
  declaration_type: string | null;
  registration_number: string | null;
  registration_date: string | null;
  items_count: number;
  imported_at: string;
}

// ---------------------------------------------------------------------------
// Disable a classification
// ---------------------------------------------------------------------------

export function disableClassification(
  hsCode: string,
  reason?: string,
): { success: boolean; changes: number } | { error: string } {
  if (!db) return { error: 'CET database not initialized' };

  const result = db.prepare(`
    UPDATE cet_codes
    SET enabled = 0,
        notes = COALESCE(notes || ' | ', '') || ?
    WHERE hs_code = ?
  `).run(`Disabled: ${reason || 'No reason provided'}`, hsCode);

  db.prepare('UPDATE cet_aliases SET enabled = 0 WHERE hs_code = ?').run(hsCode);

  return { success: true, changes: result.changes };
}
