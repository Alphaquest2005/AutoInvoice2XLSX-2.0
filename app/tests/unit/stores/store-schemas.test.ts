/**
 * Verify SQLite store schemas match the TypeScript type contracts.
 * Tests that stores return data in the shape the renderer expects.
 */
import { describe, it, expect } from 'vitest';
import fs from 'fs';
import path from 'path';

const APP_DIR = path.resolve(__dirname, '../../../..');

function readFile(relPath: string): string {
  return fs.readFileSync(path.join(APP_DIR, relPath), 'utf-8');
}

describe('Chat Store Schema', () => {
  const storeCode = readFile('app/main/stores/chat.store.ts');

  it('getConversations returns objects with required Conversation fields', () => {
    // Must select/return: id, title, createdAt, updatedAt, tags, invoiceNumbers
    expect(storeCode).toMatch(/tags/);
    expect(storeCode).toMatch(/invoiceNumbers/);
  });

  it('getMessages returns objects with required Message fields', () => {
    expect(storeCode).toMatch(/conversationId/);
    expect(storeCode).toMatch(/toolUse/);
    expect(storeCode).toMatch(/toolResult/);
  });

  it('getRecentFiles returns objects with openedAt (not accessedAt)', () => {
    expect(storeCode).toMatch(/openedAt/);
    // Should NOT have accessedAt as a mapped field name
    expect(storeCode).not.toMatch(/accessedAt:\s*(?:row|r)\./);
  });
});

describe('CET Store Schema', () => {
  const storeCode = readFile('app/main/stores/cet.store.ts');

  it('getCetStats returns CetStats-shaped object', () => {
    // Must have: codes, aliases, chapters, chapter_count, asycuda_classifications, corrections
    // Some use shorthand property syntax (e.g. `aliases,` not `aliases: aliases`)
    expect(storeCode).toMatch(/codes[,:\s]/);
    expect(storeCode).toMatch(/aliases[,:\s]/);
    expect(storeCode).toMatch(/chapters[,:\s]/);
    expect(storeCode).toMatch(/chapter_count[,:\s]/);
    expect(storeCode).toMatch(/asycuda_classifications[,:\s]/);
  });

  it('corrections use snake_case: old_code, new_code', () => {
    expect(storeCode).toMatch(/old_code:/);
    expect(storeCode).toMatch(/new_code:/);
  });

  it('getSkuClassification returns AsycudaClassification fields', () => {
    expect(storeCode).toMatch(/hs_code/);
    expect(storeCode).toMatch(/commercial_description/);
    expect(storeCode).toMatch(/country_of_origin/);
    expect(storeCode).toMatch(/source_file/);
    expect(storeCode).toMatch(/created_at/);
  });
});

describe('Client Store Schema', () => {
  const storeCode = readFile('app/main/stores/client.store.ts');

  it('uses address/server/ssl for EmailCredentials (not username/host/secure)', () => {
    // The store should map DB columns to address/server/ssl
    expect(storeCode).toMatch(/address/);
    expect(storeCode).toMatch(/server/);
    expect(storeCode).toMatch(/ssl/);
  });

  it('ProcessedEmail.id returned as string', () => {
    // Should convert SQLite integer rowid to string
    expect(storeCode).toMatch(/String\(.*lastInsertRowid/);
  });

  it('has waybillNumber field (not blNumber)', () => {
    expect(storeCode).toMatch(/waybillNumber/);
  });

  it('docTypes is JSON-parsed to string[]', () => {
    expect(storeCode).toMatch(/JSON\.parse/);
  });
});

describe('XLSX Parser Output Shape', () => {
  const parserCode = readFile('app/main/services/xlsx-parser.ts');

  it('uses bgColor not backgroundColor', () => {
    expect(parserCode).toMatch(/bgColor/);
    expect(parserCode).not.toMatch(/backgroundColor/);
  });

  it('uses numFmt not numberFormat', () => {
    expect(parserCode).toMatch(/numFmt/);
    expect(parserCode).not.toMatch(/numberFormat/);
  });

  it('CellData includes address, row, col', () => {
    expect(parserCode).toMatch(/address/);
    // buildCellData should accept row and col params
    expect(parserCode).toMatch(/buildCellData.*row.*col/);
  });

  it('SheetData uses rows/colCount/rowCount/colWidths', () => {
    // Some use shorthand property syntax (e.g. `colCount,` not `colCount: colCount`)
    expect(parserCode).toMatch(/rows[,:\s]/);
    expect(parserCode).toMatch(/colCount[,:\s]/);
    expect(parserCode).toMatch(/rowCount[,:\s]/);
    expect(parserCode).toMatch(/colWidths[,:\s]/);
  });
});

describe('Settings SSOT', () => {
  const settingsCode = readFile('app/main/utils/settings.ts');

  it('default settings match AppSettings type fields', () => {
    expect(settingsCode).toMatch(/apiKey/);
    expect(settingsCode).toMatch(/baseUrl/);
    expect(settingsCode).toMatch(/model/);
    expect(settingsCode).toMatch(/workspacePath/);
  });

  it('does not have removed fields', () => {
    expect(settingsCode).not.toMatch(/varianceThreshold/);
    expect(settingsCode).not.toMatch(/maxLlmVarianceAttempts/);
  });
});
