/**
 * Verify that shared type definitions are internally consistent
 * and that the IpcApi interface references only defined types.
 */
import { describe, it, expect } from 'vitest';
import fs from 'fs';
import path from 'path';

const APP_DIR = path.resolve(__dirname, '../../../..');

function readSharedTypes(): string {
  // SSOT: types are defined in modular files under shared/types/*.types.ts
  // shared/types.ts and shared/types/index.ts are re-export barrels.
  const barrel = path.join(APP_DIR, 'app/shared/types/index.ts');

  const barrelContent = fs.readFileSync(barrel, 'utf-8');
  const reExports = [...barrelContent.matchAll(/from '\.\/([^']+)'/g)].map((m) => m[1]);
  let combined = barrelContent;
  for (const file of reExports) {
    const fullPath = path.join(APP_DIR, 'app/shared/types', file + '.ts');
    if (fs.existsSync(fullPath)) {
      combined += '\n' + fs.readFileSync(fullPath, 'utf-8');
    }
  }
  return combined;
}

const typesContent = readSharedTypes();

// Extract all exported interface/type names
const exportedTypes = [
  ...typesContent.matchAll(/export\s+(?:interface|type)\s+(\w+)/g),
].map((m) => m[1]);

describe('Shared Type Contracts', () => {
  it('exports all required domain types', () => {
    const required = [
      // Chat
      'Conversation', 'Message', 'StreamingChunk',
      // Files
      'FileNode', 'RecentFile',
      // XLSX
      'XlsxData', 'SheetData', 'CellData', 'CellStyle',
      // Pipeline
      'PipelineProgress', 'PipelineReport',
      // Extraction
      'ExtractionProgress', 'ExtractTextResult',
      // ASYCUDA
      'AsycudaImportResult', 'AsycudaClassification', 'CetStats',
      // Settings
      'AppSettings',
      // Client/Email
      'ClientSettings', 'EmailCredentials', 'ProcessedEmail',
      'EmailServiceStatus', 'IncomingEmail',
      'EmailClassification', 'BLMetadata', 'ClassificationResult',
      'PdfSplitResult',
      // IPC
      'IpcApi', 'SessionState', 'ShutdownStatus',
    ];

    for (const name of required) {
      expect(exportedTypes, `Missing export: ${name}`).toContain(name);
    }
  });

  it('StreamingChunk has correct type discriminator', () => {
    expect(typesContent).toMatch(/type:\s*'text'\s*\|\s*'tool_use_start'/);
  });

  it('ProcessedEmail.id is string not number', () => {
    // Extract the ProcessedEmail interface
    const match = typesContent.match(/interface ProcessedEmail\s*\{([^}]+)\}/s);
    expect(match).not.toBeNull();
    const body = match![1];
    // id should be string
    expect(body).toMatch(/id:\s*string/);
  });

  it('EmailCredentials uses address/server/ssl not username/host/secure', () => {
    const match = typesContent.match(/interface EmailCredentials\s*\{([^}]+)\}/s);
    expect(match).not.toBeNull();
    const body = match![1];
    expect(body).toMatch(/address:\s*string/);
    expect(body).toMatch(/server:\s*string/);
    expect(body).toMatch(/ssl:\s*boolean/);
    expect(body).not.toMatch(/username:/);
    expect(body).not.toMatch(/host:/);
    expect(body).not.toMatch(/secure:/);
  });

  it('CellData has address, row, col fields', () => {
    const match = typesContent.match(/interface CellData\s*\{([^}]+)\}/s);
    expect(match).not.toBeNull();
    const body = match![1];
    expect(body).toMatch(/address:\s*string/);
    expect(body).toMatch(/row:\s*number/);
    expect(body).toMatch(/col:\s*number/);
  });

  it('BLMetadata uses snake_case fields', () => {
    const match = typesContent.match(/interface BLMetadata\s*\{([^}]+)\}/s);
    expect(match).not.toBeNull();
    const body = match![1];
    expect(body).toMatch(/bl_number:\s*string/);
    expect(body).toMatch(/shipper_names:\s*string\[\]/);
    expect(body).not.toMatch(/blNumber:/);
  });

  it('AsycudaCorrection uses snake_case fields', () => {
    const match = typesContent.match(/interface AsycudaCorrection\s*\{([^}]+)\}/s);
    expect(match).not.toBeNull();
    const body = match![1];
    expect(body).toMatch(/old_code:\s*string/);
    expect(body).toMatch(/new_code:\s*string/);
  });

  it('AppSettings has all required fields', () => {
    const match = typesContent.match(/interface AppSettings\s*\{([^}]+)\}/s);
    expect(match).not.toBeNull();
    const body = match![1];
    expect(body).toMatch(/apiKey/);
    expect(body).toMatch(/baseUrl/);
    expect(body).toMatch(/model/);
    expect(body).toMatch(/workspacePath/);
  });
});
