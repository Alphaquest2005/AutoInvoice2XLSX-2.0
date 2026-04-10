/**
 * SSOT path resolution for the entire application.
 * All path computation goes through this module.
 */

import path from 'path';
import fs from 'fs';

let _baseDir: string = '';

export function initBaseDirs(baseDir: string): void {
  _baseDir = baseDir;
  // Ensure workspace structure exists
  const dirs = [
    workspacePath(),
    inputPath(),
    outputPath(),
    intermediatePath(),
    shipmentsPath(),
    documentsPath(),
    dataPath(),
  ];
  for (const dir of dirs) {
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
  }
}

export function baseDir(): string {
  if (!_baseDir) throw new Error('Base directory not initialized. Call initBaseDirs() first.');
  return _baseDir;
}

export function workspacePath(): string { return path.join(baseDir(), 'workspace'); }
export function inputPath(): string { return path.join(workspacePath(), 'input'); }
export function outputPath(): string { return path.join(workspacePath(), 'output'); }
export function intermediatePath(): string { return path.join(workspacePath(), 'intermediate'); }
export function shipmentsPath(): string { return path.join(workspacePath(), 'shipments'); }
export function documentsPath(): string { return path.join(workspacePath(), 'documents'); }
export function dataPath(): string { return path.join(baseDir(), 'data'); }
export function configPath(): string { return path.join(baseDir(), 'config'); }
export function rulesPath(): string { return path.join(baseDir(), 'rules'); }

export function settingsFilePath(): string { return path.join(dataPath(), 'settings.json'); }
export function chatDbPath(): string { return path.join(dataPath(), 'chat.db'); }
export function cetDbPath(): string { return path.join(dataPath(), 'cet.db'); }
export function clientsDbPath(): string { return path.join(dataPath(), 'clients.db'); }

export function columnsConfigPath(): string { return path.join(configPath(), 'columns.yaml'); }
export function pipelineConfigPath(): string { return path.join(configPath(), 'pipeline.yaml'); }
export function groupingConfigPath(): string { return path.join(configPath(), 'grouping.yaml'); }
export function classificationRulesPath(): string { return path.join(rulesPath(), 'classification_rules.json'); }
export function invalidCodesPath(): string { return path.join(rulesPath(), 'invalid_codes.json'); }

/** Locked system folders that cannot be deleted/renamed */
export const LOCKED_FOLDERS = [
  'input', 'output', 'intermediate', '_system',
  'UnProcessed', 'Downloads', 'documents', 'shipments', 'tests',
] as const;

export function isLockedFolder(folderName: string): boolean {
  return LOCKED_FOLDERS.includes(folderName as typeof LOCKED_FOLDERS[number]);
}

export function shipmentDir(blNumber: string): string {
  return path.join(shipmentsPath(), blNumber);
}

export function getExpectedOutputDir(inputDir: string): string {
  const shipmentName = path.basename(inputDir);
  return path.join(shipmentsPath(), shipmentName, 'output');
}
