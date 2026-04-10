/**
 * SSOT settings management.
 * Single place for loading, saving, and defaulting application settings.
 */

import fs from 'fs';
import { settingsFilePath, dataPath } from './paths';
import type { AppSettings } from '../../shared/types';

/**
 * Defaults MUST match AppSettings SSOT in src/autoinvoice/domain/models/settings.py
 * and PipelineConfig in pipeline/core/config.py.
 */
const DEFAULT_SETTINGS: AppSettings = {
  apiKey: process.env.ZAI_API_KEY || '',
  baseUrl: 'https://api.z.ai/api/anthropic',
  model: process.env.ZAI_MODEL || 'glm-5',  // SSOT: src/autoinvoice/domain/models/settings.py
  workspacePath: '',
  theme: 'dark',
  enabledStages: ['extract', 'parse', 'classify', 'validate_codes', 'group', 'generate_xlsx', 'verify', 'learn'],
};

export function loadSettings(): AppSettings {
  try {
    const filePath = settingsFilePath();
    if (!fs.existsSync(filePath)) {
      return { ...DEFAULT_SETTINGS };
    }
    const raw = fs.readFileSync(filePath, 'utf-8');
    const parsed = JSON.parse(raw);
    return { ...DEFAULT_SETTINGS, ...parsed };
  } catch {
    return { ...DEFAULT_SETTINGS };
  }
}

export function saveSettings(updates: Partial<AppSettings>): void {
  const current = loadSettings();
  const merged = { ...current, ...updates };
  const dir = dataPath();
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
  fs.writeFileSync(settingsFilePath(), JSON.stringify(merged, null, 2), 'utf-8');
}

export function getApiKey(): string {
  return loadSettings().apiKey;
}

export function setApiKey(key: string): void {
  saveSettings({ apiKey: key });
}
