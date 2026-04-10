/**
 * Configuration loader – reads YAML and JSON config files from SSOT paths.
 */

import fs from 'fs';
import yaml from 'js-yaml';
import {
  columnsConfigPath,
  pipelineConfigPath,
  classificationRulesPath,
} from '../utils/paths';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface PipelineConfig {
  pipeline: { name: string; version: string; description: string };
  settings: Record<string, unknown>;
  stages: Record<string, unknown>[];
}

export interface ClassificationRules {
  rules: Record<string, unknown>[];
}

// ---------------------------------------------------------------------------
// Generic loader
// ---------------------------------------------------------------------------

/**
 * Load a config file as a parsed object. Supports `.yaml`, `.yml`, and `.json`.
 * Returns `null` if the file does not exist or cannot be parsed.
 */
export function loadConfigFile<T = unknown>(configPath: string): T | null {
  try {
    if (!fs.existsSync(configPath)) return null;
    const content = fs.readFileSync(configPath, 'utf-8');

    if (configPath.endsWith('.yaml') || configPath.endsWith('.yml')) {
      return yaml.load(content) as T;
    }

    return JSON.parse(content) as T;
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Domain-specific loaders
// ---------------------------------------------------------------------------

export function loadPipelineConfig(): PipelineConfig | null {
  return loadConfigFile<PipelineConfig>(pipelineConfigPath());
}

export function loadColumnsConfig(): Record<string, unknown> | null {
  return loadConfigFile<Record<string, unknown>>(columnsConfigPath());
}

export function loadClassificationRules(): ClassificationRules {
  return loadConfigFile<ClassificationRules>(classificationRulesPath()) ?? { rules: [] };
}
