/**
 * Central IPC handler registration with dependency injection.
 * Replaces the 2429-line ipc-handlers.ts monolith.
 */

import type { BrowserWindow } from 'electron';
import type { AppSettings } from '../../shared/types';

import { registerChatHandlers } from './chat.handlers';
import { registerFileHandlers } from './file.handlers';
import { registerXlsxHandlers } from './xlsx.handlers';
import { registerPipelineHandlers } from './pipeline.handlers';
import { registerExtractionHandlers } from './extraction.handlers';
import { registerAsycudaHandlers } from './asycuda.handlers';
import { registerSettingsHandlers } from './settings.handlers';
import { registerClientHandlers } from './client.handlers';
import { registerEmailHandlers } from './email.handlers';
import { registerPdfHandlers } from './pdf.handlers';
import { registerWindowHandlers } from './window.handlers';
import { registerShutdownHandlers } from './shutdown.handlers';

/**
 * Dependencies shared across all IPC handler groups.
 * Injected from the composition root (bootstrap.ts).
 */
export interface HandlerDependencies {
  baseDir: string;
  getMainWindow: () => BrowserWindow | null;
  settings: () => AppSettings;
}

/**
 * Register all IPC handlers with injected dependencies.
 * Called once during application startup from bootstrap.ts.
 */
export function registerAllIpcHandlers(deps: HandlerDependencies): void {
  registerChatHandlers(deps);
  registerFileHandlers(deps);
  registerXlsxHandlers(deps);
  registerPipelineHandlers(deps);
  registerExtractionHandlers(deps);
  registerAsycudaHandlers(deps);
  registerSettingsHandlers(deps);
  registerClientHandlers(deps);
  registerEmailHandlers(deps);
  registerPdfHandlers(deps);
  registerWindowHandlers(deps);
  registerShutdownHandlers(deps);
}
