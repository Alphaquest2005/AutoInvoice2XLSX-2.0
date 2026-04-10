/**
 * Composition root: wires all services and registers IPC handlers.
 * Called from index.ts after the BrowserWindow is created.
 */

import type { BrowserWindow } from 'electron';
import { initBaseDirs } from './utils/paths';
import { loadSettings } from './utils/settings';
import { registerAllIpcHandlers } from './ipc';

export function bootstrap(baseDir: string, getMainWindow: () => BrowserWindow | null): void {
  // Initialize SSOT path resolver
  initBaseDirs(baseDir);

  // Register all IPC handlers with injected dependencies
  registerAllIpcHandlers({
    baseDir,
    getMainWindow,
    settings: loadSettings,
  });
}
