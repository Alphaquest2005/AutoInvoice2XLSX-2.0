/**
 * IPC handlers for PDF text extraction.
 * Channels: extraction:run
 */

import { ipcMain } from 'electron';
import type { HandlerDependencies } from './index';
import { PythonBridge } from '../services/python-bridge';
import { loadSettings } from '../utils/settings';

// ─── Module-scoped lazy singleton ────────────────────────────────────────────

let pythonBridge: PythonBridge | null = null;

function getPythonBridge(): PythonBridge {
  if (!pythonBridge) {
    pythonBridge = new PythonBridge();
  }
  return pythonBridge;
}

// ─── Handler registration ────────────────────────────────────────────────────

export function registerExtractionHandlers(deps: HandlerDependencies): void {
  ipcMain.on('extraction:run', async (_e, inputPdf: string, outputTxt: string) => {
    const win = deps.getMainWindow();
    if (!win) return;

    try {
      const bridge = getPythonBridge();
      const settings = loadSettings();

      bridge.extractText(
        inputPdf,
        outputTxt,
        settings.apiKey ?? '',
        settings.baseUrl ?? '',
        settings.model ?? '',

        // Progress callback
        (progress) => {
          try {
            win.webContents.send('extraction:progress', progress);
          } catch {
            // Window may have been closed during extraction
          }
        },

        // Complete callback
        (result) => {
          try {
            win.webContents.send('extraction:complete', result);
            // Notify file tree to refresh
            win.webContents.send('files:changed', 'change', outputTxt);
          } catch {
            // Window may have been closed
          }
        },

        // Error callback
        (error) => {
          try {
            win.webContents.send('extraction:error', error);
          } catch {
            // Window may have been closed
          }
        },
      );
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Failed to start extraction';
      console.error('[extraction:run] Error:', msg);
      try {
        win.webContents.send('extraction:error', msg);
      } catch {
        // Window may have been closed
      }
    }
  });
}
