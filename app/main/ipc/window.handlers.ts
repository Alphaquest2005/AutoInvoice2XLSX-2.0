import { ipcMain } from 'electron';
import type { HandlerDependencies } from './index';

const ZOOM_STEP = 0.1;
const MIN_ZOOM = 0.5;
const MAX_ZOOM = 2.0;

export function registerWindowHandlers(deps: HandlerDependencies): void {
  // -- Window controls --
  ipcMain.on('window:minimize', () => {
    deps.getMainWindow()?.minimize();
  });

  ipcMain.on('window:maximize', () => {
    const win = deps.getMainWindow();
    if (!win) return;
    if (win.isMaximized()) {
      win.unmaximize();
    } else {
      win.maximize();
    }
  });

  ipcMain.on('window:close', () => {
    deps.getMainWindow()?.close();
  });

  // -- Zoom controls (matches v1: uses getZoomFactor/setZoomFactor) --
  ipcMain.on('zoom:in', () => {
    const win = deps.getMainWindow();
    if (!win) return;
    const current = win.webContents.getZoomFactor();
    const newZoom = Math.min(current + ZOOM_STEP, MAX_ZOOM);
    win.webContents.setZoomFactor(newZoom);
  });

  ipcMain.on('zoom:out', () => {
    const win = deps.getMainWindow();
    if (!win) return;
    const current = win.webContents.getZoomFactor();
    const newZoom = Math.max(current - ZOOM_STEP, MIN_ZOOM);
    win.webContents.setZoomFactor(newZoom);
  });

  ipcMain.on('zoom:reset', () => {
    deps.getMainWindow()?.webContents.setZoomFactor(1.0);
  });

  ipcMain.handle('zoom:get', async () => {
    return deps.getMainWindow()?.webContents.getZoomFactor() ?? 1.0;
  });

  // -- Folder processing --
  ipcMain.handle('folder:select', async () => {
    const { dialog } = require('electron');
    const win = deps.getMainWindow();
    if (!win) return null;
    const result = await dialog.showOpenDialog(win, { properties: ['openDirectory'] });
    return result.canceled ? null : result.filePaths[0] || null;
  });

  ipcMain.handle('folder:process', async (_e, folderPath: string, options?: { limit?: number; start?: number; outputDir?: string }) => {
    const fs = require('fs');
    const path = require('path');
    const { spawn } = require('child_process');
    const { pipelineDir, workspacePath } = require('../utils/paths');

    try {
      const scriptPath = path.join(pipelineDir(), 'test_email_workflow.py');
      const pythonCmd = process.platform === 'win32' ? 'python' : 'python3';

      if (!fs.existsSync(scriptPath)) {
        return { success: false, error: 'test_email_workflow.py not found' };
      }

      const outputDir = options?.outputDir || path.join(workspacePath(), 'tests', 'email_test');

      const args = [scriptPath, folderPath, '--output-dir', outputDir];

      if (options?.limit !== undefined) {
        args.push('--limit', String(options.limit));
      } else {
        args.push('--all');
      }

      if (options?.start !== undefined) {
        args.push('--start', String(options.start));
      }

      return new Promise((resolve) => {
        const proc = spawn(pythonCmd, args, {
          cwd: pipelineDir(),
          env: { ...process.env, PYTHONIOENCODING: 'utf-8' },
        });

        let stdout = '';
        let stderr = '';

        proc.stdout.on('data', (data: Buffer) => {
          stdout += data.toString();
          const win = deps.getMainWindow();
          win?.webContents.send('folder:progress', data.toString());
        });

        proc.stderr.on('data', (data: Buffer) => {
          stderr += data.toString();
          console.log(`[folder:process] ${data}`);
        });

        proc.on('close', (code: number) => {
          console.log(`[folder] Process finished with code ${code}`);

          const summaryPath = path.join(outputDir, 'processing_summary.json');
          if (fs.existsSync(summaryPath)) {
            try {
              const summary = JSON.parse(fs.readFileSync(summaryPath, 'utf-8'));
              resolve({ success: true, ...summary });
            } catch {
              resolve({ success: code === 0, stdout, stderr });
            }
          } else {
            resolve({
              success: code === 0,
              error: code !== 0 ? stderr || 'Process failed' : undefined,
              stdout,
            });
          }
        });

        proc.on('error', (err: Error) => {
          console.error(`[folder] spawn error:`, err);
          resolve({ success: false, error: `Failed to start Python: ${err.message}` });
        });
      });
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err);
      console.error('[folder] Process error:', error);
      return { success: false, error };
    }
  });
}
