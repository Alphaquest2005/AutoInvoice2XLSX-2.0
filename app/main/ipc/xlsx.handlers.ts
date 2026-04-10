/**
 * IPC handlers for XLSX file operations.
 * Channels: xlsx:parse, xlsx:openInExcel, xlsx:combine
 */

import { ipcMain, shell } from 'electron';
import { spawn } from 'child_process';
import path from 'path';
import type { HandlerDependencies } from './index';
import { parseXlsxFile } from '../services/xlsx-parser';
import { baseDir } from '../utils/paths';

export function registerXlsxHandlers(deps: HandlerDependencies): void {
  // ── Parse XLSX ─────────────────────────────────────────────────────────────
  ipcMain.handle('xlsx:parse', async (_e, filePath: string) => {
    return parseXlsxFile(filePath);
  });

  // ── Open in Excel ──────────────────────────────────────────────────────────
  ipcMain.handle('xlsx:openInExcel', async (_e, filePath: string) => {
    try {
      await shell.openPath(filePath);
      return { success: true };
    } catch (err) {
      return { success: false, error: String(err) };
    }
  });

  // ── Combine multiple XLSX files ────────────────────────────────────────────
  ipcMain.handle('xlsx:combine', async (_e, filePaths: string[]) => {
    if (!filePaths || filePaths.length === 0) {
      return { success: false, error: 'No files provided' };
    }

    const pythonCmd = process.platform === 'win32' ? 'python' : 'python3';
    const scriptPath = path.join(baseDir(), 'pipeline', 'xlsx_combiner.py');

    return new Promise<{ success: boolean; outputPath?: string; error?: string }>((resolve) => {
      const args = ['-u', scriptPath, '--json-output', ...filePaths];

      const proc = spawn(pythonCmd, args, {
        cwd: baseDir(),
        env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONUNBUFFERED: '1' },
      });

      let stdout = '';
      let stderr = '';

      proc.stdout.on('data', (data: Buffer) => {
        stdout += data.toString();
      });

      proc.stderr.on('data', (data: Buffer) => {
        stderr += data.toString();
      });

      proc.on('close', (code) => {
        if (code === 0) {
          // Try to parse JSON output from the script
          try {
            const lines = stdout.trim().split('\n');
            for (let i = lines.length - 1; i >= 0; i--) {
              const line = lines[i].trim();
              if (line.startsWith('{') && line.endsWith('}')) {
                const result = JSON.parse(line);
                resolve({
                  success: true,
                  outputPath: result.output_path || result.outputPath,
                });
                return;
              }
            }
          } catch { /* fall through */ }
          resolve({ success: true });
        } else {
          resolve({
            success: false,
            error: stderr || stdout || `xlsx_combiner.py exited with code ${code}`,
          });
        }
      });

      proc.on('error', (err) => {
        resolve({
          success: false,
          error: `Failed to start Python: ${err.message}. Is Python 3 installed?`,
        });
      });
    });
  });
}
