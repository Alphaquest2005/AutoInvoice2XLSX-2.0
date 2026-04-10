/**
 * IPC handlers for ASYCUDA XML import and CET classification management.
 * Channels: asycuda:importXml, asycuda:importMultiple, asycuda:getSkuClassification,
 *           asycuda:getSkuCorrections, asycuda:getImports, asycuda:getStats,
 *           asycuda:browseXmlFolder, asycuda:generateCostingSheet
 */

import { ipcMain, dialog, shell } from 'electron';
import { spawn } from 'child_process';
import path from 'path';
import type { HandlerDependencies } from './index';
import { baseDir } from '../utils/paths';
import {
  getSkuClassification,
  getSkuCorrections,
  getAsycudaImports,
  getCetStats,
  importAsycudaClassifications,
} from '../stores/cet.store';
import type { AsycudaImportData } from '../stores/cet.store';

// ---------------------------------------------------------------------------
// Helper: spawn a Python script and collect JSON output from stdout
// ---------------------------------------------------------------------------

function spawnPython(scriptPath: string, args: string[]): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const pythonCmd = process.platform === 'win32' ? 'python' : 'python3';
    const proc = spawn(pythonCmd, [scriptPath, ...args], {
      cwd: baseDir(),
      env: { ...process.env },
    });

    let stdout = '';
    let stderr = '';

    proc.stdout.on('data', (chunk: Buffer) => { stdout += chunk.toString(); });
    proc.stderr.on('data', (chunk: Buffer) => { stderr += chunk.toString(); });

    proc.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(`Python exited with code ${code}: ${stderr.trim()}`));
        return;
      }
      try {
        resolve(JSON.parse(stdout));
      } catch {
        reject(new Error(`Failed to parse Python output as JSON: ${stdout.slice(0, 200)}`));
      }
    });

    proc.on('error', (err) => {
      reject(new Error(`Failed to spawn Python: ${err.message}`));
    });
  });
}

// ---------------------------------------------------------------------------
// Handler registration
// ---------------------------------------------------------------------------

export function registerAsycudaHandlers(deps: HandlerDependencies): void {
  ipcMain.handle('asycuda:importXml', async (_e, xmlPath: string) => {
    try {
      const scriptPath = path.join(baseDir(), 'pipeline', 'asycuda_xml_parser.py');
      const parsed = (await spawnPython(scriptPath, [xmlPath])) as AsycudaImportData;

      // Ensure file_path is set
      if (!parsed.file_path) {
        parsed.file_path = xmlPath;
      }

      const result = importAsycudaClassifications(parsed);

      if ('error' in result) {
        return { success: false, importedCount: 0, updatedCount: 0, skippedCount: 0, errors: [result.error] };
      }

      return {
        success: true,
        importedCount: result.imported,
        updatedCount: result.corrected,
        skippedCount: result.skipped,
        corrections: result.corrections,
        errors: [],
      };
    } catch (err) {
      return {
        success: false,
        importedCount: 0,
        updatedCount: 0,
        skippedCount: 0,
        errors: [(err as Error).message],
      };
    }
  });

  ipcMain.handle('asycuda:importMultiple', async (_e, xmlPaths: string[]) => {
    const results: Array<{ path: string; success: boolean; imported: number; errors: string[] }> = [];
    let totalImported = 0;
    let totalUpdated = 0;
    let totalSkipped = 0;
    const allErrors: string[] = [];

    const scriptPath = path.join(baseDir(), 'pipeline', 'asycuda_xml_parser.py');

    for (const xmlPath of xmlPaths) {
      try {
        const parsed = (await spawnPython(scriptPath, [xmlPath])) as AsycudaImportData;
        if (!parsed.file_path) parsed.file_path = xmlPath;

        const result = importAsycudaClassifications(parsed);

        if ('error' in result) {
          results.push({ path: xmlPath, success: false, imported: 0, errors: [result.error] });
          allErrors.push(`${xmlPath}: ${result.error}`);
        } else {
          totalImported += result.imported;
          totalUpdated += result.corrected;
          totalSkipped += result.skipped;
          results.push({ path: xmlPath, success: true, imported: result.imported, errors: [] });
        }
      } catch (err) {
        const msg = (err as Error).message;
        results.push({ path: xmlPath, success: false, imported: 0, errors: [msg] });
        allErrors.push(`${xmlPath}: ${msg}`);
      }
    }

    return {
      success: allErrors.length === 0,
      totalImported,
      totalUpdated,
      totalSkipped,
      results,
      errors: allErrors,
    };
  });

  ipcMain.handle('asycuda:getSkuClassification', async (_e, sku: string) => {
    return getSkuClassification(sku);
  });

  ipcMain.handle('asycuda:getSkuCorrections', async (_e, sku: string) => {
    return getSkuCorrections(sku);
  });

  ipcMain.handle('asycuda:getImports', async () => {
    return getAsycudaImports();
  });

  ipcMain.handle('asycuda:getStats', async () => {
    return getCetStats();
  });

  ipcMain.handle('asycuda:browseXmlFolder', async () => {
    const win = deps.getMainWindow();
    if (!win) return [];
    const result = await dialog.showOpenDialog(win, {
      properties: ['openFile', 'multiSelections'],
      filters: [{ name: 'XML Files', extensions: ['xml'] }],
    });
    return result.canceled ? [] : result.filePaths;
  });

  ipcMain.handle(
    'asycuda:generateCostingSheet',
    async (_e, xmlPath: string, outputPath?: string) => {
      try {
        const scriptPath = path.join(baseDir(), 'pipeline', 'costing_sheet_generator.py');
        const args = [xmlPath];
        if (outputPath) args.push(outputPath);

        const result = await spawnPython(scriptPath, args);
        return { success: true, ...(result as Record<string, unknown>) };
      } catch (err) {
        return { success: false, error: (err as Error).message };
      }
    },
  );
}
