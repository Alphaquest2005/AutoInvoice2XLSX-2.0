/**
 * IPC handlers for the BL processing pipeline.
 * Channels: pipeline:run, pipeline:runFolder, pipeline:runFolderBatch, pipeline:rerunBL
 */

import { ipcMain } from 'electron';
import fs from 'fs';
import path from 'path';
import type { HandlerDependencies } from './index';
import { PythonBridge } from '../services/python-bridge';
import { runBLPipeline } from '../services/email-processor';
import type { BLPipelineResult } from '../services/email-processor';

const bridge = new PythonBridge();

export function registerPipelineHandlers(deps: HandlerDependencies): void {
  // ── Single file pipeline ──────────────────────────────────────────────────
  ipcMain.on(
    'pipeline:run',
    async (_e, inputFile: string, outputFile?: string, _stage?: string) => {
      const win = deps.getMainWindow();
      if (!win || win.isDestroyed()) return;

      // If inputFile is a directory, delegate to runBLPipeline for folder mode
      if (fs.existsSync(inputFile) && fs.statSync(inputFile).isDirectory()) {
        try {
          const result = await runBLPipeline(inputFile, (msg) => {
            if (!win.isDestroyed()) {
              win.webContents.send('pipeline:progress', { stage: 'processing', message: msg });
            }
          });
          if (!win.isDestroyed()) {
            if (result.success) {
              win.webContents.send('pipeline:complete', result);
            } else {
              win.webContents.send('pipeline:error', result.error || 'Pipeline failed');
            }
          }
        } catch (err) {
          if (!win.isDestroyed()) {
            win.webContents.send('pipeline:error', String(err));
          }
        }
        return;
      }

      // Single file: use PythonBridge.run()
      const resolvedOutput = outputFile || inputFile.replace(/\.[^.]+$/, '.xlsx');

      bridge.run(
        inputFile,
        resolvedOutput,
        (progress) => {
          if (!win.isDestroyed()) {
            win.webContents.send('pipeline:progress', progress);
          }
        },
        (report) => {
          if (!win.isDestroyed()) {
            win.webContents.send('pipeline:complete', report);
          }
        },
        (error) => {
          if (!win.isDestroyed()) {
            win.webContents.send('pipeline:error', error);
          }
        },
      );
    },
  );

  // ── Folder pipeline ───────────────────────────────────────────────────────
  ipcMain.on('pipeline:runFolder', async (_e, folderPath: string) => {
    const win = deps.getMainWindow();
    if (!win || win.isDestroyed()) return;

    try {
      const result = await runBLPipeline(folderPath, (msg) => {
        if (!win.isDestroyed()) {
          win.webContents.send('pipeline:progress', { stage: 'processing', message: msg });
          win.webContents.send('folder:progress', msg);
        }
      });

      if (win.isDestroyed()) return;

      if (result.success) {
        win.webContents.send('pipeline:complete', result);
      } else {
        win.webContents.send('pipeline:error', result.error || 'Folder pipeline failed');
      }
    } catch (err) {
      if (!win.isDestroyed()) {
        win.webContents.send('pipeline:error', String(err));
      }
    }
  });

  // ── Batch folder pipeline (each PDF as individual shipment) ───────────────
  ipcMain.on('pipeline:runFolderBatch', async (_e, folderPath: string) => {
    const win = deps.getMainWindow();
    if (!win || win.isDestroyed()) return;

    try {
      // Collect all PDFs in the folder
      const pdfs = fs.readdirSync(folderPath)
        .filter((f) => f.toLowerCase().endsWith('.pdf'))
        .map((f) => path.join(folderPath, f));

      if (pdfs.length === 0) {
        win.webContents.send('pipeline:error', `No PDF files found in: ${folderPath}`);
        return;
      }

      if (!win.isDestroyed()) {
        win.webContents.send('pipeline:progress', {
          stage: 'batch_start',
          message: `Processing ${pdfs.length} PDF(s) individually`,
          total: pdfs.length,
          current: 0,
        });
      }

      const results: { file: string; success: boolean; error?: string }[] = [];

      for (let i = 0; i < pdfs.length; i++) {
        const pdfPath = pdfs[i];
        const pdfName = path.basename(pdfPath);

        if (!win.isDestroyed()) {
          win.webContents.send('pipeline:progress', {
            stage: 'batch_item',
            message: `Processing ${pdfName} (${i + 1}/${pdfs.length})`,
            total: pdfs.length,
            current: i + 1,
          });
        }

        // Process each PDF via PythonBridge as a single-file pipeline
        const outputFile = pdfPath.replace(/\.pdf$/i, '.xlsx');

        const itemResult = await new Promise<{ success: boolean; error?: string }>((resolve) => {
          bridge.run(
            pdfPath,
            outputFile,
            (progress) => {
              if (!win.isDestroyed()) {
                win.webContents.send('pipeline:progress', {
                  ...progress,
                  message: `[${pdfName}] ${progress.message || ''}`,
                  total: pdfs.length,
                  current: i + 1,
                });
              }
            },
            (_report) => resolve({ success: true }),
            (error) => resolve({ success: false, error }),
          );
        });

        results.push({ file: pdfName, ...itemResult });
      }

      if (!win.isDestroyed()) {
        const succeeded = results.filter((r) => r.success).length;
        const failed = results.filter((r) => !r.success).length;
        win.webContents.send('pipeline:complete', {
          status: 'completed',
          mode: 'batch',
          total: pdfs.length,
          succeeded,
          failed,
          results,
        });
      }
    } catch (err) {
      if (!win.isDestroyed()) {
        win.webContents.send('pipeline:error', String(err));
      }
    }
  });

  // ── Re-run BL pipeline (after LLM fixes) ─────────────────────────────────
  ipcMain.on('pipeline:rerunBL', async (_e, inputDir: string) => {
    const win = deps.getMainWindow();
    if (!win || win.isDestroyed()) return;

    try {
      if (!win.isDestroyed()) {
        win.webContents.send('pipeline:progress', {
          stage: 'pipeline_running',
          message: `Re-running BL pipeline on: ${path.basename(inputDir)}`,
        });
      }

      // Extract BL number from directory name if available (e.g. "Shipment_TSCG12345")
      const dirName = path.basename(inputDir);
      const blMatch = dirName.match(/(?:Shipment_?\s*)(.+)/i);
      const blNumber = blMatch ? blMatch[1] : undefined;

      const result: BLPipelineResult = await runBLPipeline(inputDir, (msg) => {
        if (!win.isDestroyed()) {
          win.webContents.send('pipeline:progress', { stage: 'pipeline_running', message: msg });
        }
      }, blNumber);

      if (win.isDestroyed()) return;

      if (!result.success) {
        win.webContents.send('pipeline:error', result.error || 'BL pipeline re-run failed');
        return;
      }

      // Pipeline done
      win.webContents.send('pipeline:progress', {
        stage: 'pipeline_done',
        message: 'Pipeline complete, preparing results',
      });

      win.webContents.send('pipeline:complete', result);
    } catch (err) {
      if (!win.isDestroyed()) {
        win.webContents.send('pipeline:error', String(err));
      }
    }
  });
}
