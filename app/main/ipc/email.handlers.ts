import { ipcMain } from 'electron';
import fs from 'fs';
import path from 'path';
import type { HandlerDependencies } from './index';

import { emailService, sendEmail } from '../services/email-service';
import {
  runBLPipeline,
  sendShipmentEmailFromParams,
  classifyEmailDir,
  llmMatchEmails,
  createCombinedFolder,
} from '../services/email-processor';
import {
  getClient,
  getClients,
  updateProcessedEmail,
  getResumableEmails,
  incrementRetryCount,
  findProcessedEmailByInputDir,
  findProcessedEmailByShipmentDir,
  findRecentUnlinkedEmails,
  updateEmailDocTypes,
  linkEmailRecords,
} from '../stores/client.store';
import { onFileChange } from '../services/file-watcher';

export function registerEmailHandlers(deps: HandlerDependencies): void {
  // Auto-resend cooldown tracker: prevents double-sends when _email_params.json is written
  const recentlySentParams = new Map<string, number>(); // path -> timestamp
  const RESEND_COOLDOWN_MS = 30_000; // 30s cooldown after sending

  // -- IPC Handlers --

  ipcMain.handle('email:startMonitor', async (_e, clientId: string) => {
    try {
      const client = getClient(clientId);
      if (!client) return { success: false, error: `Client not found: ${clientId}` };
      await emailService.startMonitor(client);
      return { success: true };
    } catch (err) {
      return { success: false, error: (err as Error).message };
    }
  });

  ipcMain.handle('email:stopMonitor', async (_e, clientId: string) => {
    try {
      await emailService.stopMonitor(clientId);
      return { success: true };
    } catch (err) {
      return { success: false, error: (err as Error).message };
    }
  });

  ipcMain.handle('email:getStatus', async (_e, clientId: string) => {
    return emailService.getStatus(clientId);
  });

  ipcMain.handle('email:getAllStatuses', async () => {
    return emailService.getAllStatuses();
  });

  ipcMain.handle('email:sendTest', async (_e, clientId: string, to: string) => {
    try {
      const client = getClient(clientId);
      if (!client) return { success: false, error: `Client not found: ${clientId}` };
      await sendEmail(client, {
        to,
        subject: '[AutoInvoice] Test Email',
        body: `This is a test email sent from AutoInvoice2XLSX for client "${client.name}".\n\nTimestamp: ${new Date().toISOString()}`,
      });
      return { success: true };
    } catch (err) {
      return { success: false, error: (err as Error).message };
    }
  });

  // -- Event Listeners (wired to emailService events) --

  emailService.on('email:received', (email: unknown, clientId: string) => {
    deps.getMainWindow()?.webContents.send('email:received', { email, clientId });
  });

  emailService.on('email:error', (err: Error, clientId: string) => {
    deps.getMainWindow()?.webContents.send('email:error', {
      clientId,
      error: err.message,
    });
  });

  emailService.on('status:change', (status: unknown) => {
    deps.getMainWindow()?.webContents.send('email:status', status);
  });

  emailService.on('email:progress', (data: { clientId: string; message: string }) => {
    deps.getMainWindow()?.webContents.send('email:progress', data);
  });

  emailService.on('bl:process', async (data: {
    inputDir: string;
    clientId: string;
    recordId: string;
    blNumber?: string;
  }) => {
    const { inputDir, clientId, recordId, blNumber } = data;
    // Use a getter so we always have the current window (may be null during early resume)
    const getWin = () => deps.getMainWindow();

    try {
      updateProcessedEmail(recordId, { status: 'pipeline_running' });
      getWin()?.webContents.send('email:status', { clientId, pipelineRunning: true });

      const logProgress = (msg: string) => {
        getWin()?.webContents.send('email:progress', { clientId, message: msg });
        getWin()?.webContents.send('pipeline:progress', { clientId, recordId, message: msg });
      };

      const result = await runBLPipeline(inputDir, logProgress, blNumber);
      console.log(`[email-handlers] Pipeline result: success=${result.success}, checklist=${JSON.stringify(result.checklist?.passed)}, blocker_count=${result.checklist?.blocker_count}`);

      if (result.success) {
        const checklistBlocked = result.checklist && !result.checklist.passed && result.checklist.blocker_count > 0;

        // Send validation issues if present — but SKIP if checklist is also blocked,
        // because the checklist auto-fix prompt already covers validation issues.
        // Sending both causes a race condition where two async handlers fight over activeConversationId.
        if (result.validation && Object.keys(result.validation).length > 0 && !checklistBlocked) {
          getWin()?.webContents.send('pipeline:validationIssues', {
            conversationId: '',
            validation: result.validation,
            inputDir,
            clientId,
            recordId,
          });
        }

        // Check for checklist blockers
        if (checklistBlocked && result.checklist) {
          const checklist = result.checklist;
          logProgress(`Email BLOCKED by checklist: ${checklist.blocker_count} blocker(s)`);

          updateProcessedEmail(recordId, {
            status: 'needs_review',
            waybillNumber: result.bl?.blNumber,
            invoiceNumber: String(result.invoiceCount ?? ""),
            outputFiles: result.outputFiles,
            error: `Checklist failed: ${checklist.blocker_count} blocker(s)`,
          });

          // Build LLM auto-fix prompt (matches v1 behavior)
          const checklistLines: string[] = [];
          for (const f of checklist.failures) {
            const severity = f.severity === 'block' ? 'BLOCKER' : 'WARNING';
            checklistLines.push(`  [${severity}] ${f.check}: ${f.message}`);
            if (f.fix_hint) {
              checklistLines.push(`    Fix: ${f.fix_hint}`);
            }
          }

          // Determine output directory (where XLSX/PDFs live)
          const outputDir = result.emailParamsPath
            ? path.dirname(result.emailParamsPath)
            : inputDir;

          let dirListing = '';
          try {
            const files = fs.readdirSync(outputDir);
            const blPdfs = files.filter((f: string) => f.endsWith('-BL.pdf') || f.toLowerCase().includes('declaration'));
            const xlsxFiles = files.filter((f: string) => f.endsWith('.xlsx'));
            dirListing = `\nBL/Declaration PDFs: ${blPdfs.join(', ') || 'none found'}\n` +
              `XLSX files (${xlsxFiles.length}): ${xlsxFiles.slice(0, 10).join(', ')}${xlsxFiles.length > 10 ? '...' : ''}\n`;
          } catch { /* ignore */ }

          const fixPrompt = `The shipment email was BLOCKED by the pre-send checklist. ${checklist.blocker_count} blocker(s) must be fixed before the email can be sent.\n` +
            `Output directory: ${outputDir}\n` +
            `Email params file: ${result.emailParamsPath || 'not found'}\n` +
            `Input directory: ${inputDir}\n` +
            dirListing + `\n` +
            `Checklist failures:\n${checklistLines.join('\n')}\n\n` +
            `═══ PRIORITY: FIX BLOCKERS THEN SEND THE EMAIL ═══\n` +
            `You have a MAXIMUM of 7 tool calls. Do NOT investigate unnecessarily.\n\n` +
            `CRITICAL: The XLSX worksheet structure is FIXED. You MUST NOT:\n` +
            `  - Add, remove, or reorder rows or columns\n` +
            `  - Rewrite or recreate any XLSX file\n` +
            `  - Use write_file or edit_file on XLSX files\n` +
            `  - Add summary rows, adjustment rows, total rows, or any new content\n` +
            `  - Write text containing "TOTAL", "ADJUSTMENT", "SUMMARY", or similar into any cell\n` +
            `  - Modify row 1 (header row) — it is read-only\n` +
            `  - Modify any text/description cells — ONLY edit NUMERIC values (amounts, counts, tariff codes)\n` +
            `Row 2 is the first GROUP row (invoice-level data). Do NOT overwrite it with totals.\n` +
            `Use edit_xlsx_cell to change ONLY individual NUMERIC cell values.\n\n` +
            `STEP 1: Run validate_xlsx on the XLSX file(s). If variance_check = 0 or null, skip STEP 2.\n` +
            `STEP 2: Fix ONLY the specific blocker(s) listed above:\n` +
            `  - unfixed_variance: Use edit_xlsx_cell to adjust freight (T2), insurance (U2), tax (V2), or deductions (W2) to zero the variance.\n` +
            `  - weight_zero/packages_zero: Read the BL PDF or declaration PDF, find weight/packages, edit _email_params.json.\n` +
            `  - consignee_missing: Read the BL/declaration PDF, find consignee name, edit _email_params.json.\n` +
            `  - unfixed_tariff: Use lookup_tariff, then edit_xlsx_cell to fix column F and AK.\n` +
            `  - package_mismatch: Read BL PDF for package count, use edit_xlsx_cell to fix column X row 2.\n` +
            `  - duplicate_content: Delete the duplicate file and remove from attachment_paths in _email_params.json.\n` +
            `STEP 3: Call send_shipment_email with email_params_path="${result.emailParamsPath || ''}".\n` +
            `After STEP 3, STOP. Do NOT do anything else.`;

          getWin()?.webContents.send('pipeline:checklistFailed', {
            conversationId: '',
            clientId,
            recordId,
            checklist,
            validation: result.validation,
            emailParamsPath: result.emailParamsPath,
            inputDir: outputDir,
            outputDir,
            prompt: fixPrompt,
          });

          logProgress(`Checklist blocked: ${checklist.blocker_count} blocker(s) — forwarded to LLM for auto-fix`);
          return;
        }

        // Surface failures to UI (Python already decided whether to send)
        if (result.failures && result.failures.length > 0) {
          logProgress(`${result.failures.length} invoice(s) had processing issues`);
          getWin()?.webContents.send('pipeline:failures', {
            clientId,
            recordId,
            failures: result.failures,
          });
        }

        // Python pipeline (--send-email) is the SSOT for checklist → send → history.
        // We just read the report and update the Electron-side record.
        if (result.emailParamsPath) {
          recentlySentParams.set(result.emailParamsPath, Date.now());
        }
        updateProcessedEmail(recordId, {
          status: 'completed',
          waybillNumber: result.bl?.blNumber,
          invoiceNumber: String(result.invoiceCount ?? ""),
          outputFiles: result.outputFiles,
          emailSent: result.emailSent ?? false,
        });

        getWin()?.webContents.send('pipeline:complete', {
          clientId,
          recordId,
          result,
        });
      } else {
        updateProcessedEmail(recordId, {
          status: 'error',
          error: result.error,
        });
        getWin()?.webContents.send('pipeline:error', {
          clientId,
          recordId,
          error: result.error,
        });
      }
    } catch (err) {
      console.error('[email-handlers] bl:process CAUGHT ERROR:', (err as Error).message, (err as Error).stack);
      updateProcessedEmail(recordId, {
        status: 'error',
        error: (err as Error).message,
      });
      getWin()?.webContents.send('pipeline:error', {
        clientId,
        recordId,
        error: (err as Error).message,
      });
    }
  });

  emailService.on('bl:classifyAndRoute', async (data: {
    inputDir: string;
    clientId: string;
    recordId: string;
  }) => {
    const { inputDir, clientId, recordId } = data;

    try {
      // Classify all PDFs in the email directory
      const classification = await classifyEmailDir(inputDir);

      if (!classification.has_bl && !classification.has_invoices) {
        console.log('[email-handlers] No classifiable PDFs in', inputDir);
        return;
      }

      // Track document types on the record
      const docTypes = (Object.entries(classification.classification) as [string, string[]][])
        .filter(([, files]) => files.length > 0)
        .map(([type]) => type);
      updateEmailDocTypes(recordId, docTypes);

      const hasBL = classification.has_bl;
      const hasInvoice = classification.has_invoices;

      if (hasBL && hasInvoice) {
        // Both BL and invoices in same email -- process directly
        const blNumber = classification.bl_metadata?.bl_number;
        emailService.emit('bl:process', {
          inputDir,
          clientId,
          recordId,
          blNumber,
        });
      } else if (hasBL && !hasInvoice) {
        // BL-only email -- find matching invoice email
        const client = getClient(clientId);
        const prefix = (client?.incomingEmail.address || '').split('@')[0].split('.')[0] || 'emails';
        const blNumber = classification.bl_metadata?.bl_number;

        // Look for recent unlinked invoice emails
        const candidates = findRecentUnlinkedEmails(clientId, 'invoice', 'bl');

        if (candidates.length > 0) {
          // Read the email body from the BL email dir
          const fs = require('fs');
          const path = require('path');
          let blBody = '';
          let blSubject = '';
          const emailTxtPath = path.join(inputDir, 'email.txt');
          if (fs.existsSync(emailTxtPath)) {
            const content = fs.readFileSync(emailTxtPath, 'utf-8');
            const subjectMatch = content.match(/^Subject:\s*(.+)$/m);
            blSubject = subjectMatch ? subjectMatch[1] : '';
            blBody = content;
          }

          const matchCandidates = candidates.map((c) => {
            let body = '';
            if (c.inputDir) {
              const candidateEmailPath = path.join(c.inputDir, 'email.txt');
              if (fs.existsSync(candidateEmailPath)) {
                body = fs.readFileSync(candidateEmailPath, 'utf-8');
              }
            }
            return {
              id: String(c.id),
              subject: c.subject || '',
              body,
              receivedAt: c.receivedAt || '',
            };
          });

          const matchedId = await llmMatchEmails(
            blBody,
            blSubject,
            classification.bl_metadata,
            matchCandidates,
          );

          if (matchedId) {
            const matchedRecord = candidates.find((c) => String(c.id) === matchedId);
            if (matchedRecord?.inputDir) {
              // Link the two email records
              linkEmailRecords(recordId, matchedRecord.id);

              // Create combined folder and trigger pipeline
              const combinedDir = createCombinedFolder(
                prefix,
                inputDir,
                matchedRecord.inputDir,
                blNumber || '',
              );

              emailService.emit('bl:process', {
                inputDir: combinedDir,
                clientId,
                recordId,
                blNumber,
              });
              return;
            }
          }
        }

        // No match found -- mark as waiting for invoices
        updateProcessedEmail(recordId, {
          status: 'files_ready',
          waybillNumber: blNumber || undefined,
          error: 'BL received, waiting for matching invoice email',
        });
      } else if (hasInvoice && !hasBL) {
        // Invoice-only email -- check if there is a waiting BL email
        const blCandidates = findRecentUnlinkedEmails(clientId, 'bl', 'invoice');

        if (blCandidates.length > 0) {
          // Auto-match: use the most recent unlinked BL email
          const blRecord = blCandidates[0];
          if (blRecord.inputDir) {
            const client = getClient(clientId);
            const prefix = (client?.incomingEmail.address || '').split('@')[0].split('.')[0] || 'emails';

            linkEmailRecords(recordId, blRecord.id);

            const combinedDir = createCombinedFolder(
              prefix,
              blRecord.inputDir,
              inputDir,
              blRecord.waybillNumber || '',
            );

            emailService.emit('bl:process', {
              inputDir: combinedDir,
              clientId,
              recordId,
              blNumber: blRecord.waybillNumber,
            });
          }
        }
        // If no BL match, the email stays at files_ready until a BL arrives
      }
    } catch (err) {
      console.error('[email-handlers] classifyAndRoute error:', (err as Error).message);
      updateProcessedEmail(recordId, {
        status: 'error',
        error: `Classification failed: ${(err as Error).message}`,
      });
    }
  });

  // -- Auto-start monitors for enabled clients --
  (async () => {
    try {
      const clients = getClients().filter((c) => c.enabled);
      for (const client of clients) {
        try {
          await emailService.startMonitor(client);
        } catch (err) {
          console.error(`[email-handlers] Auto-start failed for ${client.name}:`, (err as Error).message);
        }
      }
    } catch (err) {
      console.error('[email-handlers] Auto-start error:', (err as Error).message);
    }
  })();

  // -- Resume incomplete emails from previous session --
  (async () => {
    try {
      const resumable = getResumableEmails();
      if (resumable.length === 0) return;

      console.log(`[email-handlers] Resuming ${resumable.length} incomplete email(s)`);

      for (const record of resumable) {
        incrementRetryCount(record.id);

        if (record.status === 'files_ready' && record.inputDir) {
          // Re-trigger classification and routing
          emailService.emit('bl:classifyAndRoute', {
            inputDir: record.inputDir,
            clientId: record.clientId,
            recordId: record.id,
          });
        } else if (
          (record.status === 'pipeline_running' || record.status === 'pipeline_done') &&
          record.inputDir
        ) {
          // Re-run pipeline
          emailService.emit('bl:process', {
            inputDir: record.inputDir,
            clientId: record.clientId,
            recordId: record.id,
            blNumber: record.waybillNumber,
          });
        } else if (record.status === 'email_sending' && record.inputDir) {
          // Re-attempt email sending
          const fs = require('fs');
          const path = require('path');
          const paramsPath = path.join(record.inputDir, '_email_params.json');
          if (fs.existsSync(paramsPath)) {
            const result = await sendShipmentEmailFromParams(paramsPath);
            updateProcessedEmail(record.id, {
              status: result.success ? 'completed' : 'error',
              emailSent: result.success,
              error: result.error,
            });
          } else {
            updateProcessedEmail(record.id, {
              status: 'error',
              error: 'Email params file not found on resume',
            });
          }
        }
      }
    } catch (err) {
      console.error('[email-handlers] Resume error:', (err as Error).message);
    }
  })();

  // -- Manual resend IPC handler --
  ipcMain.handle('bl:resendEmail', async (_e, paramsPath: string) => {
    try {
      if (!fs.existsSync(paramsPath)) {
        return { success: false, error: 'Email params file not found' };
      }
      const logProgress = (msg: string) => {
        deps.getMainWindow()?.webContents.send('pipeline:progress', { message: msg });
      };
      const result = await sendShipmentEmailFromParams(paramsPath, logProgress);
      if (result.success) {
        // Update the processed_email record if we can find it
        const shipmentDir = path.dirname(paramsPath);
        const record = findProcessedEmailByShipmentDir(shipmentDir)
          || findProcessedEmailByInputDir(shipmentDir);
        if (record) {
          updateProcessedEmail(record.id, { emailSent: true });
        }
        recentlySentParams.set(paramsPath, Date.now());
      }
      return result;
    } catch (err) {
      return { success: false, error: (err as Error).message };
    }
  });

  // -- Auto-resend on _email_params.json modification --
  // When _email_params.json is edited (e.g. fixing weight/consignee/total),
  // resend the email via the Python SSOT (send_shipment_email.py).
  // Works regardless of whether a SQLite record exists — CLI-initiated
  // pipelines also benefit.
  onFileChange(async (_event: string, filePath: string) => {
    if (!filePath.endsWith('_email_params.json')) return;

    const now = Date.now();
    const lastSent = recentlySentParams.get(filePath) || 0;
    if (now - lastSent < RESEND_COOLDOWN_MS) return;

    // Auto-resend if EITHER:
    //  (a) a completed record exists (previously sent — normal resend), OR
    //  (b) a record exists with emailSent=false (first send was blocked/failed), OR
    //  (c) no record at all but the file exists (CLI-initiated pipeline)
    const shipmentDir = path.dirname(filePath);
    const record = findProcessedEmailByShipmentDir(shipmentDir)
      || findProcessedEmailByInputDir(shipmentDir);

    // If a record exists but pipeline is still running, skip
    if (record && record.status === 'pipeline_running') return;

    console.log(`[email-handlers] _email_params.json modified: ${shipmentDir} (record=${record?.id ?? 'none'})`);
    console.log('[email-handlers] Auto-resending email via Python SSOT...');

    recentlySentParams.set(filePath, now);

    try {
      const logProgress = (msg: string) => {
        deps.getMainWindow()?.webContents.send('pipeline:progress', { message: msg });
      };
      const result = await sendShipmentEmailFromParams(filePath, logProgress);
      if (result.success) {
        console.log('[email-handlers] Auto-resend successful');
        if (record) {
          updateProcessedEmail(record.id, { emailSent: true, status: 'completed' });
        }
        deps.getMainWindow()?.webContents.send('email:autoResent', {
          paramsPath: filePath,
          shipmentDir,
          recordId: record?.id,
        });
      } else {
        console.error('[email-handlers] Auto-resend failed:', result.error);
      }
    } catch (err) {
      console.error('[email-handlers] Auto-resend error:', (err as Error).message);
    }
  });
}
