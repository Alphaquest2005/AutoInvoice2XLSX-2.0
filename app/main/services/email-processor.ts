/**
 * Email Processor (v2.0)
 *
 * Orchestrates the complete email processing workflow:
 *  1. processIncomingEmail — single-email pipeline (split → extract → pipeline → send)
 *  2. runBLPipeline — unified pipeline on a folder (auto-detects BL mode)
 *  3. sendShipmentEmailFromParams — send email using saved _email_params.json
 *  4. classifyEmailDir — classify all PDFs in an email directory
 *  5. llmMatchEmails — use LLM to match a BL email to the correct invoice email
 *  6. createCombinedFolder — merge BL + invoice directories
 *
 * All Python interactions use spawn + JSON-over-stdout protocol.
 */

import path from 'path';
import fs from 'fs';
import { spawn } from 'child_process';

import { baseDir, workspacePath, dataPath, shipmentsPath } from '../utils/paths';
import { loadSettings } from '../utils/settings';
import { updateProcessedEmail } from '../stores/client.store';
import { sendErrorNotification } from './email-service';
import type { RuntimeEmail } from './email-service';
import type {
  ClientSettings,
  ProcessedEmail,
  PdfSplitResult,
  ClassificationResult,
  BLMetadata,
} from '../../shared/types';

// ---------------------------------------------------------------------------
// Logging helper
// ---------------------------------------------------------------------------

const TAG = '[email-processor]';

function log(msg: string): void {
  console.log(`${TAG} ${msg}`);
}

// ---------------------------------------------------------------------------
// Python helper
// ---------------------------------------------------------------------------

function pythonCmd(): string {
  return process.platform === 'win32' ? 'python' : 'python3';
}

function pipelineDir(): string {
  return path.join(baseDir(), 'pipeline');
}

/** Spawn a Python script and collect stdout/stderr. Resolves on close. */
function spawnPython(
  scriptPath: string,
  args: string[],
  cwd: string,
  onStdoutLine?: (line: string) => void,
): Promise<{ code: number | null; stdout: string; stderr: string }> {
  return new Promise((resolve) => {
    const proc = spawn(pythonCmd(), ['-u', scriptPath, ...args], {
      cwd,
      env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONUNBUFFERED: '1' },
    });

    let stdout = '';
    let stderr = '';

    proc.stdout.on('data', (data) => {
      const chunk = data.toString();
      stdout += chunk;
      if (onStdoutLine) {
        for (const line of chunk.split('\n')) {
          const trimmed = line.trim();
          if (trimmed) onStdoutLine(trimmed);
        }
      }
    });

    proc.stderr.on('data', (data) => {
      stderr += data.toString();
    });

    proc.on('close', (code) => resolve({ code, stdout, stderr }));
    proc.on('error', (err) => resolve({ code: -1, stdout, stderr: err.message }));
  });
}

/** Extract the REPORT:JSON: line from stdout. */
function extractJsonReport(stdout: string): any | null {
  const match = stdout.match(/REPORT:JSON:(.+)/);
  if (match) {
    try { return JSON.parse(match[1]); } catch { /* ignore */ }
  }
  return null;
}

/** Sanitise a string for use as a filesystem name. */
function sanitiseFilename(input: string, maxLen = 120): string {
  return (input || 'no-subject')
    .replace(/[<>:"/\\|?*]/g, '_')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, maxLen);
}

// ---------------------------------------------------------------------------
// Types (internal to email-processor)
// ---------------------------------------------------------------------------

export interface ProcessingResult {
  success: boolean;
  waybill?: string;
  invoiceNumber?: string;
  outputFiles?: string[];
  declarationData?: DeclarationData;
  varianceCheck?: number;
  emailSkipped?: boolean;
  skipReason?: string;
  error?: string;
}

export interface PipelineResult {
  xlsxPath?: string;
  varianceCheck?: number;
  xlsxTotals?: XlsxTotals;
  success: boolean;
  error?: string;
}

export interface XlsxTotals {
  invoiceTotal?: number;
  freight?: number;
  insurance?: number;
  otherCost?: number;
  netTotal?: number;
}

export interface DeclarationData {
  waybill?: string;
  customsFile?: string;
  manifestRegistry?: string;
  consignee?: string;
  packages?: string;
  grossWeight?: string;
  countryOfOrigin?: string;
  fobValue?: string;
  freight?: string;
  insurance?: string;
  cifValue?: string;
}

export interface BLPipelineResult {
  success: boolean;
  mode?: string;
  invoiceCount?: number;
  bl?: {
    blNumber: string;
    freight: number;
    packages: string;
    weight: string;
    insurance: number;
    blPdf?: string;
  };
  emailSent?: boolean;
  emailParamsPath?: string;
  outputFiles?: string[];
  failures?: { pdf_path: string; pdf_file: string; reason: string; invoice_num?: string }[];
  validation?: {
    total_issues: number;
    fixed: number;
    unfixed: number;
    per_file: { file: string; supplier?: string; issues: any[]; remaining?: any[]; fixed: number; unfixed: number }[];
    package_check?: { expected: number; actual: number; source: string; mismatch: boolean };
    duplicate_check?: { duplicates: any[]; content_duplicates: any[]; stale: string[]; has_issues: boolean };
  };
  checklist?: {
    passed: boolean;
    failures: { check: string; severity: string; message: string; field: string; value: string; fix_hint: string }[];
    blocker_count: number;
    warning_count: number;
  };
  error?: string;
}

// ---------------------------------------------------------------------------
// 1. processIncomingEmail
// ---------------------------------------------------------------------------

/**
 * Process an incoming email through the full workflow:
 * split PDF → extract declaration → run pipeline → LLM variance fix → send email.
 */
export async function processIncomingEmail(
  email: RuntimeEmail,
  client: ClientSettings,
  recordId: string,
  logProgress?: (msg: string) => void,
): Promise<ProcessingResult> {
  const progress = (msg: string) => {
    log(msg);
    logProgress?.(msg);
  };

  try {
    progress(`Processing email: ${email.subject}`);

    // Step 1: Save PDF attachments
    const pdfAttachments = email.attachments.filter(
      (a) => a.contentType === 'application/pdf' || a.filename?.toLowerCase().endsWith('.pdf'),
    );
    if (pdfAttachments.length === 0) throw new Error('No PDF attachments found');

    const safeSubject = sanitiseFilename(email.subject);
    const emailPrefix = (client.incomingEmail.address || '').split('@')[0].split('.')[0] || 'emails';
    const workDir = path.join(workspacePath(), emailPrefix, safeSubject);
    fs.mkdirSync(workDir, { recursive: true });

    // Extract waybill from subject
    const waybillMatch = email.subject.match(/Shipment:\s*([A-Z0-9-]+)/i);
    const waybill = waybillMatch ? waybillMatch[1] : `EMAIL_${Date.now()}`;

    // Extract manifest number from email body or subject
    const manRegPattern = /(?:MNF|MAN(?:IFEST)?)\s*(?:REG(?:ISTRY)?|NUM(?:BER)?)?\s*#?\s*:?\s*(\d{4})\s*[-/\s]\s*(\d+)/i;
    const manRegMatch = (email.body && manRegPattern.exec(email.body))
      || (email.subject && manRegPattern.exec(email.subject));
    const emailManReg = manRegMatch ? `${manRegMatch[1]} ${manRegMatch[2]}` : undefined;

    progress(`Waybill: ${waybill}${emailManReg ? `, MNF#: ${emailManReg}` : ''}`);

    // Save the main PDF
    const mainPdf = pdfAttachments[0];
    const pdfFilename = mainPdf.filename || `${waybill}_document.pdf`;
    const pdfPath = path.join(workDir, pdfFilename);
    fs.writeFileSync(pdfPath, mainPdf.content);
    progress(`Saved PDF: ${pdfPath}`);

    // Step 2: Split PDF
    progress('Splitting PDF…');
    const splitResult = await splitPdf(pdfPath, workDir);
    if (!splitResult.success) throw new Error(`PDF split failed: ${splitResult.error}`);
    progress(`Split complete: ${splitResult.total_pages ?? '?'} pages`);

    // Step 3: Extract declaration data
    const declarationData = extractDeclarationFromResult(splitResult);
    declarationData.waybill = declarationData.waybill || waybill;

    // Step 4: Run invoice pipeline
    const invoicePath = (splitResult as any).output_files?.invoice;
    let xlsxPath: string | undefined;
    let varianceCheck: number | undefined;
    let xlsxTotals: XlsxTotals | undefined;

    if (invoicePath && fs.existsSync(invoicePath)) {
      progress('Running pipeline on invoice…');
      const pipelineResult = await runPipeline(invoicePath, workDir);
      xlsxPath = pipelineResult.xlsxPath;
      varianceCheck = pipelineResult.varianceCheck;
      xlsxTotals = pipelineResult.xlsxTotals;

      if (xlsxPath) {
        progress(`Pipeline done: ${xlsxPath} (variance: $${varianceCheck?.toFixed(2) ?? 'N/A'})`);
      } else {
        progress('Pipeline produced no output (may need manual processing)');
      }
    }

    // Step 5: Variance check + LLM fix
    let varianceOk = varianceCheck === undefined || Math.abs(varianceCheck) < 0.01;
    let llmFixAttempted = false;

    if (!varianceOk && xlsxPath && invoicePath) {
      progress(`Variance $${varianceCheck?.toFixed(2)} — invoking LLM fix…`);
      llmFixAttempted = true;
      const llmResult = await invokeLlmVarianceFix(xlsxPath, invoicePath, varianceCheck!, progress);

      if (llmResult.success && llmResult.newVariance !== undefined) {
        varianceCheck = llmResult.newVariance;
        varianceOk = Math.abs(varianceCheck) < 0.01;
        progress(`LLM fix ${varianceOk ? 'SUCCEEDED' : 'FAILED'}: variance $${varianceCheck.toFixed(2)}`);
      } else {
        if (llmResult.newVariance !== undefined) varianceCheck = llmResult.newVariance;
        progress(`LLM fix failed: ${llmResult.error ?? 'unknown'}`);
      }
    }

    // If variance still bad, mark for review
    if (!varianceOk) {
      progress(`SKIPPING email — variance $${varianceCheck?.toFixed(2)} not resolved`);
      updateProcessedEmail(recordId, {
        status: 'needs_review',
        waybillNumber: declarationData.waybill,
        outputFiles: xlsxPath ? [xlsxPath] : [],
        error: `Variance $${varianceCheck?.toFixed(2)} — ${llmFixAttempted ? 'LLM could not fix' : 'needs correction'}`,
      });
      return {
        success: true,
        waybill: declarationData.waybill,
        outputFiles: xlsxPath ? [xlsxPath] : [],
        declarationData,
        varianceCheck,
        emailSkipped: true,
        skipReason: `Variance $${varianceCheck?.toFixed(2)}`,
      };
    }

    // Step 6: Send email
    progress('Variance OK — sending output email…');
    const outputFiles: string[] = [];

    if ((splitResult as any).output_files?.declaration) {
      const declPath = (splitResult as any).output_files.declaration;
      if (fs.existsSync(declPath)) {
        const dest = path.join(workDir, path.basename(declPath));
        fs.copyFileSync(declPath, dest);
        outputFiles.push(dest);
      }
    }
    if (invoicePath && fs.existsSync(invoicePath)) {
      const dest = path.join(workDir, path.basename(invoicePath));
      fs.copyFileSync(invoicePath, dest);
      outputFiles.push(dest);
    }
    if (xlsxPath && fs.existsSync(xlsxPath)) {
      outputFiles.push(xlsxPath);
    }

    const emailResult = await sendShipmentEmail({
      waybill: declarationData.waybill || waybill,
      manReg: declarationData.manifestRegistry || emailManReg,
      consignee: declarationData.consignee,
      packages: declarationData.packages,
      weight: declarationData.grossWeight,
      countryOfOrigin: declarationData.countryOfOrigin,
      freight: declarationData.freight || xlsxTotals?.freight?.toFixed(2),
      attachments: outputFiles,
    }, progress);

    if (!emailResult.success) progress(`Email send failed: ${emailResult.error}`);
    else progress('Email sent successfully');

    updateProcessedEmail(recordId, {
      status: 'completed',
      waybillNumber: declarationData.waybill,
      outputFiles,
    });

    return { success: true, waybill: declarationData.waybill, outputFiles, declarationData };
  } catch (err) {
    const error = err instanceof Error ? err : new Error(String(err));
    log(`Error: ${error.message}`);

    updateProcessedEmail(recordId, {
      status: 'error',
      error: error.message,
    });

    try {
      await sendErrorNotification(client, error, `Processing email: ${email.subject}`);
    } catch { /* notification failure is non-fatal */ }

    return { success: false, error: error.message };
  }
}

// ---------------------------------------------------------------------------
// 2. runBLPipeline
// ---------------------------------------------------------------------------

/**
 * Run the unified pipeline on a folder (auto-detects BL mode).
 * Called when the documents client downloads a BL email.
 */
export async function runBLPipeline(
  inputDir: string,
  logProgress?: (msg: string) => void,
  blNumber?: string,
): Promise<BLPipelineResult> {
  const scriptPath = path.join(pipelineDir(), 'run.py');

  // Determine output dir
  const dirName = path.basename(inputDir);
  const outputDir = dirName.startsWith('Shipment_')
    ? inputDir
    : path.join(shipmentsPath(), `Shipment_ ${dirName}`);

  if (!fs.existsSync(scriptPath)) {
    return { success: false, error: 'pipeline/run.py not found' };
  }

  logProgress?.(`Starting BL pipeline: ${inputDir}`);

  const args = ['--input-dir', inputDir, '--output-dir', outputDir, '--json-output', '--send-email'];
  if (blNumber) {
    args.push('--bl', blNumber);
    logProgress?.(`Using BL number: ${blNumber}`);
  }

  const { code, stdout, stderr } = await spawnPython(
    scriptPath,
    args,
    pipelineDir(),
    (line) => {
      if (!line.startsWith('REPORT:JSON:')) logProgress?.(line);
    },
  );

  if (code !== 0) {
    logProgress?.(`BL pipeline failed (exit ${code}): ${stderr.slice(0, 200)}`);
    return { success: false, error: stderr || `Process exited with code ${code}` };
  }

  const report = extractJsonReport(stdout);
  if (!report) return { success: true };

  const outputFiles: string[] = [];
  for (const inv of report.invoices || []) {
    if (inv.xlsx_path) outputFiles.push(inv.xlsx_path);
    if (inv.pdf_path) outputFiles.push(inv.pdf_path);
  }
  if (report.bl?.bl_pdf) outputFiles.push(report.bl.bl_pdf);

  return {
    success: true,
    mode: report.mode,
    invoiceCount: report.invoice_count,
    bl: report.bl ? {
      blNumber: report.bl.bl_number,
      freight: report.bl.freight,
      packages: report.bl.packages,
      weight: report.bl.weight,
      insurance: report.bl.insurance,
      blPdf: report.bl.bl_pdf,
    } : undefined,
    emailSent: report.email_sent,
    emailParamsPath: report.email_params_path,
    outputFiles,
    failures: report.failures,
    validation: report.validation,
    checklist: report.checklist,
  };
}

// ---------------------------------------------------------------------------
// 3. sendShipmentEmailFromParams
// ---------------------------------------------------------------------------

/**
 * Send shipment email using saved _email_params.json.
 * Uses send_shipment_email.py which reads the params file.
 */
export async function sendShipmentEmailFromParams(
  paramsPath: string,
  logProgress?: (msg: string) => void,
): Promise<{ success: boolean; error?: string }> {
  const scriptPath = path.join(pipelineDir(), 'send_shipment_email.py');

  if (!fs.existsSync(scriptPath)) {
    return { success: false, error: 'send_shipment_email.py not found' };
  }
  if (!fs.existsSync(paramsPath)) {
    return { success: false, error: `Email params file not found: ${paramsPath}` };
  }

  logProgress?.(`Sending email from params: ${paramsPath}`);

  const { code, stdout } = await spawnPython(
    scriptPath,
    ['--params', paramsPath, '--json-output'],
    pipelineDir(),
    (line) => {
      if (!line.startsWith('REPORT:JSON:')) logProgress?.(line);
    },
  );

  const report = extractJsonReport(stdout);
  if (report) {
    return { success: report.email_sent === true, error: report.email_sent ? undefined : 'Email send failed' };
  }
  return {
    success: code === 0 && stdout.includes('Email sent:'),
    error: code !== 0 ? 'Process failed' : undefined,
  };
}

// ---------------------------------------------------------------------------
// 4. classifyEmailDir
// ---------------------------------------------------------------------------

/**
 * Classify all PDFs in an email directory.
 * Returns classification (BL, invoice, declaration, etc.) and BL metadata.
 */
export async function classifyEmailDir(
  inputDir: string,
): Promise<ClassificationResult> {
  const scriptPath = path.join(pipelineDir(), 'classify_pdfs.py');

  const emptyResult: ClassificationResult = {
    classification: { bill_of_lading: [], manifest: [], declaration: [], invoice: [], packing_list: [], unknown: [] },
    has_bl: false,
    has_invoices: false,
    bl_metadata: { bl_number: '', consignee: '', invoice_refs: [], shipper_names: [] },
  };

  if (!fs.existsSync(scriptPath)) {
    log('classify_pdfs.py not found');
    return emptyResult;
  }

  const { code, stdout } = await spawnPython(
    scriptPath,
    ['--dir', inputDir],
    pipelineDir(),
  );

  if (code === 0 && stdout.trim()) {
    try {
      return JSON.parse(stdout.trim());
    } catch {
      log(`classify_pdfs JSON parse failed: ${stdout.slice(0, 200)}`);
    }
  }

  return emptyResult;
}

// ---------------------------------------------------------------------------
// 5. llmMatchEmails
// ---------------------------------------------------------------------------

/**
 * Use the LLM to match a BL email to the correct invoice email.
 * Returns the record ID of the best match, or null if none found.
 */
export async function llmMatchEmails(
  blEmailBody: string,
  blSubject: string,
  blMetadata: BLMetadata,
  candidates: { id: string; subject: string; body: string; receivedAt: string }[],
): Promise<string | null> {
  if (candidates.length === 0) return null;

  const settings = loadSettings();
  const apiKey = (settings as any)?.apiKey;
  if (!apiKey) {
    log('No API key — cannot use LLM for email matching');
    return null;
  }

  const candidateDescriptions = candidates
    .map(
      (c, i) =>
        `--- Candidate #${i + 1} ---\nSubject: ${c.subject}\nReceived: ${c.receivedAt}\nBody:\n${c.body.slice(0, 3000)}\n`,
    )
    .join('\n');

  const blInfo = [
    blMetadata.bl_number ? `BL Number: ${blMetadata.bl_number}` : '',
    blMetadata.consignee ? `Consignee: ${blMetadata.consignee}` : '',
    blMetadata.shipper_names.length > 0 ? `Shipper: ${blMetadata.shipper_names.join(', ')}` : '',
  ]
    .filter(Boolean)
    .join('\n');

  const userMessage = `I have a Bill of Lading (BL) email and need to find which candidate email contains the corresponding invoices for that same shipment.

BL EMAIL:
Subject: ${blSubject}
${blInfo ? `\nExtracted BL Data:\n${blInfo}\n` : ''}
Body:
${blEmailBody.slice(0, 3000)}

CANDIDATE EMAILS (possible invoice matches):
${candidateDescriptions}

Which candidate email contains the invoices/documents for this Bill of Lading shipment?

RULES:
1. The correct email should reference the same consignee, supplier, or shipping company
2. The correct email should have been sent BEFORE the BL ship/sailing date
3. The BL email explicitly says "this is the bill of laden for the last container email previously sent" — so the match is the most recent container/shipment/document email sent before this BL
4. Look for matching company names, shipping references, container names
5. If the BL is from Tropical Shipping to Budget Marine Grenada, find the invoice email that has Budget Marine invoices

Reply with ONLY the candidate number (e.g. "1" or "3") or "NONE" if none match.`;

  try {
    log(`LLM matching: ${candidates.length} candidates, BL subject="${blSubject}"`);

    const Anthropic = require('@anthropic-ai/sdk');
    const client = new Anthropic.default({
      apiKey,
      baseURL: (settings as any).baseUrl || 'https://api.anthropic.com',
    });

    const response = await client.messages.create({
      model: (settings as any).model || 'claude-sonnet-4-20250514',
      max_tokens: 256,
      system:
        'You are a shipping logistics assistant. You match Bill of Lading emails to their corresponding invoice/document emails. Reply with ONLY the candidate number (e.g. "1") or "NONE".',
      messages: [{ role: 'user', content: userMessage }],
    });

    const answer = (response.content[0]?.text || '').trim();
    log(`LLM match result: "${answer}"`);

    if (answer.toUpperCase() === 'NONE' || !answer) return null;

    const numMatch = answer.match(/(\d+)/);
    if (numMatch) {
      const idx = parseInt(numMatch[1], 10) - 1;
      if (idx >= 0 && idx < candidates.length) {
        log(`LLM matched candidate #${idx + 1}: "${candidates[idx].subject}"`);
        return candidates[idx].id;
      }
    }

    return candidates.find((c) => answer.includes(c.id))?.id ?? null;
  } catch (err) {
    log(`LLM email matching failed: ${(err as Error).message}`);
    return null;
  }
}

// ---------------------------------------------------------------------------
// 6. createCombinedFolder
// ---------------------------------------------------------------------------

/**
 * Create a combined folder from two email directories (BL + invoices).
 * Copies files from both source folders into a new combined folder.
 */
export function createCombinedFolder(
  prefix: string,
  blDir: string,
  invoiceDir: string,
  blNumber: string,
): string {
  const timestamp = Date.now().toString(36);
  const folderName = `Combined_${blNumber || 'shipment'}_${timestamp}`;
  const combinedDir = path.join(workspacePath(), prefix, folderName);

  fs.mkdirSync(combinedDir, { recursive: true });

  const copyDir = (srcDir: string, label: string) => {
    if (!fs.existsSync(srcDir)) return;
    for (const file of fs.readdirSync(srcDir)) {
      const srcPath = path.join(srcDir, file);
      if (!fs.statSync(srcPath).isFile()) continue;

      // Rename email.txt to preserve both sources
      const destName = file === 'email.txt' ? `email_${label}.txt` : file;
      const destPath = path.join(combinedDir, destName);

      if (fs.existsSync(destPath)) {
        // Collision: prefix with label
        const ext = path.extname(file);
        const base = path.basename(file, ext);
        fs.copyFileSync(srcPath, path.join(combinedDir, `${base}_${label}${ext}`));
      } else {
        fs.copyFileSync(srcPath, destPath);
      }
    }
  };

  copyDir(invoiceDir, 'invoices');
  copyDir(blDir, 'bl');

  // Write hint file so the pipeline knows which PDFs are the BL
  const blPdfs = fs.readdirSync(blDir).filter((f) => f.toLowerCase().endsWith('.pdf'));
  if (blPdfs.length > 0) {
    fs.writeFileSync(
      path.join(combinedDir, '_bl_hint.json'),
      JSON.stringify({ bl_source_dir: path.basename(blDir), bl_pdfs: blPdfs, bl_number: blNumber }, null, 2),
    );
  }

  log(`Created combined folder: ${combinedDir}`);
  return combinedDir;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/** Split a PDF using the Python script. */
async function splitPdf(pdfPath: string, outputDir: string): Promise<PdfSplitResult> {
  const scriptPath = path.join(pipelineDir(), 'pdf_splitter.py');

  if (!fs.existsSync(scriptPath)) {
    return { success: false, error: 'pdf_splitter.py not found' };
  }

  const { code, stdout, stderr } = await spawnPython(
    scriptPath,
    [pdfPath, '--output-dir', outputDir],
    pipelineDir(),
  );

  if (code === 0 && stdout) {
    try {
      const result = JSON.parse(stdout.trim());
      return { success: result.status === 'success', ...result };
    } catch {
      return { success: false, error: `Parse error: ${stdout.slice(0, 200)}` };
    }
  }
  return { success: false, error: stderr || `Exit code ${code}` };
}

/** Run the invoice through the pipeline. */
async function runPipeline(invoicePath: string, outputDir?: string): Promise<PipelineResult> {
  const scriptPath = path.join(pipelineDir(), 'pipeline_runner.py');

  if (!fs.existsSync(scriptPath)) {
    return { success: false, error: 'pipeline_runner.py not found' };
  }

  const inputBase = path.basename(invoicePath, path.extname(invoicePath));
  const targetDir = outputDir || shipmentsPath();
  fs.mkdirSync(targetDir, { recursive: true });
  const outputPath = path.join(targetDir, `${inputBase}.xlsx`);

  const { code, stdout, stderr } = await spawnPython(
    scriptPath,
    ['--input', invoicePath, '--output', outputPath, '--json-output'],
    pipelineDir(),
  );

  if (code === 0 && fs.existsSync(outputPath)) {
    let varianceCheck: number | undefined;
    let xlsxTotals: XlsxTotals | undefined;

    try {
      const jsonMatch = stdout.match(/\{[\s\S]*"status"\s*:\s*"(?:success|completed)"[\s\S]*\}/);
      if (jsonMatch) {
        const result = JSON.parse(jsonMatch[0]);
        if (result.stages) {
          for (const stage of result.stages) {
            if (stage.name === 'generate_xlsx') {
              if (typeof stage.variance_check === 'number') varianceCheck = stage.variance_check;
              xlsxTotals = {
                invoiceTotal: stage.invoice_total,
                freight: stage.freight,
                insurance: stage.insurance,
                otherCost: stage.other_cost,
                netTotal: stage.net_total,
              };
              break;
            }
          }
        }
        if (varianceCheck === undefined && typeof result.variance_check === 'number') {
          varianceCheck = result.variance_check;
        }
      }
    } catch { /* ignore parse errors */ }

    return { success: true, xlsxPath: outputPath, varianceCheck, xlsxTotals };
  }

  return { success: false, error: stderr || `Exit code ${code}` };
}

/** Extract declaration data from PDF split result. */
function extractDeclarationFromResult(result: any): DeclarationData {
  const meta = result.declaration_metadata || {};
  return {
    waybill: meta.waybill,
    customsFile: meta.customs_file,
    manifestRegistry: meta.man_reg,
    consignee: meta.consignee,
    packages: meta.packages,
    grossWeight: meta.weight,
    countryOfOrigin: meta.country_origin,
    fobValue: meta.fob_value,
  };
}

/**
 * Send a shipment email via run.py --send-email-only.
 * Single code path for all outgoing shipment emails.
 */
async function sendShipmentEmail(
  params: {
    waybill: string;
    consignee?: string;
    consigneeCode?: string;
    packages?: string;
    weight?: string;
    countryOfOrigin?: string;
    freight?: string;
    manReg?: string;
    location?: string;
    office?: string;
    totalInvoices?: number;
    attachments: string[];
  },
  logProgress?: (msg: string) => void,
): Promise<{ success: boolean; error?: string }> {
  const scriptPath = path.join(pipelineDir(), 'run.py');

  if (!fs.existsSync(scriptPath)) {
    return { success: false, error: 'pipeline/run.py not found' };
  }

  const args = ['--send-email-only', '--waybill', params.waybill || 'UNKNOWN', '--json-output'];

  if (params.consignee) args.push('--consignee', params.consignee);
  if (params.consigneeCode) args.push('--consignee-code', params.consigneeCode);
  if (params.packages) args.push('--packages', params.packages);
  if (params.weight) args.push('--weight', params.weight);
  if (params.countryOfOrigin) args.push('--country-origin', params.countryOfOrigin);
  if (params.freight) args.push('--freight', params.freight);
  if (params.manReg) args.push('--man-reg', params.manReg);
  if (params.location) args.push('--location', params.location);
  if (params.office) args.push('--office', params.office);
  if (params.totalInvoices) args.push('--total-invoices', String(params.totalInvoices));

  const existingAttachments = params.attachments.filter((f) => fs.existsSync(f));
  if (existingAttachments.length > 0) {
    args.push('--attachments', existingAttachments.join(','));
  }

  logProgress?.(`Sending email via run.py for waybill ${params.waybill}`);

  const { code, stdout } = await spawnPython(scriptPath, args, pipelineDir());

  const report = extractJsonReport(stdout);
  if (report) {
    return { success: report.email_sent === true };
  }
  return { success: code === 0 && stdout.includes('Email sent:') };
}

/**
 * Invoke LLM to fix variance issues in the XLSX.
 * Returns the new variance after the LLM attempts corrections.
 */
async function invokeLlmVarianceFix(
  xlsxPath: string,
  invoicePath: string,
  currentVariance: number,
  logProgress?: (msg: string) => void,
): Promise<{ success: boolean; newVariance?: number; error?: string }> {
  const settings = loadSettings();
  const apiKey = (settings as any)?.apiKey;

  if (!apiKey) {
    return { success: false, error: 'No API key configured for LLM' };
  }

  logProgress?.(`Invoking LLM to fix variance $${currentVariance.toFixed(2)}…`);

  try {
    // Lazy import to avoid hard dependency
    const { LlmClient } = require('./llm-client');
    const { getAgentTools, executeAgentTool } = require('./agent-tools');

    const zaiClient = new LlmClient({
      apiKey,
      baseUrl: (settings as any).baseUrl || 'https://api.anthropic.com',
      model: (settings as any).model || 'claude-sonnet-4-20250514',
    });
    const tools = getAgentTools();

    const systemPrompt = `You are an invoice processing assistant. Fix the variance in the XLSX file.
XLSX: ${xlsxPath}
Invoice PDF: ${invoicePath}
Current variance: $${currentVariance.toFixed(2)}
Target: $0.00

Steps: validate_xlsx → read_file → compare → fix → validate_xlsx again.`;

    const userMessage = `Fix the variance in ${xlsxPath}. Current variance is $${currentVariance.toFixed(2)}.`;
    const messages = [{ role: 'user' as const, content: userMessage }];

    let finalVariance: number | undefined;

    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('LLM variance fix timed out (120s)')), 120_000);

      zaiClient.streamMessage(
        systemPrompt,
        messages,
        tools,
        {
          onText: (text: string) => {
            if (text.includes('ariance')) logProgress?.(`LLM: ${text.slice(0, 100)}…`);
          },
          onToolUse: async (toolUse: any) => {
            logProgress?.(`LLM tool: ${toolUse.name}`);
            const result = await executeAgentTool(toolUse.name, toolUse.input, baseDir());
            if (toolUse.name === 'validate_xlsx' && result?.checks?.variance_check !== undefined) {
              finalVariance = result.checks.variance_check;
              logProgress?.(`validate_xlsx: variance $${finalVariance!.toFixed(2)}`);
            }
            return result;
          },
          onEnd: () => { clearTimeout(timeout); resolve(); },
          onError: (error: string) => { clearTimeout(timeout); reject(new Error(error)); },
        },
      );
    });

    if (finalVariance !== undefined && Math.abs(finalVariance) < 0.01) {
      return { success: true, newVariance: finalVariance };
    }

    if (finalVariance !== undefined) {
      return { success: false, newVariance: finalVariance, error: `Variance still $${finalVariance.toFixed(2)}` };
    }

    // Final validation
    const { executeAgentTool: execTool } = require('./agent-tools');
    const validateResult = await execTool('validate_xlsx', { path: xlsxPath }, baseDir());
    const newVariance = validateResult?.checks?.variance_check;

    if (newVariance !== undefined && Math.abs(newVariance) < 0.01) {
      return { success: true, newVariance };
    }
    return { success: false, newVariance, error: 'LLM did not report final variance' };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    logProgress?.(`LLM variance fix error: ${msg}`);
    return { success: false, error: msg };
  }
}

/** Parse LLM instructions from email body. */
export function parseLlmInstructions(emailBody: string): {
  hasInstructions: boolean;
  instructions: string[];
} {
  const instructions: string[] = [];
  const patterns = [
    /instructions?:\s*(.+?)(?:\n|$)/gi,
    /please\s+(.+?)(?:\.|$)/gi,
    /note:\s*(.+?)(?:\n|$)/gi,
  ];

  for (const pattern of patterns) {
    for (const match of emailBody.matchAll(pattern)) {
      const instruction = match[1].trim();
      if (instruction.length > 5) instructions.push(instruction);
    }
  }

  return { hasInstructions: instructions.length > 0, instructions };
}
