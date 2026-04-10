/**
 * Agent Tools Service
 *
 * Defines all tools available to the Claude agent for CARICOM invoice processing.
 * Each tool has a schema definition (for the LLM) and an execution handler.
 *
 * Tools are organized into categories:
 * - Pipeline: run_pipeline, reclassify_items, extract_with_ocr, compare_ocr_results
 * - File operations: read_file, write_file, edit_file, list_files
 * - Rules: query_rules, update_rules
 * - XLSX: validate_xlsx, verify_line_count, edit_xlsx_cell
 * - Tariff/CET: lookup_tariff, web_search, add_cet_entry, cet_stats, get_unknown_products
 * - PDF: split_pdf
 * - Email: get_email_clients, parse_email_instructions, fetch_emails,
 *          read_processed_email, download_attachment, send_email,
 *          send_shipment_email, reprocess_email
 * - Chat: search_chat_history
 */

import fs from 'fs';
import path from 'path';
import * as XLSX from 'xlsx';
import {
  baseDir,
  workspacePath,
  outputPath,
  rulesPath,
  dataPath,
  classificationRulesPath,
} from '../utils/paths';
import { PythonBridge } from './python-bridge';
import { webSearch } from './web-search';

// ─── Types ──────────────────────────────────────────────────────

export interface ToolDefinition {
  name: string;
  description: string;
  input_schema: Record<string, unknown>;
}

type ToolInput = Record<string, unknown>;
type ToolResult = unknown;

interface FileEntry {
  name: string;
  type: 'directory' | 'file';
  size?: number;
}

interface CellEdit {
  row: number;
  col: number | string;
  value: unknown;
  sheet?: string;
}

// ─── Tool Definitions ───────────────────────────────────────────

const TOOL_DEFINITIONS: readonly ToolDefinition[] = [
  {
    name: 'run_pipeline',
    description:
      'Execute the CARICOM invoice processing pipeline. Can run the full pipeline or a specific stage. Returns a JSON report with stage results, errors, and warnings.',
    input_schema: {
      type: 'object',
      properties: {
        input_file: {
          type: 'string',
          description: 'Path to input PDF file',
        },
        output_file: {
          type: 'string',
          description: 'Path for output XLSX file (optional, auto-generated if omitted)',
        },
        stage: {
          type: 'string',
          description: 'Run only this stage (extract, parse, classify, validate_codes, group, generate_xlsx, verify, learn)',
        },
      },
      required: ['input_file'],
    },
  },
  {
    name: 'read_file',
    description:
      'Read the contents of a file with line numbers. Paths starting with "pipeline/", "config/", or "rules/" resolve relative to the project root. Other relative paths resolve to workspace/. ' +
      'Use for diagnosing parsing failures by reading pipeline/text_parser.py or input files. ' +
      'For large files, use offset and limit to read specific line ranges. Returns metadata: total_lines, truncated, next_offset.',
    input_schema: {
      type: 'object',
      properties: {
        path: {
          type: 'string',
          description: 'Path to the file (e.g. "pipeline/text_parser.py", "config/pipeline.yaml", "input/2026-02-05/file.txt", or absolute)',
        },
        offset: {
          type: 'number',
          description: 'Start reading from this line number (1-based). Default: 1',
        },
        limit: {
          type: 'number',
          description: 'Maximum number of lines to return. Default: 500',
        },
      },
      required: ['path'],
    },
  },
  {
    name: 'write_file',
    description:
      'Write content to a file. Use for updating pipeline scripts, rules, configs, or other text files. Paths starting with "pipeline/", "config/", or "rules/" resolve relative to the project root. Creates parent directories if needed.',
    input_schema: {
      type: 'object',
      properties: {
        path: {
          type: 'string',
          description: 'Path to write to (e.g. "pipeline/text_parser.py", "rules/classification_rules.json", or absolute)',
        },
        content: {
          type: 'string',
          description: 'Content to write',
        },
      },
      required: ['path', 'content'],
    },
  },
  {
    name: 'edit_file',
    description:
      'Make a surgical edit to an existing file by replacing a specific text block. ' +
      'Use this instead of write_file when modifying existing files — it avoids truncation issues with large files. ' +
      'The old_text must match EXACTLY one location in the file (including whitespace and indentation). ' +
      'For multiple edits to the same file, call edit_file multiple times.',
    input_schema: {
      type: 'object',
      properties: {
        path: {
          type: 'string',
          description: 'Path to the file to edit (same resolution rules as read_file/write_file)',
        },
        old_text: {
          type: 'string',
          description: 'The exact text to find in the file. Must match uniquely (only one occurrence). Include enough surrounding context (2-3 lines) to ensure uniqueness.',
        },
        new_text: {
          type: 'string',
          description: 'The replacement text. Use empty string to delete the old_text.',
        },
      },
      required: ['path', 'old_text', 'new_text'],
    },
  },
  {
    name: 'list_files',
    description:
      'List files and directories at the given path. Returns file names, types, and sizes. Paths starting with "pipeline/", "config/", or "rules/" resolve relative to the project root.',
    input_schema: {
      type: 'object',
      properties: {
        path: {
          type: 'string',
          description: 'Directory path to list (e.g. "pipeline/", "config/", or relative to workspace)',
        },
      },
      required: ['path'],
    },
  },
  {
    name: 'query_rules',
    description:
      'Search classification_rules.json for rules matching a pattern, code, or category. Returns matching rules with their details.',
    input_schema: {
      type: 'object',
      properties: {
        query: {
          type: 'string',
          description: 'Search term to match against rule patterns, codes, or categories',
        },
        field: {
          type: 'string',
          description: 'Field to search in: patterns, code, category, or all (default: all)',
        },
      },
      required: ['query'],
    },
  },
  {
    name: 'update_rules',
    description:
      'Add, modify, or delete classification rules in classification_rules.json. Backs up the file before changes.',
    input_schema: {
      type: 'object',
      properties: {
        action: {
          type: 'string',
          enum: ['add', 'modify', 'delete'],
          description: 'Action to perform',
        },
        rule_id: {
          type: 'string',
          description: 'ID of rule to modify or delete',
        },
        rule: {
          type: 'object',
          description: 'Rule data for add/modify. Must include: id, code, category, patterns',
        },
      },
      required: ['action'],
    },
  },
  {
    name: 'validate_xlsx',
    description:
      'Run validation checks on an XLSX file. Checks variance, group verification, formula errors, and structure.',
    input_schema: {
      type: 'object',
      properties: {
        path: {
          type: 'string',
          description: 'Path to XLSX file to validate',
        },
      },
      required: ['path'],
    },
  },
  {
    name: 'verify_line_count',
    description:
      'Verify that the number of line items in the source text/JSON matches the number of detail rows in the generated XLSX. Use this to ensure all invoice items were extracted correctly.',
    input_schema: {
      type: 'object',
      properties: {
        source_file: {
          type: 'string',
          description: 'Path to source file (text invoice or parsed/grouped JSON)',
        },
        xlsx_file: {
          type: 'string',
          description: 'Path to generated XLSX file to verify',
        },
      },
      required: ['source_file', 'xlsx_file'],
    },
  },
  {
    name: 'reclassify_items',
    description:
      'Re-run classification for specific item indices in the pipeline. Useful after updating rules.',
    input_schema: {
      type: 'object',
      properties: {
        input_file: {
          type: 'string',
          description: 'Path to parsed items JSON',
        },
        item_indices: {
          type: 'array',
          items: { type: 'number' },
          description: 'Indices of items to reclassify',
        },
      },
      required: ['input_file', 'item_indices'],
    },
  },
  {
    name: 'lookup_tariff',
    description:
      'Search the local CARICOM CET (Common External Tariff) database for tariff codes. Supports fuzzy description search, exact HS code lookup, and chapter filtering. Use this FIRST before web_search — it is instant and offline. Returns matching codes with descriptions, duty rates, and aliases.',
    input_schema: {
      type: 'object',
      properties: {
        query: {
          type: 'string',
          description: 'Search term to match against CET descriptions and aliases (e.g. "shampoo", "face wash")',
        },
        code: {
          type: 'string',
          description: 'Exact 8-digit HS code to look up (e.g. "33051000")',
        },
        chapter: {
          type: 'number',
          description: 'Filter results to a specific HS chapter (e.g. 33 for cosmetics, 34 for soap)',
        },
      },
      required: [],
    },
  },
  {
    name: 'web_search',
    description:
      'Search the internet for tariff classification information. Use this when lookup_tariff returns no results and you need to research a product description to find the correct HS code. Query should include terms like "HS code", "tariff classification", "CARICOM" for best results.',
    input_schema: {
      type: 'object',
      properties: {
        query: {
          type: 'string',
          description: 'Search query (e.g. "HS code tariff classification face wash CARICOM CET")',
        },
      },
      required: ['query'],
    },
  },
  {
    name: 'add_cet_entry',
    description:
      'Add or update an entry in the local CET database. Use this after finding a correct tariff code via web_search to cache it locally for future lookups.',
    input_schema: {
      type: 'object',
      properties: {
        hs_code: {
          type: 'string',
          description: '8-digit HS code (e.g. "34011910")',
        },
        description: {
          type: 'string',
          description: 'Official goods description',
        },
        duty_rate: {
          type: 'string',
          description: 'Duty rate percentage (e.g. "20%")',
        },
        unit: {
          type: 'string',
          description: 'Unit of measure (e.g. "KG", "L", "U")',
        },
        notes: {
          type: 'string',
          description: 'Additional notes about this code',
        },
        aliases: {
          type: 'array',
          items: { type: 'string' },
          description: 'Alternative names/patterns for fuzzy matching (e.g. ["FACE WASH", "FACIAL CLEANSER"])',
        },
      },
      required: ['hs_code', 'description'],
    },
  },
  {
    name: 'cet_stats',
    description:
      'Get statistics about the local CET database: number of codes, aliases, and chapter coverage.',
    input_schema: {
      type: 'object',
      properties: {},
      required: [],
    },
  },
  {
    name: 'search_chat_history',
    description:
      'Search past chat conversations. Use with no arguments to list all conversations. Use with a query to search message content across all conversations. Use with a conversation_id to read a specific conversation\'s messages. Useful for referencing previous user requests, past pipeline results, or earlier discussions.',
    input_schema: {
      type: 'object',
      properties: {
        query: {
          type: 'string',
          description: 'Text to search for across all conversations (e.g. "INVOICE#307500", "classification", "variance")',
        },
        conversation_id: {
          type: 'string',
          description: 'ID of a specific conversation to read its full messages',
        },
        limit: {
          type: 'number',
          description: 'Maximum number of results to return (default: 50)',
        },
      },
      required: [],
    },
  },
  {
    name: 'extract_with_ocr',
    description:
      'Extract text and data from a PDF using a specific OCR method. Use this to compare extraction quality between methods when investigating discrepancies. ' +
      'Available methods: "pdfplumber" (default, good for embedded text/tables), "pymupdf" (fast text extraction), "tesseract" (for scanned/image PDFs). ' +
      'Output files are named with the OCR method for easy comparison (e.g., extracted_pdfplumber.json, extracted_tesseract.json).',
    input_schema: {
      type: 'object',
      properties: {
        input_file: {
          type: 'string',
          description: 'Path to PDF file to extract from',
        },
        ocr_method: {
          type: 'string',
          enum: ['pdfplumber', 'pymupdf', 'tesseract'],
          description: 'OCR method to use: pdfplumber (default), pymupdf, or tesseract',
        },
        skip_txt_fallback: {
          type: 'boolean',
          description: 'If true, do not use existing .txt file even if present. Forces fresh OCR extraction.',
        },
      },
      required: ['input_file'],
    },
  },
  {
    name: 'compare_ocr_results',
    description:
      'Compare extraction results from multiple OCR methods. Provide paths to extracted JSON files and get a detailed diff of items, totals, and metadata. ' +
      'Useful for identifying which OCR method produces more accurate results for a specific invoice.',
    input_schema: {
      type: 'object',
      properties: {
        file_a: {
          type: 'string',
          description: 'Path to first extraction result JSON (e.g., extracted_pdfplumber.json)',
        },
        file_b: {
          type: 'string',
          description: 'Path to second extraction result JSON (e.g., extracted_tesseract.json)',
        },
      },
      required: ['file_a', 'file_b'],
    },
  },
  {
    name: 'get_unknown_products',
    description:
      'Analyze classified output and identify UNKNOWN products that need tariff code research. ' +
      'Groups similar products by type and suggests web search queries for each. ' +
      'Use this after a pipeline run shows items_unmatched > 0 to get a list of products to research.',
    input_schema: {
      type: 'object',
      properties: {
        classified_file: {
          type: 'string',
          description: 'Path to the classified.json file (default: auto-detect from latest pipeline run)',
        },
      },
      required: [],
    },
  },
  {
    name: 'split_pdf',
    description:
      'Split a combined PDF (Invoice + Simplified Declaration) into separate documents. ' +
      'Uses content detection to identify which pages are declaration vs invoice. ' +
      'Also extracts key metadata from the Simplified Declaration (waybill, consignee, packages, weight, FOB value).',
    input_schema: {
      type: 'object',
      properties: {
        pdf_path: {
          type: 'string',
          description: 'Path to the combined PDF file to split',
        },
        output_dir: {
          type: 'string',
          description: 'Directory to write split PDFs (default: same as input)',
        },
      },
      required: ['pdf_path'],
    },
  },
  {
    name: 'get_email_clients',
    description:
      'List all configured email processing clients. Returns client name, email addresses, enabled status, and last activity.',
    input_schema: {
      type: 'object',
      properties: {},
      required: [],
    },
  },
  {
    name: 'parse_email_instructions',
    description:
      'Parse an email body to extract any special processing instructions. Returns structured instructions for handling the shipment documents.',
    input_schema: {
      type: 'object',
      properties: {
        email_body: {
          type: 'string',
          description: 'The email body text to parse for instructions',
        },
      },
      required: ['email_body'],
    },
  },
  {
    name: 'fetch_emails',
    description:
      'Fetch processed email records for a configured email client. Returns a list of emails with subject, from, date, status, waybill, and output files. ' +
      'Use get_email_clients first to find the client ID.',
    input_schema: {
      type: 'object',
      properties: {
        client_id: {
          type: 'string',
          description: 'ID of the email client to fetch from',
        },
        limit: {
          type: 'number',
          description: 'Maximum number of emails to return (default: 20)',
        },
        status_filter: {
          type: 'string',
          enum: ['all', 'pending', 'success', 'error', 'processing'],
          description: 'Filter by processing status (default: all)',
        },
      },
      required: ['client_id'],
    },
  },
  {
    name: 'read_processed_email',
    description:
      'Get detailed information about a specific processed email, including output files, status, and any errors. ' +
      'Use fetch_emails first to find the email ID.',
    input_schema: {
      type: 'object',
      properties: {
        email_id: {
          type: 'string',
          description: 'ID of the processed email record',
        },
      },
      required: ['email_id'],
    },
  },
  {
    name: 'download_attachment',
    description:
      'Copy an output file (XLSX, PDF) from a processed email to a specified location in the workspace. ' +
      'Use read_processed_email first to see available output files.',
    input_schema: {
      type: 'object',
      properties: {
        source_path: {
          type: 'string',
          description: 'Path to the output file (from read_processed_email output_files)',
        },
        destination: {
          type: 'string',
          description: 'Destination directory path (default: workspace/downloads/)',
        },
      },
      required: ['source_path'],
    },
  },
  {
    name: 'send_email',
    description:
      "Send an email using a configured client's SMTP settings. Can send new emails or resend processed email results. " +
      'Requires client_id for SMTP credentials. Specify recipients, subject, body, and optional attachment file paths.',
    input_schema: {
      type: 'object',
      properties: {
        client_id: {
          type: 'string',
          description: 'ID of the email client (for SMTP credentials)',
        },
        to: {
          type: 'array',
          items: { type: 'string' },
          description: 'Recipient email addresses',
        },
        subject: {
          type: 'string',
          description: 'Email subject line',
        },
        body: {
          type: 'string',
          description: 'Email body text',
        },
        attachment_paths: {
          type: 'array',
          items: { type: 'string' },
          description: 'Paths to files to attach',
        },
      },
      required: ['client_id', 'to', 'subject', 'body'],
    },
  },
  {
    name: 'send_shipment_email',
    description:
      'Send a shipment email using an _email_params.json file. This composes and sends the formatted shipment notification ' +
      'email via the pipeline (compose_email + SMTP). Use this after fixing checklist blockers to send the email.',
    input_schema: {
      type: 'object',
      properties: {
        email_params_path: {
          type: 'string',
          description: 'Absolute path to the _email_params.json file',
        },
      },
      required: ['email_params_path'],
    },
  },
  {
    name: 'edit_xlsx_cell',
    description:
      'Edit specific NUMERIC cells in an XLSX file without changing the file structure. ' +
      'Use this ONLY to fix individual numeric values like tariff codes, package counts, or financial amounts. ' +
      'Do NOT write text like "TOTAL", "ADJUSTMENT", or "SUMMARY" — only numbers and tariff codes. ' +
      'Row 1 is the header (read-only). Row 2+ are data rows — do NOT overwrite them with totals. ' +
      'NEVER use write_file or edit_file on XLSX files — they corrupt the binary format.',
    input_schema: {
      type: 'object',
      properties: {
        path: {
          type: 'string',
          description: 'Path to XLSX file to edit',
        },
        edits: {
          type: 'array',
          description: 'Array of cell edits. Each edit: {row, col, value, sheet?}. col can be number (1-based) or letter (A-AK).',
          items: {
            type: 'object',
            properties: {
              row: { type: 'number', description: 'Row number (1-based)' },
              col: { type: ['number', 'string'], description: 'Column number (1-based) or letter (A-AK)' },
              value: { description: 'New cell value (string, number, or formula starting with =)' },
              sheet: { type: 'string', description: 'Sheet name (default: first sheet)' },
            },
            required: ['row', 'col', 'value'],
          },
        },
      },
      required: ['path', 'edits'],
    },
  },
  {
    name: 'reprocess_email',
    description:
      'Re-run the full processing pipeline on a previously processed email. ' +
      'Useful when rules have been updated, or to retry after fixing issues. ' +
      'Use read_processed_email first to find the email details and output files.',
    input_schema: {
      type: 'object',
      properties: {
        email_id: {
          type: 'string',
          description: 'ID of the processed email record to reprocess',
        },
        client_id: {
          type: 'string',
          description: 'Client ID (required to set up processing context)',
        },
      },
      required: ['email_id', 'client_id'],
    },
  },
] as const;

// ─── Public API ─────────────────────────────────────────────────

/** Return all tool definitions for the Claude agent. */
export function getAgentTools(): ToolDefinition[] {
  return [...TOOL_DEFINITIONS];
}

/** Execute a named agent tool with the given input. */
export async function executeAgentTool(
  name: string,
  input: ToolInput,
): Promise<ToolResult> {
  const handler = TOOL_HANDLERS[name];
  if (!handler) {
    return { error: `Unknown tool: ${name}` };
  }
  return handler(input);
}

// ─── Tool Handler Registry ──────────────────────────────────────

const TOOL_HANDLERS: Record<string, (input: ToolInput) => Promise<ToolResult> | ToolResult> = {
  run_pipeline: runPipelineTool,
  read_file: readFileTool,
  write_file: writeFileTool,
  edit_file: editFileTool,
  list_files: listFilesTool,
  query_rules: queryRulesTool,
  update_rules: updateRulesTool,
  validate_xlsx: validateXlsxTool,
  verify_line_count: verifyLineCountTool,
  reclassify_items: reclassifyItemsTool,
  lookup_tariff: lookupTariffTool,
  web_search: webSearchTool,
  add_cet_entry: addCetEntryTool,
  cet_stats: cetStatsTool,
  search_chat_history: searchChatHistoryTool,
  extract_with_ocr: extractWithOcrTool,
  compare_ocr_results: compareOcrResultsTool,
  get_unknown_products: getUnknownProductsTool,
  split_pdf: splitPdfTool,
  get_email_clients: getEmailClientsTool,
  parse_email_instructions: parseEmailInstructionsTool,
  fetch_emails: fetchEmailsTool,
  read_processed_email: readProcessedEmailTool,
  download_attachment: downloadAttachmentTool,
  send_email: sendEmailTool,
  send_shipment_email: sendShipmentEmailTool,
  edit_xlsx_cell: editXlsxCellTool,
  reprocess_email: reprocessEmailTool,
};

// ─── Path Resolution ────────────────────────────────────────────

/** Project-relative prefixes that resolve against baseDir() instead of workspace */
const PROJECT_PREFIXES = ['pipeline/', 'config/', 'rules/', 'data/'] as const;

function resolvePath(filePath: string): string {
  if (path.isAbsolute(filePath)) return filePath;

  const base = baseDir();

  // Allow direct access to project-level directories
  if (PROJECT_PREFIXES.some((p) => filePath.startsWith(p))) {
    return path.join(base, filePath);
  }

  // Prevent double-prefixing if path already includes workspace/
  if (filePath.startsWith('workspace/') || filePath === 'workspace') {
    return path.join(base, filePath);
  }

  // Handle "." and "./" as workspace root
  if (filePath === '.' || filePath === './') {
    return workspacePath();
  }

  // All other relative paths resolve under workspace/
  return path.join(workspacePath(), filePath);
}

// ─── Pipeline Tools ─────────────────────────────────────────────

async function runPipelineTool(input: ToolInput): Promise<ToolResult> {
  return new Promise((resolve) => {
    const bridge = new PythonBridge();
    const inputFile = resolvePath(input.input_file as string);
    const outputFile = input.output_file
      ? resolvePath(input.output_file as string)
      : path.join(outputPath(), `output_${Date.now()}.xlsx`);

    bridge.run(
      inputFile,
      outputFile,
      () => {},
      (report: any) => {
        // Add output file verification to the report
        if (report && typeof report === 'object') {
          const r = report as Record<string, unknown>;
          if (r.status === 'success' && r.output && typeof r.output === 'string') {
            try {
              const stat = fs.statSync(r.output as string);
              r.output_verified = true;
              r.output_size = stat.size;
              r.output_modified = stat.mtime.toISOString();
            } catch {
              r.output_verified = false;
            }
          }
        }
        resolve(report);
      },
      (error: any) => resolve({ error }),
    );
  });
}

async function reclassifyItemsTool(_input: ToolInput): Promise<ToolResult> {
  // TODO: PythonBridge does not yet have a reclassify() method - stub for now
  return { error: 'reclassify is not yet implemented in PythonBridge' };
}

async function extractWithOcrTool(input: ToolInput): Promise<ToolResult> {
  return new Promise((resolve) => {
    const bridge = new PythonBridge();
    const inputFile = resolvePath(input.input_file as string);
    const ocrMethod = (input.ocr_method as string) || 'pdfplumber';
    const skipTxtFallback = (input.skip_txt_fallback as boolean) || false;

    const inputBasename = path.basename(inputFile, path.extname(inputFile));
    const outDir = outputPath();
    const outputFile = path.join(outDir, `extracted_${ocrMethod}_${inputBasename}.json`);

    fs.mkdirSync(outDir, { recursive: true });

    bridge.extractWithOcr(
      inputFile,
      outputFile,
      ocrMethod,
      skipTxtFallback,
      (result: any) => {
        if (result && typeof result === 'object') {
          const r = result as Record<string, unknown>;
          r.output_file = outputFile;
          try {
            const stat = fs.statSync(outputFile);
            r.output_size = stat.size;
          } catch { /* ignore */ }
        }
        resolve(result);
      },
      (error: any) => resolve({ error }),
    );
  });
}

function compareOcrResultsTool(input: ToolInput): ToolResult {
  try {
    const fileA = resolvePath(input.file_a as string);
    const fileB = resolvePath(input.file_b as string);

    if (!fs.existsSync(fileA)) return { error: `File not found: ${fileA}` };
    if (!fs.existsSync(fileB)) return { error: `File not found: ${fileB}` };

    const dataA = JSON.parse(fs.readFileSync(fileA, 'utf-8'));
    const dataB = JSON.parse(fs.readFileSync(fileB, 'utf-8'));

    const methodA = dataA.ocr_method || 'unknown';
    const methodB = dataB.ocr_method || 'unknown';

    const invoiceA = dataA.invoices?.[0] || {};
    const invoiceB = dataB.invoices?.[0] || {};

    // Compare metadata
    const metadataDiff: Record<string, { a: unknown; b: unknown }> = {};
    const metaFields = ['invoice_number', 'date', 'supplier', 'total'];
    for (const field of metaFields) {
      if (invoiceA[field] !== invoiceB[field]) {
        metadataDiff[field] = { a: invoiceA[field], b: invoiceB[field] };
      }
    }

    // Compare items
    const itemsA: unknown[] = invoiceA.items || [];
    const itemsB: unknown[] = invoiceB.items || [];
    const itemsDiff: unknown[] = [];

    const maxItems = Math.max(itemsA.length, itemsB.length);
    for (let i = 0; i < maxItems; i++) {
      const itemA = itemsA[i] as Record<string, unknown> | undefined;
      const itemB = itemsB[i] as Record<string, unknown> | undefined;

      if (!itemA && itemB) {
        itemsDiff.push({ index: i, type: 'missing_in_a', b: itemB });
      } else if (itemA && !itemB) {
        itemsDiff.push({ index: i, type: 'missing_in_b', a: itemA });
      } else if (itemA && itemB) {
        const diffs: Record<string, { a: unknown; b: unknown }> = {};
        const itemFields = ['sku', 'description', 'quantity', 'unit_cost', 'total_cost'];
        for (const field of itemFields) {
          if (itemA[field] !== itemB[field]) {
            if (typeof itemA[field] === 'number' && typeof itemB[field] === 'number') {
              if (Math.abs((itemA[field] as number) - (itemB[field] as number)) > 0.01) {
                diffs[field] = { a: itemA[field], b: itemB[field] };
              }
            } else {
              diffs[field] = { a: itemA[field], b: itemB[field] };
            }
          }
        }
        if (Object.keys(diffs).length > 0) {
          itemsDiff.push({ index: i, diffs });
        }
      }
    }

    const calcTotal = (items: unknown[]): number =>
      items.reduce((sum: number, i) => sum + ((i as Record<string, number>).total_cost || 0), 0 as number) as number;
    const calcTotalA: number = calcTotal(itemsA);
    const calcTotalB: number = calcTotal(itemsB);

    return {
      comparison: { method_a: methodA, method_b: methodB, file_a: fileA, file_b: fileB },
      metadata_differences: Object.keys(metadataDiff).length > 0 ? metadataDiff : null,
      item_count: { a: itemsA.length, b: itemsB.length, difference: itemsA.length - itemsB.length },
      calculated_totals: {
        a: Math.round(calcTotalA * 100) / 100,
        b: Math.round(calcTotalB * 100) / 100,
        difference: Math.round((calcTotalA - calcTotalB) * 100) / 100,
      },
      item_differences: itemsDiff.length > 0 ? itemsDiff : null,
      summary: {
        identical: Object.keys(metadataDiff).length === 0 && itemsDiff.length === 0,
        metadata_matches: Object.keys(metadataDiff).length === 0,
        items_match: itemsDiff.length === 0,
        items_with_differences: itemsDiff.length,
      },
    };
  } catch (e: unknown) {
    return { error: e instanceof Error ? e.message : String(e) };
  }
}

// ─── File Operation Tools ───────────────────────────────────────

const MAX_READ_CHARS = 30_000;
const MAX_READ_LINES = 500;

function readFileTool(input: ToolInput): ToolResult {
  try {
    const filePath = resolvePath(input.path as string);
    const rawContent = fs.readFileSync(filePath, 'utf-8');
    const allLines = rawContent.split('\n');
    const totalLines = allLines.length;
    const fileSize = Buffer.byteLength(rawContent, 'utf-8');

    const offset = Math.max(1, (input.offset as number) || 1);
    const effectiveLimit = (input.limit as number) || MAX_READ_LINES;

    const startIdx = offset - 1;
    const endIdx = Math.min(startIdx + effectiveLimit, totalLines);
    const selectedLines = allLines.slice(startIdx, endIdx);

    let content = selectedLines
      .map((line, i) => `${offset + i}: ${line}`)
      .join('\n');

    let charTruncated = false;
    if (content.length > MAX_READ_CHARS) {
      content = content.slice(0, MAX_READ_CHARS);
      charTruncated = true;
    }

    const linesReturned = selectedLines.length;
    const hasMore = endIdx < totalLines;
    const truncated = hasMore || charTruncated;

    const result: Record<string, unknown> = {
      path: filePath,
      content,
      total_lines: totalLines,
      file_size: fileSize,
      showing_lines: `${offset}-${offset + linesReturned - 1}`,
      lines_returned: linesReturned,
    };

    if (truncated) {
      result.truncated = true;
      result.next_offset = offset + linesReturned;
      result.remaining_lines = totalLines - (offset + linesReturned - 1);
      result.hint = `File has ${totalLines} total lines. Use read_file with offset: ${offset + linesReturned} to continue reading.`;
    }

    return result;
  } catch (e: unknown) {
    return { error: e instanceof Error ? e.message : String(e) };
  }
}

function writeFileTool(input: ToolInput): ToolResult {
  try {
    // Block XLSX writes — use edit_xlsx_cell instead
    const writePath = ((input.path as string) || '').toLowerCase();
    if (writePath.endsWith('.xlsx') || writePath.endsWith('.xls')) {
      return {
        error:
          'Cannot write XLSX files with write_file — it corrupts the binary format and destroys the worksheet structure. ' +
          'Use edit_xlsx_cell(path, edits: [{row, col, value}]) to modify specific cell values instead. ' +
          'NEVER rewrite, recreate, or replace XLSX files.',
      };
    }

    // Reject writes from truncated/recovered JSON to prevent corrupting files
    if (input._truncated) {
      return {
        error:
          'File content was truncated during transmission (file too large for a single write_file call). ' +
          'Use edit_file instead: read_file the file first, then use edit_file(path, old_text, new_text) ' +
          'to make surgical replacements. Do NOT attempt to rewrite the entire file in one write_file call.',
      };
    }

    const filePath = resolvePath(input.path as string);
    const content = input.content as string;
    fs.mkdirSync(path.dirname(filePath), { recursive: true });
    fs.writeFileSync(filePath, content);

    // Verify: read back and confirm
    const readback = fs.readFileSync(filePath, 'utf-8');
    const stat = fs.statSync(filePath);
    const lineCount = readback.split('\n').length;
    const matches = readback === content;
    const first200 = readback.slice(0, 200);
    const last200 = readback.length > 400 ? readback.slice(-200) : '';

    return {
      success: true,
      verified: matches,
      path: filePath,
      size: stat.size,
      lines: lineCount,
      preview_start: first200,
      preview_end: last200 || undefined,
    };
  } catch (e: unknown) {
    return { error: e instanceof Error ? e.message : String(e) };
  }
}

function editFileTool(input: ToolInput): ToolResult {
  try {
    // Block XLSX edits — use edit_xlsx_cell instead
    const editPath = ((input.path as string) || '').toLowerCase();
    if (editPath.endsWith('.xlsx') || editPath.endsWith('.xls')) {
      return {
        error:
          'Cannot edit XLSX files with edit_file — it corrupts the binary format. ' +
          'Use edit_xlsx_cell(path, edits: [{row, col, value}]) to modify specific cell values instead.',
      };
    }

    if (input._truncated) {
      return {
        error:
          'Tool input was truncated during transmission. Make your edit_file call smaller: ' +
          'use shorter old_text (minimum uniquely-identifying context) and shorter new_text.',
      };
    }

    const filePath = resolvePath(input.path as string);
    const oldText = input.old_text as string;
    const newText = input.new_text as string;

    if (!oldText) {
      return { error: 'old_text is required and must not be empty' };
    }

    if (!fs.existsSync(filePath)) {
      return { error: `File not found: ${filePath}` };
    }

    const content = fs.readFileSync(filePath, 'utf-8');

    // Normalize line endings for matching (Windows \r\n -> \n)
    const hadCRLF = content.includes('\r\n');
    const normalizedContent = content.replace(/\r\n/g, '\n');
    const normalizedOldText = oldText.replace(/\r\n/g, '\n');

    // Count occurrences
    let count = 0;
    let searchPos = 0;
    while (true) {
      const idx = normalizedContent.indexOf(normalizedOldText, searchPos);
      if (idx === -1) break;
      count++;
      searchPos = idx + 1;
    }

    if (count === 0) {
      return {
        error: 'old_text not found in file. It must match exactly, including whitespace and indentation. Use read_file to see the current content.',
        file_lines: content.split('\n').length,
        file_preview: content.slice(0, 300),
      };
    }

    if (count > 1) {
      return {
        error: `old_text matches ${count} locations in the file. It must match exactly 1. Add more surrounding context lines to make it unique.`,
        match_count: count,
      };
    }

    // Perform the replacement
    const normalizedNewText = newText.replace(/\r\n/g, '\n');
    let updated = normalizedContent.replace(normalizedOldText, normalizedNewText);

    // Restore original line endings if file used CRLF
    if (hadCRLF) {
      updated = updated.replace(/\n/g, '\r\n');
    }

    fs.writeFileSync(filePath, updated);

    // Verify
    const readback = fs.readFileSync(filePath, 'utf-8');
    const stat = fs.statSync(filePath);
    const verified = readback === updated;
    const lineCount = readback.split('\n').length;

    // Show edit in context
    const checkText = hadCRLF ? normalizedNewText.replace(/\n/g, '\r\n') : normalizedNewText;
    const editStart = readback.indexOf(checkText);
    let contextPreview = '';
    if (editStart !== -1 && checkText.length > 0) {
      const before = readback.slice(Math.max(0, editStart - 80), editStart);
      const after = readback.slice(editStart + checkText.length, editStart + checkText.length + 80);
      contextPreview = before + '>>>' + checkText + '<<<' + after;
    }

    return {
      success: true,
      verified,
      path: filePath,
      size: stat.size,
      lines: lineCount,
      old_text_length: oldText.length,
      new_text_length: newText.length,
      context_preview: contextPreview.slice(0, 500) || undefined,
    };
  } catch (e: unknown) {
    return { error: e instanceof Error ? e.message : String(e) };
  }
}

function listFilesTool(input: ToolInput): ToolResult {
  try {
    const dirPath = resolvePath(input.path as string);
    const entries = fs.readdirSync(dirPath, { withFileTypes: true });
    return entries.map((entry): FileEntry => ({
      name: entry.name,
      type: entry.isDirectory() ? 'directory' : 'file',
      size: entry.isFile() ? fs.statSync(path.join(dirPath, entry.name)).size : undefined,
    }));
  } catch (e: unknown) {
    return { error: e instanceof Error ? e.message : String(e) };
  }
}

// ─── Rules Tools ────────────────────────────────────────────────

function queryRulesTool(input: ToolInput): ToolResult {
  try {
    const rulesFile = classificationRulesPath();
    const data = JSON.parse(fs.readFileSync(rulesFile, 'utf-8'));
    const query = (input.query as string).toUpperCase();
    const field = (input.field as string) || 'all';

    const matches = data.rules.filter((rule: Record<string, unknown>) => {
      if (field === 'code' || field === 'all') {
        if ((rule.code as string)?.includes(query)) return true;
      }
      if (field === 'category' || field === 'all') {
        if ((rule.category as string)?.toUpperCase().includes(query)) return true;
      }
      if (field === 'patterns' || field === 'all') {
        if ((rule.patterns as string[])?.some((p) => p.toUpperCase().includes(query))) return true;
      }
      return false;
    });

    return { count: matches.length, rules: matches };
  } catch (e: unknown) {
    return { error: e instanceof Error ? e.message : String(e) };
  }
}

function updateRulesTool(input: ToolInput): ToolResult {
  try {
    const rulesFile = classificationRulesPath();
    const data = JSON.parse(fs.readFileSync(rulesFile, 'utf-8'));

    // Backup before changes
    const backupPath = rulesFile + `.backup_${Date.now()}`;
    fs.writeFileSync(backupPath, JSON.stringify(data, null, 2));

    const action = input.action as string;
    const ruleId = input.rule_id as string;
    const rule = input.rule as Record<string, unknown>;

    if (action === 'add') {
      if (!rule || !rule.id) return { error: 'Rule must have an id' };
      data.rules.push(rule);
    } else if (action === 'modify') {
      const idx = data.rules.findIndex((r: Record<string, unknown>) => r.id === ruleId);
      if (idx === -1) return { error: `Rule ${ruleId} not found` };
      data.rules[idx] = { ...data.rules[idx], ...rule };
    } else if (action === 'delete') {
      const idx = data.rules.findIndex((r: Record<string, unknown>) => r.id === ruleId);
      if (idx === -1) return { error: `Rule ${ruleId} not found` };
      data.rules.splice(idx, 1);
    }

    fs.writeFileSync(rulesFile, JSON.stringify(data, null, 2));
    return { success: true, action, backup: backupPath, ruleCount: data.rules.length };
  } catch (e: unknown) {
    return { error: e instanceof Error ? e.message : String(e) };
  }
}

// ─── XLSX Tools ─────────────────────────────────────────────────

async function validateXlsxTool(input: ToolInput): Promise<ToolResult> {
  return new Promise((resolve) => {
    const bridge = new PythonBridge();
    const filePath = resolvePath(input.path as string);
    bridge.validate(filePath, (result: any) => resolve(result), (error: any) => resolve({ error }));
  });
}

function verifyLineCountTool(input: ToolInput): ToolResult {
  const sourceFile = resolvePath(input.source_file as string);
  const xlsxFile = resolvePath(input.xlsx_file as string);

  // Count items in source file
  let sourceItemCount = 0;
  const sourceItems: string[] = [];

  try {
    const sourceContent = fs.readFileSync(sourceFile, 'utf-8');
    const ext = path.extname(sourceFile).toLowerCase();

    if (ext === '.json') {
      const data = JSON.parse(sourceContent);

      if (data.groups && Array.isArray(data.groups)) {
        for (const group of data.groups) {
          if (group.items && Array.isArray(group.items)) {
            sourceItemCount += group.items.length;
            for (const item of group.items) {
              sourceItems.push(`${item.line_number || '?'}: ${item.description?.substring(0, 40) || 'N/A'}`);
            }
          }
        }
      } else if (data.items && Array.isArray(data.items)) {
        sourceItemCount = data.items.length;
        for (const item of data.items) {
          sourceItems.push(`${item.line_number || '?'}: ${item.description?.substring(0, 40) || 'N/A'}`);
        }
      }
    } else if (ext === '.txt') {
      const lines = sourceContent.split('\n');
      const itemPattern = /^\s*(\d+)\s+[\w-]+\s+.+\s+[\d,.]+\s+[\d,.]+/;

      for (const line of lines) {
        if (itemPattern.test(line)) {
          sourceItemCount++;
          const match = line.match(/^\s*(\d+)\s+[\w-]+\s+(.{1,40})/);
          if (match) {
            sourceItems.push(`${match[1]}: ${match[2].trim()}`);
          }
        }
      }
    }
  } catch (e: unknown) {
    return { error: `Failed to read source file: ${e instanceof Error ? e.message : String(e)}` };
  }

  // Count detail rows in XLSX
  let xlsxDetailCount = 0;
  const xlsxDetails: string[] = [];

  try {
    const buffer = fs.readFileSync(xlsxFile);
    const workbook = XLSX.read(buffer, { type: 'buffer' });
    const sheetName = workbook.SheetNames[0];
    const ws = workbook.Sheets[sheetName];
    const range = XLSX.utils.decode_range(ws['!ref'] || 'A1');

    for (let r = 1; r <= range.e.r; r++) {
      const supplierItemCell = ws[XLSX.utils.encode_cell({ r, c: 8 })];
      const descCell = ws[XLSX.utils.encode_cell({ r, c: 9 })];

      if (supplierItemCell && supplierItemCell.v !== undefined && supplierItemCell.v !== '') {
        const val = supplierItemCell.v;
        if (typeof val === 'number' || (typeof val === 'string' && /^\d+$/.test(val.trim()))) {
          xlsxDetailCount++;
          const desc = descCell?.v?.toString()?.substring(0, 40) || 'N/A';
          xlsxDetails.push(`${val}: ${desc}`);
        }
      }
    }
  } catch (e: unknown) {
    return { error: `Failed to read XLSX file: ${e instanceof Error ? e.message : String(e)}` };
  }

  const match = sourceItemCount === xlsxDetailCount;
  const difference = xlsxDetailCount - sourceItemCount;

  return {
    status: match ? 'MATCH' : 'MISMATCH',
    source_file: path.basename(sourceFile),
    xlsx_file: path.basename(xlsxFile),
    source_item_count: sourceItemCount,
    xlsx_detail_count: xlsxDetailCount,
    difference,
    message: match
      ? `Line count verified: ${sourceItemCount} items in source match ${xlsxDetailCount} detail rows in XLSX`
      : `Line count MISMATCH: Source has ${sourceItemCount} items, XLSX has ${xlsxDetailCount} detail rows (difference: ${difference > 0 ? '+' : ''}${difference})`,
    ...(match ? {} : {
      source_items_sample: sourceItems.slice(0, 10),
      xlsx_details_sample: xlsxDetails.slice(0, 10),
      hint: difference > 0
        ? 'XLSX has more rows than source - check for duplicate entries or extra rows'
        : 'XLSX has fewer rows than source - some items may not have been extracted',
    }),
  };
}

async function editXlsxCellTool(input: ToolInput): Promise<ToolResult> {
  try {
    const filePath = resolvePath(input.path as string);
    const edits = input.edits as CellEdit[];

    if (!edits || !Array.isArray(edits) || edits.length === 0) {
      return { error: 'edits must be a non-empty array of {row, col, value}' };
    }

    if (edits.length > 50) {
      return { error: 'Maximum 50 cell edits per call to prevent structural damage' };
    }

    if (!fs.existsSync(filePath)) {
      return { error: `File not found: ${filePath}` };
    }

    // Guard: reject edits that write summary/adjustment/total text into cells
    const forbiddenPatterns = /\b(TOTAL|ADJUSTMENT|SUMMARY|GRAND\s*TOTAL|ALL\s*ITEMS)\b/i;
    for (const edit of edits) {
      if (edit.row === 1) {
        return { error: 'Row 1 is the header row and cannot be edited. Data rows start at row 2.' };
      }
      const valStr = String(edit.value ?? '');
      if (forbiddenPatterns.test(valStr) && !valStr.startsWith('=')) {
        return {
          error:
            `Cannot write "${valStr}" — adding TOTAL/ADJUSTMENT/SUMMARY text is not allowed. ` +
            'edit_xlsx_cell is for fixing individual data values (numbers, tariff codes, etc.), not for inserting new content rows.',
        };
      }
    }

    // Use openpyxl via Python for reliable XLSX editing (preserves styles/formulas)
    const editsJson = JSON.stringify(edits);
    const pyScript = `
import json, sys, os
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter

file_path = ${JSON.stringify(filePath.replace(/\\/g, '/'))}
edits = json.loads(${JSON.stringify(editsJson)})

wb = load_workbook(file_path)
applied = []

for edit in edits:
    sheet_name = edit.get('sheet') or wb.sheetnames[0]
    ws = wb[sheet_name]

    col = edit['col']
    if isinstance(col, str):
        from openpyxl.utils import column_index_from_string
        col_idx = column_index_from_string(col.upper())
    else:
        col_idx = int(col)

    row = int(edit['row'])
    cell = ws.cell(row=row, column=col_idx)
    old_val = cell.value

    val = edit['value']
    if val is None or val == '':
        cell.value = None
    elif isinstance(val, str) and val.startswith('='):
        cell.value = val
    else:
        try:
            cell.value = float(val) if '.' in str(val) else int(val)
        except (ValueError, TypeError):
            cell.value = str(val)

    col_letter = get_column_letter(col_idx)
    applied.append({'cell': f'{col_letter}{row}', 'old': str(old_val) if old_val is not None else None, 'new': val})

wb.save(file_path)
print('REPORT:' + json.dumps({'success': True, 'path': file_path, 'edits_applied': len(applied), 'details': applied}))
`;

    const tmpScript = path.join(workspacePath(), '_edit_xlsx_cell.py');
    fs.writeFileSync(tmpScript, pyScript);

    try {
      const { execSync } = require('child_process');
      const result = execSync(`py "${tmpScript}"`, {
        cwd: baseDir(),
        encoding: 'utf-8',
        timeout: 30_000,
      });

      try { fs.unlinkSync(tmpScript); } catch { /* cleanup */ }

      const reportMatch = result.match(/REPORT:(.+)/);
      if (reportMatch) {
        return JSON.parse(reportMatch[1]);
      }
      return { success: true, output: result.trim() };
    } catch (e: unknown) {
      try { fs.unlinkSync(tmpScript); } catch { /* cleanup */ }
      const message = e instanceof Error ? (e as any).stderr || e.message : String(e);
      return { error: `Python edit failed: ${message}` };
    }
  } catch (e: unknown) {
    return { error: e instanceof Error ? e.message : String(e) };
  }
}

// ─── Tariff / CET Tools ────────────────────────────────────────

function lookupTariffTool(input: ToolInput): ToolResult {
  // Lazy import: cet-store may not be ported yet
  const { lookupTariff } = require('../services/cet-store');
  const query = (input.query as string) || '';
  const options: { code?: string; chapter?: number } = {};
  if (input.code) options.code = input.code as string;
  if (input.chapter) options.chapter = input.chapter as number;
  return lookupTariff(query, options);
}

async function webSearchTool(input: ToolInput): Promise<ToolResult> {
  const query = input.query as string;
  if (!query) return { error: 'query is required' };

  // Read API key from settings
  let apiKey = '';
  try {
    const settingsFile = path.join(dataPath(), 'settings.json');
    const settings = JSON.parse(fs.readFileSync(settingsFile, 'utf-8'));
    apiKey = settings.apiKey || settings.api_key || '';
  } catch {
    return { error: 'Could not read API key from data/settings.json' };
  }

  if (!apiKey) {
    return { error: 'No API key configured in data/settings.json' };
  }

  return webSearch(query, apiKey);
}

function addCetEntryTool(input: ToolInput): ToolResult {
  const { addCetEntry } = require('../services/cet-store');
  const hsCode = input.hs_code as string;
  const description = input.description as string;
  if (!hsCode || !description) {
    return { error: 'hs_code and description are required' };
  }
  return addCetEntry({
    hs_code: hsCode,
    description,
    duty_rate: input.duty_rate as string | undefined,
    unit: input.unit as string | undefined,
    notes: input.notes as string | undefined,
    aliases: input.aliases as string[] | undefined,
  });
}

function cetStatsTool(): ToolResult {
  const { getCetStats } = require('../services/cet-store');
  return getCetStats();
}

function getUnknownProductsTool(input: ToolInput): ToolResult {
  try {
    let classifiedFile = input.classified_file as string | undefined;

    if (!classifiedFile) {
      // Auto-detect from latest pipeline run in /tmp
      const tmpDirs = fs.readdirSync('/tmp')
        .filter((d) => d.startsWith('caricom_'))
        .map((d) => ({
          name: d,
          path: path.join('/tmp', d),
          mtime: fs.statSync(path.join('/tmp', d)).mtime,
        }))
        .sort((a, b) => b.mtime.getTime() - a.mtime.getTime());

      for (const dir of tmpDirs) {
        const candidate = path.join(dir.path, 'classified.json');
        if (fs.existsSync(candidate)) {
          classifiedFile = candidate;
          break;
        }
      }

      if (!classifiedFile) {
        const outDir = outputPath();
        try {
          const files = fs.readdirSync(outDir).filter((f) => f.includes('classified'));
          if (files.length > 0) {
            classifiedFile = path.join(outDir, files[0]);
          }
        } catch { /* output dir may not exist */ }
      }
    } else {
      classifiedFile = resolvePath(classifiedFile);
    }

    if (!classifiedFile || !fs.existsSync(classifiedFile)) {
      return { error: 'No classified.json file found. Run the pipeline first.' };
    }

    const data = JSON.parse(fs.readFileSync(classifiedFile, 'utf-8'));
    const items: Record<string, unknown>[] = data.items || [];

    const unknownItems = items.filter(
      (item) =>
        (item.classification as Record<string, unknown>)?.code === 'UNKNOWN' ||
        (item.classification as Record<string, unknown>)?.category === 'UNCLASSIFIED',
    );

    if (unknownItems.length === 0) {
      return {
        status: 'success',
        message: 'All items are classified. No UNKNOWN products.',
        total_items: items.length,
        unknown_count: 0,
      };
    }

    // Group by product type
    const productTypes = new Map<string, { count: number; examples: string[] }>();

    for (const item of unknownItems) {
      const desc = (item.description as string) || '';
      const coreType = desc
        .split('(')[0]
        .trim()
        .toUpperCase()
        .replace(/\s+(BLACK|WHITE|BROWN|GOLD|SILVER|PINK|RED|BLUE|GREEN|PURPLE|ASST|ASSORTED)\s*$/i, '')
        .replace(/\s+\d+\s*$/, '')
        .trim();

      if (!productTypes.has(coreType)) {
        productTypes.set(coreType, { count: 0, examples: [] });
      }
      const entry = productTypes.get(coreType)!;
      entry.count++;
      if (entry.examples.length < 3) {
        entry.examples.push(desc);
      }
    }

    // HS chapter hint lookup
    const CHAPTER_HINTS: Array<{ keywords: RegExp; chapter: string }> = [
      { keywords: /BRUSH|COMB|CLIP/, chapter: 'chapter 96 (brushes, combs, hair accessories)' },
      { keywords: /SOAP|WASH|SHAMPOO/, chapter: 'chapter 33 or 34 (cosmetics/soap)' },
      { keywords: /CREAM|LOTION|SERUM/, chapter: 'chapter 33 (cosmetics)' },
      { keywords: /RAZOR|BLADE|SCISSORS/, chapter: 'chapter 82 (cutlery, tools)' },
      { keywords: /IRON|DRYER|ELECTRIC/, chapter: 'chapter 85 (electrical appliances)' },
      { keywords: /CAP|BONNET|WRAP/, chapter: 'chapter 65 (headwear)' },
      { keywords: /SOCK|TIGHT/, chapter: 'chapter 61 (hosiery)' },
    ];

    const suggestions = Array.from(productTypes.entries())
      .sort((a, b) => b[1].count - a[1].count)
      .map(([productType, info]) => {
        let searchQuery = `HS code tariff classification "${productType}"`;
        let likelyChapter = '';

        for (const hint of CHAPTER_HINTS) {
          if (hint.keywords.test(productType)) {
            likelyChapter = hint.chapter;
            searchQuery += ` harmonized system ${hint.chapter.split(' ')[0]} ${hint.chapter.split(' ')[1]}`;
            break;
          }
        }

        return {
          product_type: productType,
          count: info.count,
          examples: info.examples,
          likely_chapter: likelyChapter || 'unknown - research needed',
          suggested_search_query: searchQuery,
          suggested_rule_pattern: productType.split(' ').filter((w) => w.length > 3)[0] || productType,
        };
      });

    return {
      status: 'needs_research',
      source_file: classifiedFile,
      total_items: items.length,
      unknown_count: unknownItems.length,
      unique_product_types: productTypes.size,
      products_to_research: suggestions,
      next_steps: [
        'For each product type, use web_search with the suggested query',
        'Verify the found HS code with lookup_tariff(code: "XXXXXXXX")',
        'Add a classification rule with update_rules using the suggested pattern',
        'Add a CET entry with add_cet_entry for future lookups',
        'Re-run the pipeline to verify all items are classified',
      ],
    };
  } catch (e: unknown) {
    return { error: e instanceof Error ? e.message : String(e) };
  }
}

// ─── PDF Tools ──────────────────────────────────────────────────

async function splitPdfTool(input: ToolInput): Promise<ToolResult> {
  const pdfPath = resolvePath(input.pdf_path as string);
  const outDir = input.output_dir ? resolvePath(input.output_dir as string) : path.dirname(pdfPath);

  if (!fs.existsSync(pdfPath)) {
    return { error: `PDF file not found: ${pdfPath}` };
  }

  const pipelineDir = path.join(baseDir(), 'pipeline');
  const scriptPath = path.join(pipelineDir, 'pdf_splitter.py');
  const pythonCmd = process.platform === 'win32' ? 'python' : 'python3';

  if (!fs.existsSync(scriptPath)) {
    return { error: 'pdf_splitter.py not found in pipeline directory' };
  }

  return new Promise((resolve) => {
    const { spawn } = require('child_process');
    const proc = spawn(pythonCmd, [scriptPath, pdfPath, '--output-dir', outDir], {
      cwd: pipelineDir,
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' },
    });

    let stdout = '';
    let stderr = '';

    proc.stdout.on('data', (data: Buffer) => { stdout += data.toString(); });
    proc.stderr.on('data', (data: Buffer) => { stderr += data.toString(); });

    proc.on('close', (code: number) => {
      if (code === 0 && stdout) {
        try {
          resolve(JSON.parse(stdout.trim()));
        } catch {
          resolve({ error: `Failed to parse output: ${stdout}` });
        }
      } else {
        resolve({ error: stderr || `Process exited with code ${code}` });
      }
    });

    proc.on('error', (err: Error) => {
      resolve({ error: `Failed to start Python: ${err.message}` });
    });
  });
}

// ─── Email Tools ────────────────────────────────────────────────

async function getEmailClientsTool(): Promise<ToolResult> {
  try {
    const { getClients, getProcessedEmails } = await import('../stores/client.store');
    const clients = getClients();

    return {
      status: 'success',
      client_count: clients.length,
      clients: clients.map((c: any) => {
        const recentEmails = getProcessedEmails(c.id, 5);
        return {
          id: c.id,
          name: c.name,
          enabled: c.enabled,
          incoming_email: c.incomingEmail.address,
          outgoing_email: c.outgoingEmail.address,
          watch_folder: c.watchFolder,
          auto_process: c.autoProcess,
          output_recipients: c.outputRecipients,
          recent_processed: recentEmails.length,
          last_activity: recentEmails[0]?.processedAt || null,
        };
      }),
    };
  } catch (e: unknown) {
    return { error: e instanceof Error ? e.message : String(e) };
  }
}

function parseEmailInstructionsTool(input: ToolInput): ToolResult {
  const emailBody = input.email_body as string;
  if (!emailBody) {
    return { error: 'email_body is required' };
  }

  const instructions: string[] = [];
  const actions: Array<{ action: string; details?: string }> = [];

  const patterns = [
    { regex: /please\s+(.+?)(?:\.|$)/gi, type: 'request' },
    { regex: /instructions?:\s*(.+?)(?:\n|$)/gi, type: 'instruction' },
    { regex: /note:\s*(.+?)(?:\n|$)/gi, type: 'note' },
    { regex: /send\s+(?:the\s+)?(?:output|results?|xlsx|files?)\s+to\s+([^\n.]+)/gi, type: 'send_to' },
    { regex: /copy\s+(?:to\s+)?([^\n.]+)/gi, type: 'copy_to' },
    { regex: /(?:classify|categorize)\s+(?:as|under)\s+([^\n.]+)/gi, type: 'classify' },
    { regex: /(?:urgent|priority|rush)/gi, type: 'priority' },
    { regex: /hold|delay|wait/gi, type: 'hold' },
  ] as const;

  for (const { regex, type } of patterns) {
    const matches = emailBody.matchAll(regex);
    for (const match of matches) {
      const text = match[1]?.trim() || match[0];
      if (text && text.length > 3) {
        instructions.push(text);
        if (type === 'send_to') {
          actions.push({ action: 'send_to', details: text });
        } else if (type === 'copy_to') {
          actions.push({ action: 'copy', details: text });
        } else if (type === 'priority') {
          actions.push({ action: 'mark_priority' });
        } else if (type === 'hold') {
          actions.push({ action: 'hold_processing' });
        }
      }
    }
  }

  const emailMatches = emailBody.match(/[\w.-]+@[\w.-]+\.[a-zA-Z]{2,}/g) || [];

  return {
    has_instructions: instructions.length > 0,
    instruction_count: instructions.length,
    instructions: [...new Set(instructions)],
    suggested_actions: actions,
    mentioned_emails: [...new Set(emailMatches)],
    analysis: {
      is_urgent: /urgent|priority|rush|asap/i.test(emailBody),
      is_hold: /hold|delay|wait|do not process/i.test(emailBody),
      has_custom_recipients: emailMatches.length > 0,
    },
  };
}

async function fetchEmailsTool(input: ToolInput): Promise<ToolResult> {
  try {
    const { getProcessedEmails, getClient } = await import('../stores/client.store');
    const clientId = input.client_id as string;
    const limit = (input.limit as number) || 20;
    const statusFilter = (input.status_filter as string) || 'all';

    const client = getClient(clientId);
    if (!client) {
      return { error: `Client not found: ${clientId}` };
    }

    let emails = getProcessedEmails(clientId, limit);
    if (statusFilter !== 'all') {
      emails = emails.filter((e: any) => e.status === statusFilter);
    }

    return {
      status: 'success',
      client_name: client.name,
      email_count: emails.length,
      emails: emails.map((e: any) => ({
        id: e.id,
        message_id: e.messageId,
        subject: e.subject,
        from: e.from,
        received_at: e.receivedAt,
        processed_at: e.processedAt,
        status: e.status,
        waybill: e.waybillNumber,
        invoice_number: e.invoiceNumber,
        output_files: e.outputFiles,
        error: e.error,
      })),
    };
  } catch (e: unknown) {
    return { error: e instanceof Error ? e.message : String(e) };
  }
}

async function readProcessedEmailTool(input: ToolInput): Promise<ToolResult> {
  try {
    const { getProcessedEmails } = await import('../stores/client.store');
    const emailId = input.email_id as string;

    const allEmails = getProcessedEmails(undefined, 1000);
    const email = allEmails.find((e: any) => e.id === emailId);

    if (!email) {
      return { error: `Processed email not found: ${emailId}` };
    }

    const outputFileStatus = (email.outputFiles || []).map((f: string) => ({
      path: f,
      filename: path.basename(f),
      exists: fs.existsSync(f),
      size: fs.existsSync(f) ? fs.statSync(f).size : 0,
    }));

    return {
      status: 'success',
      email: {
        id: email.id,
        message_id: email.messageId,
        client_id: email.clientId,
        subject: email.subject,
        from: email.from,
        received_at: email.receivedAt,
        processed_at: email.processedAt,
        status: email.status,
        waybill: email.waybillNumber,
        invoice_number: email.invoiceNumber,
        error: email.error,
        output_file_status: outputFileStatus,
      },
    };
  } catch (e: unknown) {
    return { error: e instanceof Error ? e.message : String(e) };
  }
}

async function downloadAttachmentTool(input: ToolInput): Promise<ToolResult> {
  try {
    const sourcePath = input.source_path as string;
    if (!fs.existsSync(sourcePath)) {
      return { error: `Source file not found: ${sourcePath}` };
    }

    const destDir = input.destination
      ? resolvePath(input.destination as string)
      : path.join(workspacePath(), 'downloads');

    fs.mkdirSync(destDir, { recursive: true });

    const destPath = path.join(destDir, path.basename(sourcePath));
    fs.copyFileSync(sourcePath, destPath);

    return {
      status: 'success',
      source: sourcePath,
      destination: destPath,
      size: fs.statSync(destPath).size,
    };
  } catch (e: unknown) {
    return { error: e instanceof Error ? e.message : String(e) };
  }
}

async function sendEmailTool(input: ToolInput): Promise<ToolResult> {
  try {
    const { getClient } = await import('../stores/client.store');
    const { sendEmail } = await import('./email-service');

    const clientId = input.client_id as string;
    const client = getClient(clientId);
    if (!client) {
      return { error: `Client not found: ${clientId}` };
    }

    const to = input.to as string[];
    const subject = input.subject as string;
    const body = input.body as string;
    const attachmentPaths = (input.attachment_paths as string[]) || [];

    const validAttachments = attachmentPaths.filter((p) => {
      if (!fs.existsSync(p)) {
        console.warn(`[agent] Attachment not found, skipping: ${p}`);
        return false;
      }
      return true;
    });

    await sendEmail(client, {
      to,
      subject,
      body,
      attachments: validAttachments.map((p) => ({
        filename: path.basename(p),
        path: p,
      })),
    });

    return {
      status: 'success',
      message: `Email sent to ${to.join(', ')}`,
      subject,
      attachments_sent: validAttachments.length,
    };
  } catch (e: unknown) {
    return { error: e instanceof Error ? e.message : String(e) };
  }
}

async function sendShipmentEmailTool(input: ToolInput): Promise<ToolResult> {
  try {
    const emailParamsPath = input.email_params_path as string;
    if (!emailParamsPath) {
      return { error: 'email_params_path is required' };
    }
    if (!fs.existsSync(emailParamsPath)) {
      return { error: `File not found: ${emailParamsPath}` };
    }

    console.log(`[agent] send_shipment_email: sending from ${emailParamsPath}`);
    const { sendShipmentEmailFromParams } = await import('./email-processor');
    const result = await sendShipmentEmailFromParams(emailParamsPath, (msg: string) => {
      console.log(`[agent] send_shipment_email: ${msg}`);
    });

    console.log(`[agent] send_shipment_email result:`, JSON.stringify(result));
    return result;
  } catch (e: unknown) {
    console.error(`[agent] send_shipment_email error:`, e);
    return { error: e instanceof Error ? e.message : String(e) };
  }
}

async function reprocessEmailTool(input: ToolInput): Promise<ToolResult> {
  try {
    const { getProcessedEmails, getClient, updateProcessedEmail } = await import('../stores/client.store');

    const emailId = input.email_id as string;
    const clientId = input.client_id as string;

    const client = getClient(clientId);
    if (!client) {
      return { error: `Client not found: ${clientId}` };
    }

    const allEmails = getProcessedEmails(clientId, 500);
    const emailRecord = allEmails.find((e: any) => e.id === emailId);
    if (!emailRecord) {
      return { error: `Processed email not found: ${emailId}` };
    }

    const pdfFiles = (emailRecord.outputFiles || []).filter(
      (f: string) => f.toLowerCase().endsWith('.pdf') && fs.existsSync(f),
    );

    if (pdfFiles.length === 0) {
      return {
        error: 'No PDF files found from original processing. Cannot reprocess.',
        available_files: emailRecord.outputFiles,
      };
    }

    updateProcessedEmail(emailId, {
      status: 'pipeline_running',
      error: undefined,
    });

    const invoicePdfs = pdfFiles.filter(
      (f: string) =>
        !path.basename(f).toLowerCase().includes('manifest') &&
        !path.basename(f).toLowerCase().includes('declaration'),
    );

    const results: unknown[] = [];
    for (const pdfPath of invoicePdfs) {
      const xlsxPath = pdfPath.replace(/\.pdf$/i, '.xlsx');
      try {
        const result = await new Promise<unknown>((resolve, reject) => {
          const bridge = new PythonBridge();
          bridge.run(
            pdfPath,
            xlsxPath,
            () => { /* progress */ },
            (r: any) => resolve(r),
            (err: any) => reject(new Error(err)),
          );
        });
        results.push({ pdf: pdfPath, xlsx: xlsxPath, result });
      } catch (err) {
        results.push({ pdf: pdfPath, error: err instanceof Error ? err.message : String(err) });
      }
    }

    const allSucceeded = results.every(
      (r: any) => typeof r === 'object' && r !== null && !('error' in (r as Record<string, unknown>)),
    );
    updateProcessedEmail(emailId, {
      status: allSucceeded ? 'completed' : 'error',
      processedAt: new Date().toISOString(),
      error: allSucceeded ? undefined : 'Some files failed reprocessing',
    });

    return {
      status: 'success',
      email_id: emailId,
      reprocessed_files: invoicePdfs.length,
      results,
    };
  } catch (e: unknown) {
    return { error: e instanceof Error ? e.message : String(e) };
  }
}

// ─── Chat History Tool ──────────────────────────────────────────

async function searchChatHistoryTool(input: ToolInput): Promise<ToolResult> {
  const { searchChatHistory } = await import('../stores/chat.store');
  return searchChatHistory(
    input.query as string | undefined,
    input.conversation_id as string | undefined,
    (input.limit as number) || 50,
  );
}
