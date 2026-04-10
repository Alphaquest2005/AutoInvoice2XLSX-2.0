/**
 * Python Bridge Service
 *
 * Manages communication with the Python autoinvoice pipeline.
 * Handles Python auto-detection, dependency installation, and
 * spawning pipeline processes for extraction, processing, and validation.
 */

import { spawn, execSync, ChildProcess } from 'child_process';
import fs from 'fs';
import { baseDir, pipelineConfigPath } from '../utils/paths';
import type {
  PipelineProgress,
  PipelineReport,
  ExtractionProgress,
  ExtractTextResult,
} from '../../shared/types';

/** Packages required by the v2.0 pipeline (from pyproject.toml dependencies). */
const REQUIRED_PACKAGES: ReadonlyArray<{ pip: string; importName: string; timeout: number }> = [
  { pip: 'pdfplumber', importName: 'pdfplumber', timeout: 120_000 },
  { pip: 'PyMuPDF', importName: 'fitz', timeout: 300_000 },
  { pip: 'pytesseract', importName: 'pytesseract', timeout: 120_000 },
  { pip: 'Pillow', importName: 'PIL', timeout: 120_000 },
  { pip: 'openpyxl', importName: 'openpyxl', timeout: 120_000 },
  { pip: 'pyyaml', importName: 'yaml', timeout: 120_000 },
  { pip: 'pydantic', importName: 'pydantic', timeout: 120_000 },
  { pip: 'pydantic-settings', importName: 'pydantic_settings', timeout: 120_000 },
  { pip: 'anthropic', importName: 'anthropic', timeout: 120_000 },
  { pip: 'httpx', importName: 'httpx', timeout: 120_000 },
];

export class PythonBridge {
  private pythonPath: string;
  private activeProcess: ChildProcess | null = null;
  private depsChecked = false;

  constructor() {
    this.pythonPath = detectPython();
  }

  /** Run the full autoinvoice pipeline on an input file. */
  run(
    inputFile: string,
    outputFile: string,
    onProgress: (progress: PipelineProgress) => void,
    onComplete: (report: PipelineReport) => void,
    onError: (error: string) => void,
  ): void {
    this.ensureDependencies();

    const args = [
      '-m', 'autoinvoice',
      '--input', inputFile,
      '--output', outputFile,
      '--base-dir', baseDir(),
      '--json-output',
    ];

    const configFile = pipelineConfigPath();
    if (fs.existsSync(configFile)) {
      args.push('--config', configFile);
    }

    console.log(`[python-bridge] Spawning: ${this.pythonPath} ${args.join(' ')}`);

    this.activeProcess = spawn(this.pythonPath, ['-u', ...args], {
      cwd: baseDir(),
      env: { ...process.env, PYTHONUNBUFFERED: '1' },
    });

    console.log(`[python-bridge] Process spawned with PID: ${this.activeProcess.pid}`);

    const { stdout, stderr } = collectOutput(this.activeProcess, (line) => {
      if (line.startsWith('PROGRESS:')) {
        try {
          onProgress(JSON.parse(line.slice(9)));
        } catch { /* ignore malformed progress */ }
      } else if (!line.startsWith('REPORT:')) {
        onProgress({ stage: 'processing', message: line });
      }
    });

    this.activeProcess.on('close', (code) => {
      console.log(`[python-bridge] Process exited with code: ${code}`);
      this.activeProcess = null;

      if (code === 0) {
        const report = extractReport(stdout.value);
        onComplete(report ?? buildFallbackReport(inputFile, outputFile));
      } else {
        const report = extractReport(stdout.value);
        if (report?.errors?.length) {
          onError(`Pipeline failed:\n${report.errors.join('\n')}`);
        } else {
          onError(`Pipeline failed (exit code ${code}):\n${stderr.value || stdout.value}`);
        }
      }
    });

    this.activeProcess.on('error', (err) => {
      this.activeProcess = null;
      onError(`Failed to start Python: ${err.message}\nIs Python 3 installed?`);
    });
  }

  /** Validate an XLSX file against pipeline rules. */
  validate(
    xlsxPath: string,
    onComplete: (result: unknown) => void,
    onError: (error: string) => void,
  ): void {
    const args = [
      '-m', 'autoinvoice',
      '--validate', xlsxPath,
      '--base-dir', baseDir(),
      '--json-output',
    ];

    const proc = spawn(this.pythonPath, args, { cwd: baseDir() });

    const { stdout, stderr } = collectOutput(proc);

    proc.on('close', (code) => {
      const report = extractReport(stdout.value);
      if (report) {
        onComplete(report);
      } else if (code === 0) {
        onComplete({ status: 'valid', raw: stdout.value });
      } else {
        onError(stderr.value || stdout.value || `Validation failed (code ${code})`);
      }
    });

    proc.on('error', (err) => {
      onError(`Failed to start Python: ${err.message}`);
    });
  }

  /** Extract text from a PDF using the Python text extractor. */
  extractText(
    inputPdf: string,
    outputTxt: string,
    apiKey: string,
    baseUrl: string,
    model: string,
    onProgress: (progress: ExtractionProgress) => void,
    onComplete: (result: ExtractTextResult) => void,
    onError: (error: string) => void,
  ): void {
    this.ensureDependencies();

    const args = [
      '-m', 'autoinvoice',
      '--extract-text',
      '--input', inputPdf,
      '--output', outputTxt,
      '--base-dir', baseDir(),
      '--json-output',
    ];

    if (apiKey) args.push('--api-key', apiKey);
    if (baseUrl) args.push('--base-url', baseUrl);
    if (model) args.push('--model', model);

    const proc = spawn(this.pythonPath, ['-u', ...args], {
      cwd: baseDir(),
      env: { ...process.env, PYTHONUNBUFFERED: '1' },
    });

    const { stdout, stderr } = collectOutput(proc, (line) => {
      if (line.startsWith('PROGRESS:')) {
        try {
          onProgress(JSON.parse(line.slice(9)));
        } catch { /* ignore malformed progress */ }
      }
    });

    proc.on('close', (code) => {
      if (code === 0) {
        const result = extractReport(stdout.value);
        onComplete(result ?? { success: true, outputPath: outputTxt });
      } else {
        const result = extractReport(stdout.value);
        if (result?.error) {
          onError(result.error);
        } else {
          onError(`Text extraction failed (exit code ${code}):\n${stderr.value || stdout.value}`);
        }
      }
    });

    proc.on('error', (err) => {
      onError(`Failed to start Python: ${err.message}\nIs Python 3 installed?`);
    });
  }

  /** Extract text from a scanned PDF using OCR. */
  extractWithOcr(
    inputFile: string,
    outputFile: string,
    ocrMethod: string,
    skipTxtFallback: boolean,
    onComplete: (result: unknown) => void,
    onError: (error: string) => void,
  ): void {
    this.ensureDependencies();

    const config = JSON.stringify({
      ocr_method: ocrMethod,
      skip_txt_fallback: skipTxtFallback,
    });

    const args = [
      '-m', 'autoinvoice',
      '--extract-ocr',
      '--input', inputFile,
      '--output', outputFile,
      '--ocr-config', config,
      '--base-dir', baseDir(),
      '--json-output',
    ];

    console.log(`[python-bridge] extractWithOcr: ${this.pythonPath} ${args.join(' ')}`);

    const proc = spawn(this.pythonPath, ['-u', ...args], {
      cwd: baseDir(),
      env: { ...process.env, PYTHONUNBUFFERED: '1' },
    });

    const { stdout, stderr } = collectOutput(proc);

    proc.on('close', (code) => {
      console.log(`[python-bridge] OCR process exited with code: ${code}`);

      const report = extractReport(stdout.value);
      if (report) {
        onComplete(report);
      } else if (fs.existsSync(outputFile)) {
        try {
          onComplete(JSON.parse(fs.readFileSync(outputFile, 'utf-8')));
        } catch {
          onComplete({ status: code === 0 ? 'success' : 'error', ocr_method: ocrMethod });
        }
      } else if (code !== 0) {
        onError(stderr.value || stdout.value || `OCR extraction failed (code ${code})`);
      } else {
        onComplete({ status: 'success', ocr_method: ocrMethod });
      }
    });

    proc.on('error', (err) => {
      onError(`Failed to start Python: ${err.message}`);
    });
  }

  /** Kill the active pipeline process, if any. */
  kill(): void {
    if (this.activeProcess) {
      console.log(`[python-bridge] Killing process PID: ${this.activeProcess.pid}`);
      this.activeProcess.kill();
      this.activeProcess = null;
    }
  }

  /** Ensure critical Python packages are installed. Runs once per session. */
  private ensureDependencies(): void {
    if (this.depsChecked) return;
    this.depsChecked = true;

    for (const { pip, importName, timeout } of REQUIRED_PACKAGES) {
      try {
        execSync(`${this.pythonPath} -c "import ${importName}"`, {
          timeout: 10_000,
          stdio: ['pipe', 'pipe', 'pipe'],
        });
      } catch {
        console.log(`[python-bridge] Installing missing package: ${pip}`);
        try {
          execSync(`${this.pythonPath} -m pip install ${pip}`, {
            timeout,
            stdio: ['pipe', 'pipe', 'pipe'],
          });
          console.log(`[python-bridge] Installed ${pip}`);
        } catch (installErr: unknown) {
          const msg = installErr instanceof Error ? installErr.message.split('\n')[0] : String(installErr);
          console.warn(`[python-bridge] Failed to install ${pip}: ${msg}`);
        }
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Pure helper functions (not class methods, easier to test)
// ---------------------------------------------------------------------------

/** Detect a working Python 3 interpreter on the system PATH. */
function detectPython(): string {
  const candidates = process.platform === 'win32'
    ? ['py', 'python', 'python3']
    : ['python3', 'python', 'py'];

  for (const cmd of candidates) {
    try {
      const result = execSync(`${cmd} --version`, {
        encoding: 'utf-8',
        timeout: 5_000,
        stdio: ['pipe', 'pipe', 'pipe'],
      });
      if (result.includes('Python 3')) {
        console.log(`[python-bridge] Detected Python: "${cmd}" -> ${result.trim()}`);
        return cmd;
      }
    } catch {
      // This candidate is not available; try the next.
    }
  }

  // Fallback: common Windows install paths
  if (process.platform === 'win32') {
    const winPaths = [
      'C:\\Python313\\python.exe',
      'C:\\Python312\\python.exe',
      'C:\\Python311\\python.exe',
      'C:\\Python310\\python.exe',
    ];
    for (const p of winPaths) {
      if (fs.existsSync(p)) {
        console.log(`[python-bridge] Found Python at: ${p}`);
        return p;
      }
    }
  }

  console.warn('[python-bridge] No Python 3 found, defaulting to "python"');
  return 'python';
}

/**
 * Extract a JSON report from stdout using the REPORT: prefix marker.
 * Falls back to scanning for the last standalone JSON object line.
 */
function extractReport(stdout: string): any {
  const lines = stdout.split('\n');

  // Primary: look for the last REPORT: prefixed line
  for (let i = lines.length - 1; i >= 0; i--) {
    const trimmed = lines[i].trim();
    if (trimmed.startsWith('REPORT:')) {
      try {
        return JSON.parse(trimmed.slice(7));
      } catch { /* malformed, keep scanning */ }
    }
  }

  // Fallback: last standalone JSON object
  for (let i = lines.length - 1; i >= 0; i--) {
    const trimmed = lines[i].trim();
    if (trimmed.startsWith('{') && trimmed.endsWith('}')) {
      try {
        return JSON.parse(trimmed);
      } catch { /* not valid JSON, try previous */ }
    }
  }

  return null;
}

/** Mutable string ref for accumulating process output. */
interface StringRef { value: string }

/**
 * Wire up stdout/stderr collection for a child process.
 * Optionally invoke a per-line callback for stdout lines.
 */
function collectOutput(
  proc: ChildProcess,
  onStdoutLine?: (line: string) => void,
): { stdout: StringRef; stderr: StringRef } {
  const stdout: StringRef = { value: '' };
  const stderr: StringRef = { value: '' };

  proc.stdout?.on('data', (data: Buffer) => {
    const text = data.toString();
    stdout.value += text;
    console.log(`[python-bridge] STDOUT: ${text.trimEnd()}`);

    if (onStdoutLine) {
      for (const line of text.split('\n')) {
        const trimmed = line.trim();
        if (trimmed) onStdoutLine(trimmed);
      }
    }
  });

  proc.stderr?.on('data', (data: Buffer) => {
    const text = data.toString();
    stderr.value += text;
    console.log(`[python-bridge] STDERR: ${text.trimEnd()}`);
  });

  return { stdout, stderr };
}

/** Build a minimal success report when the pipeline produces no JSON report. */
function buildFallbackReport(inputFile: string, outputFile: string): PipelineReport {
  return {
    status: 'completed',
    started: new Date().toISOString(),
    input: inputFile,
    output: outputFile,
    stages: [],
    errors: [],
    warnings: [],
  };
}
