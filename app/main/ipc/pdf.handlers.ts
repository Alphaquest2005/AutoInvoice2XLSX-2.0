import { ipcMain } from 'electron';
import { spawn } from 'child_process';
import path from 'path';
import type { HandlerDependencies } from './index';

// ---------------------------------------------------------------------------
// Python helper
// ---------------------------------------------------------------------------

function pythonCmd(): string {
  return process.platform === 'win32' ? 'python' : 'python3';
}

/**
 * Spawn a Python process and collect stdout/stderr.
 * Resolves with parsed JSON from stdout, or rejects on error.
 */
function runPython(
  args: string[],
  cwd?: string,
): Promise<{ success: boolean; [key: string]: unknown }> {
  return new Promise((resolve, reject) => {
    const proc = spawn(pythonCmd(), args, {
      cwd,
      env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONUNBUFFERED: '1' },
    });

    let stdout = '';
    let stderr = '';

    proc.stdout.on('data', (data) => { stdout += data.toString(); });
    proc.stderr.on('data', (data) => { stderr += data.toString(); });

    proc.on('error', (err) => {
      reject(new Error(`Failed to spawn Python: ${err.message}`));
    });

    proc.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(stderr || `Python process exited with code ${code}`));
        return;
      }

      // Parse the last JSON object from stdout
      const trimmed = stdout.trim();
      if (!trimmed) {
        reject(new Error('Python produced no output'));
        return;
      }

      try {
        // Find the last JSON line (in case of mixed output)
        const lines = trimmed.split('\n');
        for (let i = lines.length - 1; i >= 0; i--) {
          const line = lines[i].trim();
          if (line.startsWith('{')) {
            resolve(JSON.parse(line));
            return;
          }
        }
        // Fallback: try parsing the whole output
        resolve(JSON.parse(trimmed));
      } catch {
        reject(new Error(`Failed to parse Python output: ${trimmed.slice(0, 300)}`));
      }
    });
  });
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

export function registerPdfHandlers(deps: HandlerDependencies): void {
  ipcMain.handle('pdf:split', async (_e, pdfPath: string, outputDir?: string) => {
    try {
      const scriptPath = path.join(deps.baseDir, 'pipeline', 'pdf_splitter.py');
      const args = [scriptPath, pdfPath];
      if (outputDir) {
        args.push('--output-dir', outputDir);
      }

      const result = await runPython(args, path.join(deps.baseDir, 'pipeline'));
      const ok = result.status === 'success' || result.success === true;
      return { ...result, success: ok };
    } catch (err) {
      return { success: false, error: (err as Error).message };
    }
  });

  ipcMain.handle('pdf:reorder', async (_e, pdfPath: string, pageOrder: number[]) => {
    try {
      const script = `
import fitz, sys, json
doc = fitz.open(sys.argv[1])
order = json.loads(sys.argv[2])
new_doc = fitz.open()
for p in order:
    new_doc.insert_pdf(doc, from_page=p-1, to_page=p-1)
output = sys.argv[1].replace('.pdf', '_reordered.pdf')
new_doc.save(output)
print(json.dumps({"success": True, "output": output}))
`;
      const result = await runPython(
        ['-c', script, pdfPath, JSON.stringify(pageOrder)],
      );
      return result;
    } catch (err) {
      return { success: false, error: (err as Error).message };
    }
  });

  ipcMain.handle('pdf:getPageCount', async (_e, pdfPath: string) => {
    try {
      const script = `
import fitz, sys, json
doc = fitz.open(sys.argv[1])
print(json.dumps({"success": True, "page_count": len(doc)}))
`;
      const result = await runPython(['-c', script, pdfPath]);
      return result;
    } catch (err) {
      return { success: false, error: (err as Error).message };
    }
  });
}
