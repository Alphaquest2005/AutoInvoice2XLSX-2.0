/**
 * Electron main process entry point.
 *
 * Responsibilities:
 *  - Global error handling (EPIPE-safe)
 *  - Single-instance enforcement
 *  - Window creation and lifecycle
 *  - Composition root wiring via bootstrap()
 */

import { app, BrowserWindow, ipcMain } from 'electron';
import path from 'path';
import fs from 'fs';
import { execSync } from 'child_process';
import { bootstrap } from './bootstrap';
import { initBaseDirs } from './utils/paths';
import { initChatStore } from './stores/chat.store';
import { initCetStore, seedFromRules } from './stores/cet.store';
import { initClientStore } from './stores/client.store';
import { initFileWatcher } from './services/file-watcher';

// ─── GPU Workaround for WSLg ─────────────────────────────────────────────────
// WSLg GPU forwarding can crash the renderer process. Disable GPU when running
// under WSL to prevent SIGTRAP / exit_code=9 crashes.
if (process.platform === 'linux' && process.env.WSL_DISTRO_NAME) {
  app.disableHardwareAcceleration();
}

// ─── Constants ───────────────────────────────────────────────────────────────

const APP_MODEL_ID = 'com.insightsoftware.autoinvoice2xlsx';
const LOG_MAX_BYTES = 10 * 1024 * 1024; // 10 MB
const LOG_KEEP_BYTES = 1024 * 1024;      // Keep last 1 MB on trim
const SCREENSHOT_DELAY_MS = 3000;
const WINDOW_DEFAULTS = {
  width: 1400,
  height: 900,
  minWidth: 800,
  minHeight: 600,
  frame: false,
  titleBarStyle: 'hidden' as const,
  backgroundColor: '#0f172a',
};

// ─── Global Error Handlers ───────────────────────────────────────────────────
// Guard against EPIPE crash loops: when the console pipe is broken (app closing),
// writing to console triggers EPIPE -> uncaughtException -> console.error -> EPIPE.
// This guard prevents infinite recursion that previously filled error.log to 278 MB.

let _handlingError = false;

function safeLogToFile(entry: string): void {
  try {
    const logDir = app.isReady() ? app.getPath('userData') : process.cwd();
    const logPath = path.join(logDir, 'error.log');

    try {
      const stat = fs.statSync(logPath);
      if (stat.size > LOG_MAX_BYTES) {
        const data = fs.readFileSync(logPath);
        fs.writeFileSync(logPath, data.slice(-LOG_KEEP_BYTES));
      }
    } catch { /* file may not exist yet */ }

    fs.appendFileSync(logPath, entry);
  } catch {
    // Nothing else we can do
  }
}

process.on('uncaughtException', (error: Error) => {
  if (_handlingError || error.message?.includes('EPIPE')) {
    safeLogToFile(`[${new Date().toISOString()}] EPIPE (suppressed)\n`);
    return;
  }
  _handlingError = true;
  try { console.error(`[UNCAUGHT] ${error.name}: ${error.message}`); } catch { /* EPIPE */ }
  safeLogToFile(
    `[${new Date().toISOString()}] UNCAUGHT EXCEPTION\n${error.name}: ${error.message}\n${error.stack}\n\n`,
  );
  _handlingError = false;
});

process.on('unhandledRejection', (reason: unknown) => {
  if (_handlingError) return;
  _handlingError = true;
  const errorStr = reason instanceof Error
    ? `${reason.name}: ${reason.message}\n${reason.stack}`
    : String(reason);
  try { console.error(`[UNHANDLED REJECTION] ${errorStr.split('\n')[0]}`); } catch { /* EPIPE */ }
  safeLogToFile(`[${new Date().toISOString()}] UNHANDLED REJECTION\n${errorStr}\n\n`);
  _handlingError = false;
});

// ─── EPIPE-safe console wrappers ─────────────────────────────────────────────
// When Electron outlives its launching terminal (common on WSL / Windows),
// stdout/stderr become broken pipes. Wrapping silences the noise at the source.

const _origLog = console.log.bind(console);
const _origWarn = console.warn.bind(console);
const _origError = console.error.bind(console);
console.log = (...args: unknown[]) => { try { _origLog(...args); } catch { /* EPIPE */ } };
console.warn = (...args: unknown[]) => { try { _origWarn(...args); } catch { /* EPIPE */ } };
console.error = (...args: unknown[]) => { try { _origError(...args); } catch { /* EPIPE */ } };

// ─── Application State ──────────────────────────────────────────────────────

let mainWindow: BrowserWindow | null = null;

// Whether to load the Vite dev server or the built HTML.
// .trim() handles Windows env vars with trailing spaces from cmd.exe.
// Fallback: if dist/index.html exists on disk, treat as production (WSL env vars may not cross).
const nodeEnv = (process.env.NODE_ENV || '').trim();
const hasDevServerUrl = !!process.env.VITE_DEV_SERVER_URL;
const distHtmlExists = fs.existsSync(path.join(__dirname, '../../dist/index.html'));
const useDevServer = hasDevServerUrl || (!process.env.ELECTRON_IS_TEST && nodeEnv !== 'production' && !distHtmlExists);
const isFromSource = !app.isPackaged;

// ─── Stale Instance Cleanup ─────────────────────────────────────────────────

function killStaleInstances(): void {
  if (process.platform === 'win32') {
    killStaleInstancesWindows();
  } else {
    killStaleInstancesLinux();
  }
}

function killStaleInstancesLinux(): void {
  try {
    const myPid = process.pid;
    const output = execSync('pgrep -f "electron.*autoinvoice" 2>/dev/null || true', {
      encoding: 'utf-8',
      timeout: 5000,
    });
    const pids = output
      .split('\n')
      .map((line) => parseInt(line.trim(), 10))
      .filter((pid) => pid > 0 && pid !== myPid);

    if (pids.length > 0) {
      console.log(`Killing ${pids.length} stale Electron process(es): ${pids.join(', ')}`);
      for (const pid of pids) {
        try { process.kill(pid, 'SIGKILL'); } catch { /* already exited */ }
      }
    }
  } catch { /* non-critical */ }
}

function killStaleInstancesWindows(): void {
  try {
    const myPid = process.pid;
    const output = execSync('tasklist /FI "IMAGENAME eq electron.exe" /FO CSV /NH', {
      encoding: 'utf-8',
      timeout: 5000,
    });
    const pids = output
      .split('\n')
      .filter((line) => line.includes('electron.exe'))
      .map((line) => {
        const match = line.match(/"electron\.exe","(\d+)"/);
        return match ? parseInt(match[1], 10) : 0;
      })
      .filter((pid) => pid > 0 && pid !== myPid);

    if (pids.length > 0) {
      console.log(`Killing ${pids.length} stale Electron process(es): ${pids.join(', ')}`);
      for (const pid of pids) {
        try { execSync(`taskkill /PID ${pid} /F /T`, { timeout: 3000 }); } catch { /* already exited */ }
      }
    }
  } catch { /* non-critical */ }
}

// ─── Window Creation ─────────────────────────────────────────────────────────

function createWindow(): void {
  mainWindow = new BrowserWindow({
    ...WINDOW_DEFAULTS,
    webPreferences: {
      preload: path.join(__dirname, '../index.js'),
      nodeIntegration: false,
      contextIsolation: true,
      sandbox: false, // needed for better-sqlite3
    },
  });

  attachRendererDiagnostics();

  if (useDevServer) {
    const devUrl = process.env.VITE_DEV_SERVER_URL || 'http://localhost:5173';
    mainWindow.loadURL(devUrl);
  } else {
    mainWindow.loadFile(path.join(__dirname, '../../dist/index.html'));
  }

  mainWindow.on('closed', () => {
    mainWindow = null;
  });
}

// ─── Renderer Diagnostics ────────────────────────────────────────────────────

function attachRendererDiagnostics(): void {
  if (!mainWindow) return;
  const wc = mainWindow.webContents;

  wc.on('render-process-gone', (_event, details) => {
    console.error('═══════════════════════════════════════════════════════════');
    console.error('[RENDERER] PROCESS GONE:');
    console.error('  Reason:', details.reason);
    console.error('  Exit Code:', details.exitCode);
    console.error('═══════════════════════════════════════════════════════════');
  });

  wc.on('unresponsive', () => console.error('[RENDERER] Window became unresponsive'));
  wc.on('responsive', () => console.log('[RENDERER] Window became responsive again'));

  wc.on('did-fail-load', (_event, errorCode, errorDescription, validatedURL) => {
    console.error('═══════════════════════════════════════════════════════════');
    console.error('[RENDERER] FAILED TO LOAD:');
    console.error('  Error Code:', errorCode);
    console.error('  Description:', errorDescription);
    console.error('  URL:', validatedURL);
    console.error('═══════════════════════════════════════════════════════════');
  });

  wc.on('preload-error', (_event, preloadPath, error) => {
    console.error('═══════════════════════════════════════════════════════════');
    console.error('[PRELOAD] ERROR:');
    console.error('  Path:', preloadPath);
    console.error('  Error:', error);
    console.error('═══════════════════════════════════════════════════════════');
  });

  // Console message forwarding and collection for screenshots
  const consoleMessages: string[] = [];
  wc.on('console-message', (_event, level, message, line, sourceId) => {
    const levelStr = ['LOG', 'WARN', 'ERROR'][level] || 'INFO';
    const entry = `[${levelStr}] ${message} (${sourceId}:${line})`;
    consoleMessages.push(entry);
    if (level >= 2) {
      console.error('RENDERER:', entry);
    } else {
      console.log('RENDERER:', entry);
    }
  });

  // Auto-screenshot after content loads
  wc.on('did-finish-load', () => {
    setTimeout(() => {
      if (!mainWindow) return;
      mainWindow.webContents.capturePage().then((image) => {
        const screenshotDir = path.join(app.getPath('userData'), 'screenshots');
        fs.mkdirSync(screenshotDir, { recursive: true });
        const screenshotPath = path.join(screenshotDir, 'latest.png');
        fs.writeFileSync(screenshotPath, image.toPNG());
        const logPath = path.join(screenshotDir, 'console.log');
        fs.writeFileSync(logPath, consoleMessages.join('\n'));
        console.log(`Screenshot saved to: ${screenshotPath}`);
        console.log(`Console log saved to: ${logPath}`);
      }).catch((err) => {
        console.warn('Screenshot capture failed:', err);
      });
    }, SCREENSHOT_DELAY_MS);
  });

  // On-demand screenshots via IPC
  ipcMain.handle('debug:screenshot', async () => {
    if (!mainWindow) return null;
    const image = await mainWindow.webContents.capturePage();
    const screenshotDir = path.join(app.getPath('userData'), 'screenshots');
    fs.mkdirSync(screenshotDir, { recursive: true });
    const screenshotPath = path.join(screenshotDir, `screenshot-${Date.now()}.png`);
    fs.writeFileSync(screenshotPath, image.toPNG());
    return screenshotPath;
  });
}

// ─── Single Instance Lock ────────────────────────────────────────────────────

if (process.platform === 'win32') {
  app.setAppUserModelId(APP_MODEL_ID);
}

const gotTheLock = app.requestSingleInstanceLock();

if (!gotTheLock) {
  console.log('[startup] Another instance is already running -- quitting.');
  app.quit();
} else {
  app.on('second-instance', () => {
    if (mainWindow) {
      if (mainWindow.isMinimized()) mainWindow.restore();
      mainWindow.focus();
    }
  });

// ─── App Lifecycle ───────────────────────────────────────────────────────────

app.whenReady().then(async () => {
  killStaleInstances();

  // Resolve base directory (SSOT for all paths)
  const resolvedBase = isFromSource
    ? path.resolve(__dirname, '../../..')
    : path.join(process.resourcesPath);

  initBaseDirs(resolvedBase);

  // Initialize stores
  initStore('initChatStore', initChatStore);
  initStore('initCetStore', initCetStore);
  initStore('initClientStore', initClientStore);

  // Seed CET classification rules
  try {
    const { classificationRulesPath, invalidCodesPath } = await import('./utils/paths');
    seedFromRules(classificationRulesPath(), invalidCodesPath());
  } catch (err) {
    console.error('[startup] seedFromRules failed:', err);
    safeLogToFile(`[${new Date().toISOString()}] seedFromRules failed: ${err}\n`);
  }

  // Start file watcher
  try {
    initFileWatcher();
  } catch (err) {
    console.warn('[startup] File watcher initialization failed:', (err as Error).message);
  }

  // Wire composition root (registers all IPC handlers)
  try {
    bootstrap(resolvedBase, getMainWindow);
  } catch (err) {
    console.error('[startup] bootstrap failed:', err);
    safeLogToFile(`[${new Date().toISOString()}] bootstrap failed: ${err}\n`);
  }

  createWindow();

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow();
    }
  });
}).catch((err) => {
  console.error('[startup] Fatal error during app initialization:', err);
  safeLogToFile(`[${new Date().toISOString()}] FATAL STARTUP ERROR: ${err}\n${err?.stack || ''}\n`);
});

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit();
  }
});

} // end of single-instance else block

// ─── Helpers ─────────────────────────────────────────────────────────────────

function initStore(label: string, fn: () => void): void {
  try {
    fn();
  } catch (err) {
    console.error(`[startup] ${label} failed:`, err);
    safeLogToFile(`[${new Date().toISOString()}] ${label} failed: ${err}\n`);
  }
}

// ─── Exports ─────────────────────────────────────────────────────────────────

export function getMainWindow(): BrowserWindow | null {
  return mainWindow;
}
