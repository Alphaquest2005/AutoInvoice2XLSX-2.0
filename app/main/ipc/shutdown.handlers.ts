import { ipcMain, app } from 'electron';
import fs from 'fs';
import path from 'path';
import type { HandlerDependencies } from './index';
import { dataPath } from '../utils/paths';

const SESSION_FILE = () => path.join(dataPath(), 'session_state.json');

function saveSessionState(state: Record<string, unknown>): void {
  try {
    fs.mkdirSync(path.dirname(SESSION_FILE()), { recursive: true });
    fs.writeFileSync(SESSION_FILE(), JSON.stringify(state, null, 2), 'utf-8');
  } catch {
    // Best-effort session save
  }
}

function loadSessionState(): Record<string, unknown> | null {
  try {
    if (!fs.existsSync(SESSION_FILE())) return null;
    return JSON.parse(fs.readFileSync(SESSION_FILE(), 'utf-8'));
  } catch {
    return null;
  }
}

function clearSessionState(): void {
  try {
    if (fs.existsSync(SESSION_FILE())) {
      fs.unlinkSync(SESSION_FILE());
    }
  } catch {
    // Best-effort cleanup
  }
}

export function registerShutdownHandlers(deps: HandlerDependencies): void {
  let isLlmBusy = false;
  let shutdownRequested = false;

  ipcMain.on('shutdown:request', (_e, activeConversationId: string | null) => {
    const win = deps.getMainWindow();
    if (!win) return;

    console.log('[shutdown] Shutdown requested, LLM busy:', isLlmBusy);
    shutdownRequested = true;

    if (!isLlmBusy) {
      // LLM is idle, close immediately
      console.log('[shutdown] LLM idle, closing now');
      win.webContents.send('shutdown:status', { status: 'closing', message: 'Closing...' });
      setTimeout(() => win.close(), 500);
    } else {
      // LLM is busy, notify renderer and wait
      console.log('[shutdown] LLM busy, waiting for completion');
      win.webContents.send('shutdown:status', {
        status: 'waiting',
        message: 'Waiting for LLM to finish...',
      });

      // Save session state so we can resume
      saveSessionState({
        activeConversationId,
        wasLlmBusy: true,
        shutdownTime: new Date().toISOString(),
        resumePrompt: 'Continue from where you left off. The app was closed while you were working.',
      });
    }
  });

  ipcMain.on('shutdown:cancel', () => {
    console.log('[shutdown] Shutdown cancelled');
    shutdownRequested = false;
    clearSessionState();
    deps.getMainWindow()?.webContents.send('shutdown:status', { status: 'cancelled', message: '' });
  });

  ipcMain.on('shutdown:force', (_e, activeConversationId: string | null) => {
    console.log('[shutdown] Force shutdown');
    saveSessionState({
      activeConversationId,
      wasLlmBusy: isLlmBusy,
      shutdownTime: new Date().toISOString(),
      resumePrompt: 'Continue from where you left off. The app was closed while you were working.',
    });
    deps.getMainWindow()?.close();
  });

  ipcMain.on('llm:busy', (_e, busy: boolean) => {
    const wasBusy = isLlmBusy;
    isLlmBusy = busy;
    console.log(`[shutdown] LLM busy: ${wasBusy} -> ${busy}`);

    // If shutdown was requested and LLM just finished, close now
    if (wasBusy && !busy && shutdownRequested) {
      console.log('[shutdown] LLM finished, closing now');
      const win = deps.getMainWindow();
      win?.webContents.send('shutdown:status', { status: 'closing', message: 'LLM finished. Closing...' });
      clearSessionState(); // Clean shutdown, no need to resume
      setTimeout(() => win?.close(), 500);
    }
  });

  ipcMain.handle('shutdown:getPendingSession', async () => {
    const state = loadSessionState();
    if (state) {
      console.log('[shutdown] Found pending session:', state);
    }
    return state;
  });

  ipcMain.on('shutdown:clearSession', () => {
    clearSessionState();
  });
}
