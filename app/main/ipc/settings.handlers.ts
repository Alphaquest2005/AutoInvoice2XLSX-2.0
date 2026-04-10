import { ipcMain } from 'electron';
import type { HandlerDependencies } from './index';
import { loadSettings, saveSettings, getApiKey, setApiKey } from '../utils/settings';

export function registerSettingsHandlers(_deps: HandlerDependencies): void {
  ipcMain.handle('settings:get', async () => {
    return loadSettings();
  });

  ipcMain.handle('settings:save', async (_e, updates: Record<string, unknown>) => {
    saveSettings(updates);
  });

  ipcMain.handle('settings:getApiKey', async () => {
    return getApiKey();
  });

  ipcMain.handle('settings:setApiKey', async (_e, key: string) => {
    setApiKey(key);
    // No explicit LLM client invalidation needed: the chat handler uses a
    // lazy-init pattern that re-reads settings on each message, so a new key
    // is picked up automatically on the next request.
  });
}
