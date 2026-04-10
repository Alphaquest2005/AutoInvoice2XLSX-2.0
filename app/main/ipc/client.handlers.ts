import { ipcMain, dialog } from 'electron';
import type { HandlerDependencies } from './index';
import {
  getClients,
  getClient,
  createClient,
  updateClient,
  deleteClient,
  getProcessedEmails,
} from '../stores/client.store';

export function registerClientHandlers(deps: HandlerDependencies): void {
  ipcMain.handle('clients:getAll', async () => {
    return getClients();
  });

  ipcMain.handle('clients:get', async (_e, id: string) => {
    return getClient(id);
  });

  ipcMain.handle('clients:create', async (_e, settings: Record<string, unknown>) => {
    return createClient(settings as Parameters<typeof createClient>[0]);
  });

  ipcMain.handle('clients:update', async (_e, id: string, updates: Record<string, unknown>) => {
    return updateClient(id, updates);
  });

  ipcMain.handle('clients:delete', async (_e, id: string) => {
    return deleteClient(id);
  });

  ipcMain.handle('clients:getProcessedEmails', async (_e, clientId?: string, limit?: number) => {
    return getProcessedEmails(clientId, limit);
  });

  ipcMain.handle('clients:selectWatchFolder', async () => {
    const win = deps.getMainWindow();
    if (!win) return null;
    const result = await dialog.showOpenDialog(win, { properties: ['openDirectory'] });
    return result.canceled ? null : result.filePaths[0] || null;
  });
}
