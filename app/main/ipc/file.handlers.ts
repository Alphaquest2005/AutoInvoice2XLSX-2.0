/**
 * IPC handlers for file system operations.
 * Channels: files:getTree, files:read, files:readBinary, files:write,
 *           files:openDialog, files:openFolderDialog, files:copyToWorkspace,
 *           files:delete, files:rename, files:copy, files:createFolder,
 *           files:createFile, files:showInExplorer, files:openExternal
 */

import { ipcMain, dialog, shell } from 'electron';
import fs from 'fs';
import path from 'path';
import type { HandlerDependencies } from './index';
import { isLockedFolder } from '../utils/paths';
import {
  getFileTree,
  copyToWorkspace,
  deleteFileOrDir,
  onFileChange,
} from '../services/file-watcher';

export function registerFileHandlers(deps: HandlerDependencies): void {
  // ── File tree ──────────────────────────────────────────────────────────────
  ipcMain.handle('files:getTree', async (_e, dirPath?: string) => {
    const targetDir = dirPath || deps.baseDir;
    if (!fs.existsSync(targetDir)) {
      return { name: path.basename(targetDir), path: targetDir, type: 'directory', children: [] };
    }
    return getFileTree(targetDir);
  });

  // ── Read / Write ───────────────────────────────────────────────────────────
  ipcMain.handle('files:read', async (_e, filePath: string) => {
    return fs.readFileSync(filePath, 'utf-8');
  });

  ipcMain.handle('files:readBinary', async (_e, filePath: string) => {
    const buffer = fs.readFileSync(filePath);
    return buffer.toString('base64');
  });

  ipcMain.handle('files:write', async (_e, filePath: string, content: string) => {
    fs.writeFileSync(filePath, content, 'utf-8');
  });

  // ── Dialogs ────────────────────────────────────────────────────────────────
  ipcMain.handle(
    'files:openDialog',
    async (_e, filters?: { name: string; extensions: string[] }[]) => {
      const win = deps.getMainWindow();
      if (!win) return null;
      const result = await dialog.showOpenDialog(win, {
        properties: ['openFile'],
        filters: filters || [{ name: 'All Files', extensions: ['*'] }],
      });
      return result.canceled ? null : result.filePaths[0] || null;
    },
  );

  ipcMain.handle('files:openFolderDialog', async () => {
    const win = deps.getMainWindow();
    if (!win) return null;
    const result = await dialog.showOpenDialog(win, { properties: ['openDirectory'] });
    return result.canceled ? null : result.filePaths[0] || null;
  });

  // ── Copy to workspace ─────────────────────────────────────────────────────
  ipcMain.handle('files:copyToWorkspace', async (_e, sourcePath: string, subdir?: string) => {
    return copyToWorkspace(sourcePath, subdir);
  });

  // ── Delete ─────────────────────────────────────────────────────────────────
  ipcMain.handle('files:delete', async (_e, filePath: string) => {
    const folderName = path.basename(filePath);
    if (isLockedFolder(folderName)) {
      throw new Error(`Cannot delete locked system folder: ${folderName}`);
    }
    deleteFileOrDir(filePath);
  });

  // ── Rename / Copy / Create ─────────────────────────────────────────────────
  ipcMain.handle('files:rename', async (_e, oldPath: string, newPath: string) => {
    const folderName = path.basename(oldPath);
    if (isLockedFolder(folderName)) {
      throw new Error(`Cannot rename locked system folder: ${folderName}`);
    }
    fs.renameSync(oldPath, newPath);
  });

  ipcMain.handle('files:copy', async (_e, sourcePath: string, destPath: string) => {
    fs.copyFileSync(sourcePath, destPath);
  });

  ipcMain.handle('files:createFolder', async (_e, dirPath: string) => {
    fs.mkdirSync(dirPath, { recursive: true });
  });

  ipcMain.handle('files:createFile', async (_e, filePath: string) => {
    fs.writeFileSync(filePath, '', 'utf-8');
  });

  // ── Shell operations ───────────────────────────────────────────────────────
  ipcMain.on('files:showInExplorer', (_e, filePath: string) => {
    shell.showItemInFolder(filePath);
  });

  ipcMain.handle('files:openExternal', async (_e, filePath: string) => {
    try {
      await shell.openPath(filePath);
      return { success: true };
    } catch (err) {
      return { success: false, error: String(err) };
    }
  });

  // ── File-change event forwarding ──────────────────────────────────────────
  onFileChange((event, changedPath) => {
    const win = deps.getMainWindow();
    if (win && !win.isDestroyed()) {
      win.webContents.send('files:changed', event, changedPath);
    }
  });
}
