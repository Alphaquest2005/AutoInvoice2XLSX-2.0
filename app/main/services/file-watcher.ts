/**
 * File-watcher service: watches the workspace directory for changes,
 * builds file trees, and provides file CRUD operations.
 */

import fs from 'fs';
import path from 'path';
import { workspacePath, LOCKED_FOLDERS, isLockedFolder } from '../utils/paths';
import type { FileNode } from '../../shared/types';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Hidden entries never shown in the file tree */
const HIDDEN_ENTRIES = new Set(['node_modules', '__pycache__', '_system', '.git']);

/** Folders that are junction/symlink targets (shown with link icon) */
const LINK_FOLDERS = new Set(['Downloads']);

/** Cutoff for "new" files: created within the last 24 hours */
const NEW_FILE_HOURS = 24;

/** Debounce window for coalescing rapid fs events */
const DEBOUNCE_MS = 300;

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

let watchers: fs.FSWatcher[] = [];
let changeCallbacks: Array<(event: string, filePath: string) => void> = [];

// ---------------------------------------------------------------------------
// Initialisation
// ---------------------------------------------------------------------------

/**
 * Start watching the workspace directory for file-system changes.
 * Must be called after `initBaseDirs()`.
 */
export function initFileWatcher(): void {
  const wsPath = workspacePath();

  // ── System folder: locked config/scripts the user shouldn't touch ──
  const sysDir = path.join(wsPath, '_system');
  fs.mkdirSync(sysDir, { recursive: true });
  try {
    fs.writeFileSync(
      path.join(sysDir, '.locked'),
      'This folder is managed by AutoInvoice2XLSX.\nDo not edit or delete.\n',
    );
  } catch { /* best-effort */ }

  // ── WebSource Downloads folder link ──
  const websourceDownloads = 'D:\\OneDrive\\Clients\\WebSource\\Downloads';
  const downloadsLink = path.join(wsPath, 'Downloads');
  if (fs.existsSync(websourceDownloads) && !fs.existsSync(downloadsLink)) {
    try {
      fs.symlinkSync(websourceDownloads, downloadsLink, 'junction');
      console.log(`[file-watcher] Created Downloads junction: ${downloadsLink} -> ${websourceDownloads}`);
    } catch (err) {
      console.warn(`[file-watcher] Failed to create Downloads link: ${(err as Error).message}`);
    }
  }

  // ── Debounced file-change dispatch ──
  let pendingChanges = new Map<string, string>();
  let debounceTimer: ReturnType<typeof setTimeout> | null = null;

  function onRawChange(event: string, fullPath: string): void {
    pendingChanges.set(fullPath, event);
    if (debounceTimer) return;
    debounceTimer = setTimeout(() => {
      debounceTimer = null;
      const batch = pendingChanges;
      pendingChanges = new Map();
      for (const [fp, ev] of batch) {
        for (const cb of changeCallbacks) cb(ev, fp);
      }
    }, DEBOUNCE_MS);
  }

  // ── Set up fs.watch ──
  try {
    // Prefer recursive watch (Windows / macOS)
    const watcher = fs.watch(wsPath, { recursive: true }, (_event, filename) => {
      if (filename) onRawChange(_event, path.join(wsPath, filename));
    });
    watchers.push(watcher);
  } catch {
    // Fallback: watch each top-level directory individually (Linux)
    const subdirs = fs.readdirSync(wsPath, { withFileTypes: true })
      .filter((d) => d.isDirectory())
      .map((d) => d.name);

    for (const dir of ['', ...subdirs]) {
      const dirPath = path.join(wsPath, dir);
      try {
        const watcher = fs.watch(dirPath, (_event, filename) => {
          if (filename) onRawChange(_event, path.join(dirPath, filename));
        });
        watchers.push(watcher);
      } catch { /* skip unwatchable dirs */ }
    }
  }
}

/**
 * Stop all file-system watchers and clear callbacks.
 */
export function stopFileWatcher(): void {
  for (const w of watchers) w.close();
  watchers = [];
  changeCallbacks = [];
}

// ---------------------------------------------------------------------------
// Change subscription
// ---------------------------------------------------------------------------

/**
 * Register a callback that fires (debounced) on every file-system change.
 * Returns an unsubscribe function.
 */
export function onFileChange(callback: (event: string, filePath: string) => void): () => void {
  changeCallbacks.push(callback);
  return () => {
    changeCallbacks = changeCallbacks.filter((cb) => cb !== callback);
  };
}

// ---------------------------------------------------------------------------
// File tree
// ---------------------------------------------------------------------------

/**
 * Build a recursive `FileNode` tree rooted at `dirPath`.
 */
export function getFileTree(dirPath: string): FileNode {
  const lstat = fs.lstatSync(dirPath);
  const name = path.basename(dirPath);
  const isSymlink = lstat.isSymbolicLink();
  const newCutoff = Date.now() - NEW_FILE_HOURS * 60 * 60 * 1000;

  const stat = isSymlink ? fs.statSync(dirPath) : lstat;

  // ── File node ──
  if (!stat.isDirectory()) {
    const isNew = stat.mtimeMs > newCutoff;
    return {
      name,
      path: dirPath,
      type: 'file',
      extension: path.extname(dirPath).slice(1),
      size: stat.size,
      modifiedAt: stat.mtime.toISOString(),
      ...(isNew ? { isNew: true } : {}),
      ...(isSymlink ? { isSymlink: true } : {}),
    };
  }

  // ── Directory node ──
  let children: FileNode[] = [];
  try {
    children = fs.readdirSync(dirPath, { withFileTypes: true })
      .filter((e) => !e.name.startsWith('.') && !HIDDEN_ENTRIES.has(e.name))
      .sort((a, b) => {
        const aDir = a.isDirectory() || a.isSymbolicLink();
        const bDir = b.isDirectory() || b.isSymbolicLink();
        if (aDir !== bDir) return aDir ? -1 : 1;
        return a.name.localeCompare(b.name);
      })
      .map((entry): FileNode | null => {
        const fullPath = path.join(dirPath, entry.name);
        try {
          if (entry.isDirectory() || entry.isSymbolicLink()) {
            return getFileTree(fullPath);
          }
          const fileStat = fs.statSync(fullPath);
          const isNew = fileStat.mtimeMs > newCutoff;
          return {
            name: entry.name,
            path: fullPath,
            type: 'file',
            extension: path.extname(entry.name).slice(1),
            size: fileStat.size,
            modifiedAt: fileStat.mtime.toISOString(),
            ...(isNew ? { isNew: true } : {}),
          };
        } catch {
          return null; // broken symlink, permission issue, etc.
        }
      })
      .filter((n): n is FileNode => n !== null);
  } catch { /* unreadable dir */ }

  const isLink = isSymlink || LINK_FOLDERS.has(name);
  const hasNewChildren = children.some((c) => c.isNew);

  return {
    name,
    path: dirPath,
    type: 'directory',
    children,
    ...(isLockedFolder(name) ? { isLocked: true } : {}),
    ...(isLink ? { isSymlink: true } : {}),
    ...(hasNewChildren ? { isNew: true } : {}),
  };
}

// ---------------------------------------------------------------------------
// File CRUD helpers
// ---------------------------------------------------------------------------

export function readFileContent(filePath: string): string {
  return fs.readFileSync(filePath, 'utf-8');
}

export function writeFileContent(filePath: string, content: string): void {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, content);
}

export function deleteFileOrDir(targetPath: string): void {
  fs.rmSync(targetPath, { recursive: true, force: true });
}

export function renameFileOrDir(oldPath: string, newPath: string): void {
  fs.renameSync(oldPath, newPath);
}

export function copyFileOrDir(srcPath: string, destPath: string): void {
  const stat = fs.statSync(srcPath);
  if (stat.isDirectory()) {
    fs.mkdirSync(destPath, { recursive: true });
    for (const entry of fs.readdirSync(srcPath)) {
      copyFileOrDir(path.join(srcPath, entry), path.join(destPath, entry));
    }
  } else {
    fs.mkdirSync(path.dirname(destPath), { recursive: true });
    fs.copyFileSync(srcPath, destPath);
  }
}

export function createDirectory(dirPath: string): void {
  fs.mkdirSync(dirPath, { recursive: true });
}

export function createEmptyFile(filePath: string): void {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, '');
}

// ---------------------------------------------------------------------------
// Copy-to-workspace (drag-and-drop / import)
// ---------------------------------------------------------------------------

/** File-type to default workspace subdirectory mapping */
const EXT_SUBDIR_MAP: Record<string, string> = {
  '.pdf': 'input',
  '.xlsx': 'output',
  '.xls': 'output',
  '.json': 'intermediate',
  '.yaml': 'intermediate',
  '.yml': 'intermediate',
};

/**
 * Copy an external file into the workspace, placing it under a date-based
 * subfolder. Duplicate filenames get a `(n)` counter suffix.
 *
 * Returns the final destination path.
 */
export function copyToWorkspace(sourcePath: string, subdir?: string): string {
  const ext = path.extname(sourcePath).toLowerCase();
  const baseName = path.basename(sourcePath);
  const targetSubdir = subdir ?? EXT_SUBDIR_MAP[ext] ?? 'input';

  // Date-based subfolder
  const today = new Date();
  const dateFolder = [
    today.getFullYear(),
    String(today.getMonth() + 1).padStart(2, '0'),
    String(today.getDate()).padStart(2, '0'),
  ].join('-');

  const targetDir = path.join(workspacePath(), targetSubdir, dateFolder);
  fs.mkdirSync(targetDir, { recursive: true });

  // Handle duplicate filenames
  let targetPath = path.join(targetDir, baseName);
  if (fs.existsSync(targetPath)) {
    const nameNoExt = path.basename(sourcePath, ext);
    let counter = 1;
    while (fs.existsSync(targetPath)) {
      targetPath = path.join(targetDir, `${nameNoExt} (${counter})${ext}`);
      counter++;
    }
  }

  fs.copyFileSync(sourcePath, targetPath);
  console.log(`[file-watcher] Copied ${sourcePath} -> ${targetPath}`);
  return targetPath;
}
