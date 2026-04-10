/**
 * Verify every preload IPC channel has a matching handler registration.
 * This catches channel name mismatches (the #1 class of IPC bugs).
 */
import { describe, it, expect } from 'vitest';
import fs from 'fs';
import path from 'path';

const APP_DIR = path.resolve(__dirname, '../../../..');

function extractChannels(filePath: string, pattern: RegExp): string[] {
  const content = fs.readFileSync(path.join(APP_DIR, filePath), 'utf-8');
  const matches = [...content.matchAll(pattern)];
  return matches.map((m) => m[1]).sort();
}

const preloadInvoke = extractChannels(
  'app/preload/index.ts',
  /ipcRenderer\.invoke\('([^']+)'/g,
);

const preloadSend = extractChannels(
  'app/preload/index.ts',
  /ipcRenderer\.send\('([^']+)'/g,
);

// The preload uses helper functions on('channel', cb) and on2('channel', cb)
// instead of calling ipcRenderer.on directly. Extract from both patterns.
const preloadOnDirect = extractChannels(
  'app/preload/index.ts',
  /ipcRenderer\.on\('([^']+)'/g,
);
const preloadOnHelper = extractChannels(
  'app/preload/index.ts',
  /\bon2?\('([^']+)'/g,
);
// Merge and deduplicate (exclude helper function definitions by filtering to colon-namespaced channels)
const preloadOn = [...new Set([...preloadOnDirect, ...preloadOnHelper])]
  .filter((ch) => ch.includes(':'))
  .sort();

// Collect all handler registrations from main/ipc/*.ts and main/index.ts
function collectHandlerChannels(): { handle: string[]; on: string[] } {
  const ipcDir = path.join(APP_DIR, 'app/main/ipc');
  const files = fs.readdirSync(ipcDir).filter((f) => f.endsWith('.ts'));
  const handle: string[] = [];
  const on: string[] = [];

  // Match both single-line: ipcMain.handle('channel', ...)
  // and multi-line:  ipcMain.handle(\n    'channel', ...)
  const handleRe = /ipcMain\.handle\(\s*'([^']+)'/g;
  const onRe = /ipcMain\.on\(\s*'([^']+)'/g;

  for (const file of files) {
    const content = fs.readFileSync(path.join(ipcDir, file), 'utf-8');
    for (const m of content.matchAll(handleRe)) {
      handle.push(m[1]);
    }
    for (const m of content.matchAll(onRe)) {
      on.push(m[1]);
    }
  }

  // Also check main/index.ts for any inline handlers
  const indexContent = fs.readFileSync(path.join(APP_DIR, 'app/main/index.ts'), 'utf-8');
  for (const m of indexContent.matchAll(/ipcMain\.handle\(\s*'([^']+)'/g)) {
    handle.push(m[1]);
  }
  for (const m of indexContent.matchAll(/ipcMain\.on\(\s*'([^']+)'/g)) {
    on.push(m[1]);
  }

  return { handle: handle.sort(), on: on.sort() };
}

const handlers = collectHandlerChannels();

// Collect all main->renderer event channels (webContents.send)
function collectRendererEvents(): string[] {
  const dirs = ['app/main/ipc', 'app/main/services', 'app/main/index.ts'];
  const events = new Set<string>();

  for (const dir of dirs) {
    const fullPath = path.join(APP_DIR, dir);
    const stat = fs.statSync(fullPath);
    const files = stat.isDirectory()
      ? fs.readdirSync(fullPath).filter((f) => f.endsWith('.ts')).map((f) => path.join(fullPath, f))
      : [fullPath];

    for (const file of files) {
      const content = fs.readFileSync(file, 'utf-8');
      for (const m of content.matchAll(/\.send\('([^']+)'/g)) {
        events.add(m[1]);
      }
    }
  }
  return [...events].sort();
}

const rendererEvents = collectRendererEvents();

describe('IPC Channel Wiring', () => {
  describe('preload invoke channels have matching ipcMain.handle', () => {
    for (const channel of preloadInvoke) {
      it(`handler exists for invoke('${channel}')`, () => {
        expect(handlers.handle).toContain(channel);
      });
    }
  });

  describe('preload send channels have matching ipcMain.on', () => {
    for (const channel of preloadSend) {
      it(`handler exists for send('${channel}')`, () => {
        expect(handlers.on).toContain(channel);
      });
    }
  });

  describe('preload on channels receive events from main', () => {
    for (const channel of preloadOn) {
      it(`main sends event '${channel}'`, () => {
        expect(rendererEvents).toContain(channel);
      });
    }
  });

  it('no duplicate handle registrations', () => {
    const seen = new Set<string>();
    const duplicates: string[] = [];
    for (const ch of handlers.handle) {
      if (seen.has(ch)) duplicates.push(ch);
      seen.add(ch);
    }
    expect(duplicates).toEqual([]);
  });

  it('no duplicate on registrations', () => {
    const seen = new Set<string>();
    const duplicates: string[] = [];
    for (const ch of handlers.on) {
      if (seen.has(ch)) duplicates.push(ch);
      seen.add(ch);
    }
    expect(duplicates).toEqual([]);
  });
});
