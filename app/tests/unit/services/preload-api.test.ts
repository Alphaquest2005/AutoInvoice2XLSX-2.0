/**
 * Verify the preload script exposes all IpcApi methods
 * and maps them to the correct IPC channels.
 */
import { describe, it, expect } from 'vitest';
import fs from 'fs';
import path from 'path';

const APP_DIR = path.resolve(__dirname, '../../../..');

function readFile(relPath: string): string {
  return fs.readFileSync(path.join(APP_DIR, relPath), 'utf-8');
}

const preloadCode = readFile('app/preload/index.ts');

// Extract all IpcApi method names from the types
// SSOT: IpcApi interface is defined in shared/types/ipc-api.types.ts
// shared/types.ts re-exports from modular files.
function extractIpcApiMethods(): string[] {
  const ipcApiFile = path.join(APP_DIR, 'app/shared/types/ipc-api.types.ts');
  const typesFile = fs.existsSync(ipcApiFile)
    ? readFile('app/shared/types/ipc-api.types.ts')
    : readFile('app/shared/types.ts');

  const ipcApiMatch = typesFile.match(/interface IpcApi\s*\{([\s\S]*?)\n\}/);
  if (!ipcApiMatch) return [];

  const body = ipcApiMatch[1];
  const methods = [...body.matchAll(/^\s+(\w+)[\?:]?\s*[:(]/gm)];
  return methods.map((m) => m[1]);
}

const ipcApiMethods = extractIpcApiMethods();

describe('Preload API Completeness', () => {
  it('IpcApi has methods defined', () => {
    expect(ipcApiMethods.length).toBeGreaterThan(50);
  });

  for (const method of ipcApiMethods) {
    it(`preload exposes '${method}'`, () => {
      // The preload should have this method name as a key in the api object
      expect(preloadCode).toContain(`${method}:`);
    });
  }
});

describe('Preload Channel Mapping', () => {
  // Helper: check that a method uses the expected IPC pattern (handles multi-line lambdas)
  function methodUsesPattern(method: string, pattern: string): boolean {
    const methodIdx = preloadCode.indexOf(`${method}:`);
    if (methodIdx === -1) return false;
    // Look at the next 200 chars after the method name for the IPC call
    const snippet = preloadCode.slice(methodIdx, methodIdx + 200);
    return snippet.includes(pattern);
  }

  it('sendMessage uses send (fire-and-forget, not invoke)', () => {
    expect(methodUsesPattern('sendMessage', 'ipcRenderer.send')).toBe(true);
  });

  it('getConversations uses invoke (returns promise)', () => {
    expect(methodUsesPattern('getConversations', 'ipcRenderer.invoke')).toBe(true);
  });

  it('runPipeline uses send (fire-and-forget)', () => {
    expect(methodUsesPattern('runPipeline', 'ipcRenderer.send')).toBe(true);
  });

  it('parseXlsx uses invoke (returns data)', () => {
    expect(methodUsesPattern('parseXlsx', 'ipcRenderer.invoke')).toBe(true);
  });

  it('window controls use send (fire-and-forget)', () => {
    expect(methodUsesPattern('minimizeWindow', 'ipcRenderer.send')).toBe(true);
    expect(methodUsesPattern('maximizeWindow', 'ipcRenderer.send')).toBe(true);
    expect(methodUsesPattern('closeWindow', 'ipcRenderer.send')).toBe(true);
  });

  it('shutdown uses correct channel names', () => {
    expect(methodUsesPattern('requestShutdown', "send('shutdown:request'")).toBe(true);
    expect(methodUsesPattern('cancelShutdown', "send('shutdown:cancel'")).toBe(true);
    expect(methodUsesPattern('forceShutdown', "send('shutdown:force'")).toBe(true);
    expect(methodUsesPattern('getPendingSession', "invoke('shutdown:getPendingSession'")).toBe(true);
    expect(methodUsesPattern('clearPendingSession', "send('shutdown:clearSession'")).toBe(true);
  });

  it('zoom uses correct channel names', () => {
    expect(methodUsesPattern('zoomIn', "send('zoom:in'")).toBe(true);
    expect(methodUsesPattern('zoomOut', "send('zoom:out'")).toBe(true);
    expect(methodUsesPattern('zoomReset', "send('zoom:reset'")).toBe(true);
    expect(methodUsesPattern('getZoomLevel', "invoke('zoom:get'")).toBe(true);
  });
});

describe('Preload Event Listeners', () => {
  it('onStreamChunk listens on chat:streamChunk', () => {
    expect(preloadCode).toMatch(/onStreamChunk:.*'chat:streamChunk'/);
  });

  it('onFileChanged listens on files:changed', () => {
    expect(preloadCode).toMatch(/onFileChanged:.*'files:changed'/);
  });

  it('onPipelineProgress listens on pipeline:progress', () => {
    expect(preloadCode).toMatch(/onPipelineProgress:.*'pipeline:progress'/);
  });

  it('onPipelineComplete listens on pipeline:complete', () => {
    expect(preloadCode).toMatch(/onPipelineComplete:.*'pipeline:complete'/);
  });

  it('onEmailReceived listens on email:received', () => {
    expect(preloadCode).toMatch(/onEmailReceived:.*'email:received'/);
  });

  it('onShutdownStatus listens on shutdown:status', () => {
    expect(preloadCode).toMatch(/onShutdownStatus:.*'shutdown:status'/);
  });
});
