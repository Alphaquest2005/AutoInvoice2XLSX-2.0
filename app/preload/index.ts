/**
 * Preload script: exposes a typed IPC API to the renderer via contextBridge.
 *
 * Every method here maps 1-to-1 to an IPC channel handled in app/main/ipc/.
 * The IpcApi interface (shared/types) is the single source of truth for the
 * contract between renderer and main process.
 */

import { contextBridge, ipcRenderer, webUtils } from 'electron';
import type { IpcApi } from '../shared/types';

// ─── Helpers ─────────────────────────────────────────────────────────────────

/** Subscribe to a main->renderer event; returns an unsubscribe function. */
function on<T>(channel: string, callback: (data: T) => void): () => void {
  const handler = (_event: Electron.IpcRendererEvent, data: T) => callback(data);
  ipcRenderer.on(channel, handler);
  return () => ipcRenderer.removeListener(channel, handler);
}

/** Same as `on` but the callback receives two positional args (event, arg1, arg2). */
function on2<A, B>(channel: string, callback: (a: A, b: B) => void): () => void {
  const handler = (_event: Electron.IpcRendererEvent, a: A, b: B) => callback(a, b);
  ipcRenderer.on(channel, handler);
  return () => ipcRenderer.removeListener(channel, handler);
}

// ─── IPC API ─────────────────────────────────────────────────────────────────

const api: IpcApi = {
  // -- Chat -------------------------------------------------------------------
  sendMessage: (conversationId, content, attachments?) =>
    ipcRenderer.send('chat:sendMessage', conversationId, content, attachments),
  addSystemMessage: (conversationId, content) =>
    ipcRenderer.invoke('chat:addSystemMessage', conversationId, content),
  onStreamChunk: (callback) => on('chat:streamChunk', callback),
  getConversations: () => ipcRenderer.invoke('chat:getConversations'),
  getMessages: (conversationId) => ipcRenderer.invoke('chat:getMessages', conversationId),
  createConversation: (title?) => ipcRenderer.invoke('chat:createConversation', title),
  deleteConversation: (id) => ipcRenderer.invoke('chat:deleteConversation', id),

  // -- Files ------------------------------------------------------------------
  getFileTree: (dirPath?) => ipcRenderer.invoke('files:getTree', dirPath),
  readFile: (filePath) => ipcRenderer.invoke('files:read', filePath),
  readBinary: (filePath) => ipcRenderer.invoke('files:readBinary', filePath),
  writeFile: (filePath, content) => ipcRenderer.invoke('files:write', filePath, content),
  openFileDialog: (filters?) => ipcRenderer.invoke('files:openDialog', filters),
  openFolderDialog: () => ipcRenderer.invoke('files:openFolderDialog'),
  copyToWorkspace: (sourcePath, subdir?) => ipcRenderer.invoke('files:copyToWorkspace', sourcePath, subdir),
  deleteFile: (filePath) => ipcRenderer.invoke('files:delete', filePath),
  renameFile: (oldPath, newPath) => ipcRenderer.invoke('files:rename', oldPath, newPath),
  copyFileTo: (sourcePath, destPath) => ipcRenderer.invoke('files:copy', sourcePath, destPath),
  createFolder: (dirPath) => ipcRenderer.invoke('files:createFolder', dirPath),
  createFile: (filePath) => ipcRenderer.invoke('files:createFile', filePath),
  showInExplorer: (filePath) => ipcRenderer.send('files:showInExplorer', filePath),
  openExternal: (filePath) => ipcRenderer.invoke('files:openExternal', filePath),
  getFilePath: (file) => webUtils.getPathForFile(file),
  onFileChanged: (callback) => on2('files:changed', callback),

  // -- XLSX -------------------------------------------------------------------
  parseXlsx: (filePath) => ipcRenderer.invoke('xlsx:parse', filePath),
  openInExcel: (filePath) => ipcRenderer.invoke('xlsx:openInExcel', filePath),
  combineXlsx: (filePaths) => ipcRenderer.invoke('xlsx:combine', filePaths),
  onXlsxAutoLoad: (callback) => on('xlsx:autoLoad', callback),

  // -- Pipeline ---------------------------------------------------------------
  runPipeline: (inputFile, outputFile?, stage?) =>
    ipcRenderer.send('pipeline:run', inputFile, outputFile, stage),
  runFolderPipeline: (folderPath) => ipcRenderer.send('pipeline:runFolder', folderPath),
  runFolderBatchPipeline: (folderPath) => ipcRenderer.send('pipeline:runFolderBatch', folderPath),
  onPipelineProgress: (callback) => on('pipeline:progress', callback),
  onPipelineComplete: (callback) => on('pipeline:complete', callback),
  onPipelineError: (callback) => on('pipeline:error', callback),
  onPipelineFailures: (callback) => on('pipeline:failures', callback),
  onValidationIssues: (callback) => on('pipeline:validationIssues', callback),
  onChecklistFailed: (callback) => on('pipeline:checklistFailed', callback),
  rerunBLPipeline: (inputDir) => ipcRenderer.send('pipeline:rerunBL', inputDir),

  // -- Extraction -------------------------------------------------------------
  extractText: (inputPdf, outputTxt) =>
    ipcRenderer.send('extraction:run', inputPdf, outputTxt),
  onExtractionProgress: (callback) => on('extraction:progress', callback),
  onExtractionComplete: (callback) => on('extraction:complete', callback),
  onExtractionError: (callback) => on('extraction:error', callback),

  // -- ASYCUDA ----------------------------------------------------------------
  importAsycudaXml: (xmlPath) => ipcRenderer.invoke('asycuda:importXml', xmlPath),
  importAsycudaMultiple: (xmlPaths) => ipcRenderer.invoke('asycuda:importMultiple', xmlPaths),
  getSkuClassification: (sku) => ipcRenderer.invoke('asycuda:getSkuClassification', sku),
  getSkuCorrections: (sku) => ipcRenderer.invoke('asycuda:getSkuCorrections', sku),
  getAsycudaImports: () => ipcRenderer.invoke('asycuda:getImports'),
  getAsycudaStats: () => ipcRenderer.invoke('asycuda:getStats'),
  browseAsycudaXml: () => ipcRenderer.invoke('asycuda:browseXmlFolder'),
  generateCostingSheet: (xmlPath, outputPath?) =>
    ipcRenderer.invoke('asycuda:generateCostingSheet', xmlPath, outputPath),

  // -- Settings ---------------------------------------------------------------
  getSettings: () => ipcRenderer.invoke('settings:get'),
  saveSettings: (settings) => ipcRenderer.invoke('settings:save', settings),
  getApiKey: () => ipcRenderer.invoke('settings:getApiKey'),
  setApiKey: (key) => ipcRenderer.invoke('settings:setApiKey', key),

  // -- Client Management ------------------------------------------------------
  getClients: () => ipcRenderer.invoke('clients:getAll'),
  getClient: (id) => ipcRenderer.invoke('clients:get', id),
  createClient: (settings) => ipcRenderer.invoke('clients:create', settings),
  updateClient: (id, updates) => ipcRenderer.invoke('clients:update', id, updates),
  deleteClient: (id) => ipcRenderer.invoke('clients:delete', id),
  getProcessedEmails: (clientId?, limit?) =>
    ipcRenderer.invoke('clients:getProcessedEmails', clientId, limit),
  selectWatchFolder: () => ipcRenderer.invoke('clients:selectWatchFolder'),

  // -- Email Service ----------------------------------------------------------
  startEmailMonitor: (clientId) => ipcRenderer.invoke('email:startMonitor', clientId),
  stopEmailMonitor: (clientId) => ipcRenderer.invoke('email:stopMonitor', clientId),
  getEmailStatus: (clientId) => ipcRenderer.invoke('email:getStatus', clientId),
  getAllEmailStatuses: () => ipcRenderer.invoke('email:getAllStatuses'),
  sendTestEmail: (clientId, to) => ipcRenderer.invoke('email:sendTest', clientId, to),
  onEmailReceived: (callback) => on('email:received', callback),
  onEmailError: (callback) => on('email:error', callback),
  onEmailStatus: (callback) => on('email:status', callback),
  onEmailProgress: (callback) => on('email:progress', callback),
  resendShipmentEmail: (paramsPath) => ipcRenderer.invoke('bl:resendEmail', paramsPath),
  onEmailAutoResent: (callback) => on('email:autoResent', callback),

  // -- PDF Processing ---------------------------------------------------------
  splitPdf: (pdfPath, outputDir?) => ipcRenderer.invoke('pdf:split', pdfPath, outputDir),
  reorderPdf: (pdfPath, pageOrder) => ipcRenderer.invoke('pdf:reorder', pdfPath, pageOrder),
  getPdfPageCount: (pdfPath) => ipcRenderer.invoke('pdf:getPageCount', pdfPath),

  // -- Folder Processing ------------------------------------------------------
  processFolder: (folderPath, options?) =>
    ipcRenderer.invoke('folder:process', folderPath, options),
  selectFolder: () => ipcRenderer.invoke('folder:select'),
  onFolderProgress: (callback) => on('folder:progress', callback),

  // -- Window -----------------------------------------------------------------
  minimizeWindow: () => ipcRenderer.send('window:minimize'),
  maximizeWindow: () => ipcRenderer.send('window:maximize'),
  closeWindow: () => ipcRenderer.send('window:close'),

  // -- Zoom -------------------------------------------------------------------
  zoomIn: () => ipcRenderer.send('zoom:in'),
  zoomOut: () => ipcRenderer.send('zoom:out'),
  zoomReset: () => ipcRenderer.send('zoom:reset'),
  getZoomLevel: () => ipcRenderer.invoke('zoom:get'),

  // -- Graceful Shutdown ------------------------------------------------------
  requestShutdown: (activeConversationId) =>
    ipcRenderer.send('shutdown:request', activeConversationId),
  cancelShutdown: () => ipcRenderer.send('shutdown:cancel'),
  forceShutdown: (activeConversationId) =>
    ipcRenderer.send('shutdown:force', activeConversationId),
  notifyLlmBusy: (busy) => ipcRenderer.send('llm:busy', busy),
  getPendingSession: () => ipcRenderer.invoke('shutdown:getPendingSession'),
  clearPendingSession: () => ipcRenderer.send('shutdown:clearSession'),
  onShutdownStatus: (callback) => on('shutdown:status', callback),
};

contextBridge.exposeInMainWorld('api', api);
