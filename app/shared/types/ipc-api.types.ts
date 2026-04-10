import type { Conversation, Message, StreamingChunk } from './chat.types';
import type { FileNode } from './file.types';
import type { XlsxData } from './xlsx.types';
import type {
  PipelineProgress,
  PipelineReport,
  ExtractionProgress,
  ExtractTextResult,
} from './pipeline.types';
import type {
  AsycudaImportResult,
  AsycudaMultiImportResult,
  AsycudaImport,
  AsycudaClassification,
  CostingSheetResult,
  CetStats,
} from './asycuda.types';
import type { AppSettings } from './settings.types';
import type {
  ClientSettings,
  ProcessedEmail,
  EmailServiceStatus,
  IncomingEmail,
  PdfSplitResult,
} from './client.types';

export interface SessionState {
  activeConversationId: string | null;
  wasLlmBusy: boolean;
  shutdownTime: string;
  resumePrompt: string;
}

export interface ShutdownStatus {
  status: 'waiting' | 'closing' | 'cancelled';
  message: string;
}

export interface IpcApi {
  // -- Chat --
  sendMessage(conversationId: string, content: string, attachments?: string[]): void;
  addSystemMessage(conversationId: string, content: string): Promise<void>;
  onStreamChunk(callback: (chunk: StreamingChunk) => void): () => void;
  getConversations(): Promise<Conversation[]>;
  getMessages(conversationId: string): Promise<Message[]>;
  createConversation(title?: string): Promise<Conversation>;
  deleteConversation(id: string): Promise<void>;

  // -- Files --
  getFileTree(dirPath?: string): Promise<FileNode>;
  readFile(filePath: string): Promise<string>;
  readBinary(filePath: string): Promise<string>;
  writeFile(filePath: string, content: string): Promise<void>;
  openFileDialog(filters?: { name: string; extensions: string[] }[]): Promise<string | null>;
  openFolderDialog(): Promise<string | null>;
  copyToWorkspace(sourcePath: string, subdir?: string): Promise<string>;
  deleteFile(filePath: string): Promise<void>;
  renameFile(oldPath: string, newPath: string): Promise<void>;
  copyFileTo(sourcePath: string, destPath: string): Promise<void>;
  createFolder(dirPath: string): Promise<void>;
  createFile(filePath: string): Promise<void>;
  showInExplorer(filePath: string): void;
  openExternal(filePath: string): Promise<{ success: boolean; error?: string }>;
  getFilePath(file: File): string;
  onFileChanged(callback: (event: string, path: string) => void): () => void;

  // -- XLSX --
  parseXlsx(filePath: string): Promise<XlsxData>;
  openInExcel(filePath: string): Promise<{ success: boolean; error?: string }>;
  combineXlsx(filePaths: string[]): Promise<{ success: boolean; outputPath?: string; error?: string }>;
  onXlsxAutoLoad(callback: (filePath: string) => void): () => void;

  // -- Pipeline --
  runPipeline(inputFile: string, outputFile?: string, stage?: string): void;
  runFolderPipeline(folderPath: string): void;
  runFolderBatchPipeline(folderPath: string): void;
  onPipelineProgress(callback: (progress: PipelineProgress) => void): () => void;
  onPipelineComplete(callback: (report: PipelineReport) => void): () => void;
  onPipelineError(callback: (error: string) => void): () => void;
  onPipelineFailures?(callback: (data: { conversationId: string; failures: any[]; inputDir: string; prompt: string }) => void): () => void;
  onValidationIssues?(callback: (data: { conversationId: string; validation: any; inputDir: string; outputDir: string; prompt: string }) => void): () => void;
  onChecklistFailed?(callback: (data: { conversationId: string; checklist: any; emailParamsPath: string; inputDir: string; outputDir: string; prompt: string }) => void): () => void;
  rerunBLPipeline(inputDir: string): void;

  // -- Extraction --
  extractText(inputPdf: string, outputTxt: string): void;
  onExtractionProgress(callback: (progress: ExtractionProgress) => void): () => void;
  onExtractionComplete(callback: (result: ExtractTextResult) => void): () => void;
  onExtractionError(callback: (error: string) => void): () => void;

  // -- ASYCUDA --
  importAsycudaXml(xmlPath: string): Promise<AsycudaImportResult>;
  importAsycudaMultiple(xmlPaths: string[]): Promise<AsycudaMultiImportResult>;
  getSkuClassification(sku: string): Promise<AsycudaClassification>;
  getSkuCorrections(sku: string): Promise<{ sku: string; corrections: any[] }>;
  getAsycudaImports(): Promise<{ imports: AsycudaImport[] }>;
  getAsycudaStats(): Promise<CetStats>;
  browseAsycudaXml(): Promise<string[]>;
  generateCostingSheet(xmlPath: string, outputPath?: string): Promise<CostingSheetResult>;

  // -- Settings --
  getSettings(): Promise<AppSettings>;
  saveSettings(settings: Partial<AppSettings>): Promise<void>;
  getApiKey(): Promise<string>;
  setApiKey(key: string): Promise<void>;

  // -- Client Management --
  getClients(): Promise<ClientSettings[]>;
  getClient(id: string): Promise<ClientSettings | null>;
  createClient(settings: Omit<ClientSettings, 'id' | 'createdAt' | 'updatedAt'>): Promise<ClientSettings>;
  updateClient(id: string, updates: Partial<ClientSettings>): Promise<ClientSettings | null>;
  deleteClient(id: string): Promise<boolean>;
  getProcessedEmails(clientId?: string, limit?: number): Promise<ProcessedEmail[]>;
  selectWatchFolder(): Promise<string | null>;

  // -- Email Service --
  startEmailMonitor(clientId: string): Promise<{ success: boolean; error?: string }>;
  stopEmailMonitor(clientId: string): Promise<{ success: boolean; error?: string }>;
  getEmailStatus(clientId: string): Promise<EmailServiceStatus | null>;
  getAllEmailStatuses(): Promise<EmailServiceStatus[]>;
  sendTestEmail(clientId: string, to: string): Promise<{ success: boolean; error?: string }>;
  onEmailReceived(callback: (data: { email: IncomingEmail; clientId: string }) => void): () => void;
  onEmailError(callback: (data: { error: string; clientId: string }) => void): () => void;
  onEmailStatus(callback: (status: EmailServiceStatus) => void): () => void;
  onEmailProgress(callback: (data: { clientId: string; message: string }) => void): () => void;
  resendShipmentEmail(paramsPath: string): Promise<{ success: boolean; error?: string }>;
  onEmailAutoResent(callback: (data: { paramsPath: string; shipmentDir: string; recordId: string | number }) => void): () => void;

  // -- PDF Processing --
  splitPdf(pdfPath: string, outputDir?: string): Promise<PdfSplitResult>;
  reorderPdf(pdfPath: string, pageOrder: number[]): Promise<{ success: boolean; output?: string; error?: string }>;
  getPdfPageCount(pdfPath: string): Promise<{ success: boolean; page_count?: number; error?: string }>;

  // -- Folder Processing --
  processFolder(folderPath: string, options?: { limit?: number; start?: number; outputDir?: string }): Promise<any>;
  selectFolder(): Promise<string | null>;
  onFolderProgress(callback: (progress: string) => void): () => void;

  // -- Window --
  minimizeWindow(): void;
  maximizeWindow(): void;
  closeWindow(): void;

  // -- Zoom --
  zoomIn(): void;
  zoomOut(): void;
  zoomReset(): void;
  getZoomLevel(): Promise<number>;

  // -- Graceful Shutdown --
  requestShutdown(activeConversationId: string | null): void;
  cancelShutdown(): void;
  forceShutdown(activeConversationId: string | null): void;
  notifyLlmBusy(busy: boolean): void;
  getPendingSession(): Promise<SessionState | null>;
  clearPendingSession(): void;
  onShutdownStatus(callback: (status: ShutdownStatus) => void): () => void;
}
