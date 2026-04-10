/**
 * Shared type definitions - re-exports from modular type files.
 *
 * SSOT: Each type is defined in exactly one file under shared/types/.
 * This file re-exports everything for backward compatibility.
 */

// ─── Chat Types ─────────────────────────────────────────────
export type { Conversation, Message, ToolUse, ToolResult, StreamingChunk } from './types/chat.types';

// ─── File Types ─────────────────────────────────────────────
export type { FileNode, RecentFile } from './types/file.types';

// ─── XLSX Types ─────────────────────────────────────────────
export type {
  XlsxData, SheetData, CellData, CellStyle,
  MergeRange, CellSelection, CellAnnotation,
} from './types/xlsx.types';

// ─── Pipeline Types ─────────────────────────────────────────
export type {
  PipelineStatus, PipelineProgress, PipelineReport, StageResult,
  ExtractionProgress, ExtractTextResult,
} from './types/pipeline.types';
export type { ExtractionStatus } from './types/pipeline.types';

// ─── ASYCUDA Import Types ────────────────────────────────────
export type {
  AsycudaImportResult, AsycudaCorrection, AsycudaImport,
  AsycudaClassification, AsycudaMultiImportResult,
  CostingSheetResult, CetStats,
} from './types/asycuda.types';

// ─── Settings Types ─────────────────────────────────────────
export type { AppSettings } from './types/settings.types';

// ─── Client Email Types ─────────────────────────────────────
export type {
  EmailCredentials, ClientSettings, ProcessedEmail,
  EmailServiceStatus, IncomingEmail,
  EmailClassification, BLMetadata, ClassificationResult,
  PdfSplitResult,
} from './types/client.types';

// ─── IPC & Session Types ────────────────────────────────────
export type { IpcApi, SessionState, ShutdownStatus } from './types/ipc-api.types';
