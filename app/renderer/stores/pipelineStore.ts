import { create } from 'zustand';
import type {
  PipelineStatus,
  PipelineProgress,
  PipelineReport,
  ExtractionStatus,
  ExtractionProgress,
  ExtractTextResult,
} from '../../shared/types';

interface PipelineState extends PipelineStatus {
  // Pipeline
  run: (inputFile: string, outputFile?: string, stage?: string) => void;
  handleProgress: (progress: PipelineProgress) => void;
  handleComplete: (report: PipelineReport) => void;
  handleError: (error: string) => void;
  reset: () => void;

  // Extraction
  extraction: ExtractionStatus;
  extractionProgress: ExtractionProgress | undefined;
  extractionResult: ExtractTextResult | undefined;
  extractionError: string | undefined;
  extractText: (inputPdf: string, outputTxt: string) => void;
  handleExtractionProgress: (progress: ExtractionProgress) => void;
  handleExtractionComplete: (result: ExtractTextResult) => void;
  handleExtractionError: (error: string) => void;
  resetExtraction: () => void;
}

export const usePipelineStore = create<PipelineState>((set) => ({
  // Pipeline state
  state: 'idle',
  currentStage: undefined,
  progress: undefined,
  report: undefined,
  error: undefined,

  run: (inputFile: string, outputFile?: string, stage?: string) => {
    set({ state: 'running', error: undefined, report: undefined, progress: undefined });
    window.api.runPipeline(inputFile, outputFile, stage);
  },

  handleProgress: (progress: PipelineProgress) => {
    set({ progress, currentStage: progress.stage });
  },

  handleComplete: (report: PipelineReport) => {
    set({
      state: report.status === 'success' ? 'success' : 'error',
      report,
      progress: undefined,
      currentStage: undefined,
    });
  },

  handleError: (error: string) => {
    set({ state: 'error', error, progress: undefined, currentStage: undefined });
  },

  reset: () => {
    set({ state: 'idle', currentStage: undefined, progress: undefined, report: undefined, error: undefined });
  },

  // Extraction state
  extraction: 'idle',
  extractionProgress: undefined,
  extractionResult: undefined,
  extractionError: undefined,

  extractText: (inputPdf: string, outputTxt: string) => {
    set({ extraction: 'extracting', extractionError: undefined, extractionResult: undefined, extractionProgress: undefined });
    window.api.extractText(inputPdf, outputTxt);
  },

  handleExtractionProgress: (progress: ExtractionProgress) => {
    set({ extractionProgress: progress });
  },

  handleExtractionComplete: (result: ExtractTextResult) => {
    set({
      extraction: result.status === 'success' ? 'success' : 'error',
      extractionResult: result,
      extractionProgress: undefined,
    });
  },

  handleExtractionError: (error: string) => {
    set({ extraction: 'error', extractionError: error, extractionProgress: undefined });
  },

  resetExtraction: () => {
    set({ extraction: 'idle', extractionProgress: undefined, extractionResult: undefined, extractionError: undefined });
  },
}));
