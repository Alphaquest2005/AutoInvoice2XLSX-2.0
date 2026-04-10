export interface PipelineStatus {
  state: 'idle' | 'running' | 'success' | 'error';
  currentStage?: string;
  progress?: PipelineProgress;
  report?: PipelineReport;
  error?: string;
}

export interface PipelineProgress {
  stage: string;
  item?: number;
  total?: number;
  message?: string;
}

export interface PipelineReport {
  status: string;
  started: string;
  completed?: string;
  input: string;
  output: string;
  stages: StageResult[];
  errors: string[];
  warnings: string[];
}

export interface StageResult {
  name: string;
  type: string;
  status: string;
  started: string;
  completed?: string;
  error?: string;
}

export type ExtractionStatus = 'idle' | 'extracting' | 'success' | 'error';

export interface ExtractionProgress {
  stage: string;
  message: string;
  item: number;
  total: number;
}

export interface ExtractTextResult {
  status: 'success' | 'error';
  method: string;
  char_count: number;
  output: string;
  error?: string;
}
