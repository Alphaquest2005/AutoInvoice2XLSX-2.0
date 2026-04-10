export interface AsycudaImportResult {
  success: boolean;
  importId?: number;
  imported?: number;
  skipped?: number;
  corrected?: number;
  corrections?: AsycudaCorrection[];
  error?: string;
}

export interface AsycudaCorrection {
  sku: string;
  old_code: string;
  new_code: string;
}

export interface AsycudaImport {
  id: number;
  file_path: string;
  declaration_type: string | null;
  registration_number: string | null;
  registration_date: string | null;
  items_count: number;
  imported_at: string;
}

export interface AsycudaClassification {
  found: boolean;
  source?: 'asycuda' | 'rules';
  hs_code?: string;
  description?: string;
  commercial_description?: string;
  country_of_origin?: string;
  source_file?: string;
  created_at?: string;
}

export interface AsycudaMultiImportResult {
  success: boolean;
  total_files: number;
  successful: number;
  total_imported: number;
  total_corrected: number;
  results: AsycudaImportResult[];
}

export interface CostingSheetResult {
  success: boolean;
  output_path?: string;
  message?: string;
  error?: string;
}

export interface CetStats {
  codes: number;
  aliases: number;
  chapters: number[];
  chapter_count: number;
  asycuda_classifications: number;
  corrections: number;
}
