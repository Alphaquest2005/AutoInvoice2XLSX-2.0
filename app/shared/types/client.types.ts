export interface EmailCredentials {
  address: string;
  server: string;
  port: number;
  password: string;
  ssl: boolean;
}

export interface ClientSettings {
  id: string;
  name: string;
  enabled: boolean;
  createdAt: string;
  updatedAt: string;
  incomingEmail: EmailCredentials;
  outgoingEmail: EmailCredentials;
  watchFolder: string;
  outputRecipients: string[];
  developerEmail: string;
  autoProcess: boolean;
  markAsReadAfterProcessing: boolean;
}

export interface ProcessedEmail {
  id: string;
  clientId: string;
  messageId: string;
  subject: string;
  from: string;
  receivedAt: string;
  processedAt: string;
  status: 'saving' | 'files_ready' | 'pipeline_running' | 'pipeline_done' | 'email_sending' | 'completed' | 'error' | 'needs_review';
  waybillNumber?: string;
  invoiceNumber?: string;
  outputFiles?: string[];
  error?: string;
  emailSent?: boolean;
  inputDir?: string;
  retryCount?: number;
  docTypes?: string[];
  linkedRecordId?: string;
}

export interface EmailServiceStatus {
  clientId: string;
  connected: boolean;
  lastCheck: Date | null;
  lastError: string | null;
  emailsProcessed: number;
}

export interface IncomingEmail {
  messageId: string;
  uid: number;
  subject: string;
  from: string;
  to: string;
  date: Date;
  body: string;
  attachments: { filename: string; contentType: string; size: number }[];
}

export interface EmailClassification {
  bill_of_lading: string[];
  manifest: string[];
  declaration: string[];
  invoice: string[];
  packing_list: string[];
  unknown: string[];
}

export interface BLMetadata {
  bl_number: string;
  consignee: string;
  invoice_refs: string[];
  shipper_names: string[];
}

export interface ClassificationResult {
  classification: EmailClassification;
  has_bl: boolean;
  has_invoices: boolean;
  bl_metadata: BLMetadata;
}

export interface PdfSplitResult {
  success: boolean;
  status?: string;
  input_path?: string;
  total_pages?: number;
  pages?: {
    page_num: number;
    doc_type: 'declaration' | 'invoice' | 'unknown';
    confidence: number;
    keywords: string[];
  }[];
  output_files?: {
    declaration?: string;
    invoice?: string;
    unknown?: string;
  };
  declaration_metadata?: {
    waybill?: string;
    customs_file?: string;
    consignee?: string;
    packages?: string;
    weight?: string;
    country_origin?: string;
    fob_value?: string;
  };
  error?: string;
}
