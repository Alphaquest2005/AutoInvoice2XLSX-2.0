/**
 * ASYCUDA SAD (Single Administrative Document) XML Import — Interface Definitions
 *
 * These interfaces define the data structures for importing verified tariff
 * classifications from ASYCUDA World customs declarations. Implementation
 * is deferred to a future phase.
 *
 * ASYCUDA XML Structure (SAD):
 *   <Declaration>
 *     <DeclarationHeader> — office, type, reference, date
 *     <GoodsItems>
 *       <GoodsItem> — HS code, description, quantity, value, duties, taxes
 *
 * Data flow: ASYCUDA XML → parse → update CET database + classification rules
 */

/** A single goods item from an ASYCUDA SAD declaration */
export interface AsycudaGoodsItem {
  itemNumber: number;
  hsCode: string;              // 8-digit HS code, verified by customs
  description: string;         // Official goods description
  countryOfOrigin: string;     // ISO country code
  quantity: number;
  quantityUnit: string;        // KG, L, U (units), etc.
  customsValue: number;        // CIF value
  currency: string;            // ISO currency code
  dutyRate: number;            // CET duty rate (percentage)
  dutyAmount: number;          // Calculated duty
  vatRate: number;             // VAT percentage
  vatAmount: number;           // Calculated VAT
  statisticalValue: number;
  supplementaryUnit?: string;
  supplementaryQuantity?: number;
  cpc?: string;                // Customs Procedure Code (e.g., "4000-000")
}

/** A full ASYCUDA SAD declaration */
export interface AsycudaDeclaration {
  referenceNumber: string;     // e.g., "2026/C001234"
  declarationType: string;     // IM4 (import), EX1 (export), etc.
  office: string;              // Customs office code
  date: string;                // Declaration date
  importer: string;
  exporter: string;
  countryOfExport: string;
  totalPackages: number;
  totalGrossWeight: number;
  totalCustomsValue: number;
  currency: string;
  items: AsycudaGoodsItem[];
}

/** Result of importing ASYCUDA data into the CET database */
export interface AsycudaImportResult {
  declarationsProcessed: number;
  newCetEntries: number;           // Codes added to CET database
  updatedCetEntries: number;       // Codes with updated duty rates
  newClassificationRules: number;  // Rules auto-generated from descriptions
  conflicts: AsycudaConflict[];    // Cases where data differs from existing
}

/** A conflict between ASYCUDA data and existing CET/rules data */
export interface AsycudaConflict {
  hsCode: string;
  field: string;                   // 'duty_rate', 'description', etc.
  existingValue: string;
  asycudaValue: string;
  resolution: 'keep_existing' | 'use_asycuda' | 'manual';
}

/**
 * Future tool schema for the LLM agent:
 *
 * {
 *   name: 'import_asycuda',
 *   description: 'Import ASYCUDA SAD XML file to update CET database with verified tariff codes, duty rates, and descriptions.',
 *   input_schema: {
 *     type: 'object',
 *     properties: {
 *       xml_path: { type: 'string', description: 'Path to ASYCUDA SAD XML file' },
 *       update_rules: { type: 'boolean', description: 'Auto-generate classification rules (default: true)' },
 *       conflict_resolution: {
 *         type: 'string',
 *         enum: ['keep_existing', 'prefer_asycuda', 'manual'],
 *         description: 'How to handle conflicts with existing data'
 *       },
 *     },
 *     required: ['xml_path'],
 *   },
 * }
 */

// Future implementation stubs:
// export function parseAsycudaXml(xmlPath: string): AsycudaDeclaration { ... }
// export function importToDatabase(declaration: AsycudaDeclaration): AsycudaImportResult { ... }
