export interface XlsxData {
  fileName: string;
  sheets: SheetData[];
  activeSheet: number;
}

export interface SheetData {
  name: string;
  rows: CellData[][];
  colCount: number;
  rowCount: number;
  colWidths: number[];
  merges?: MergeRange[];
}

export interface CellData {
  value: string | number | boolean | null;
  formula?: string;
  type: 'string' | 'number' | 'boolean' | 'date' | 'error' | 'empty';
  style?: CellStyle;
  address: string;
  row: number;
  col: number;
}

export interface CellStyle {
  bold?: boolean;
  italic?: boolean;
  fontSize?: number;
  fontColor?: string;
  bgColor?: string;
  numFmt?: string;
  alignment?: 'left' | 'center' | 'right';
  borderBottom?: boolean;
  borderRight?: boolean;
}

export interface MergeRange {
  startRow: number;
  startCol: number;
  endRow: number;
  endCol: number;
}

export interface CellSelection {
  startRow: number;
  startCol: number;
  endRow: number;
  endCol: number;
}

export interface CellAnnotation {
  selection: CellSelection;
  cells: CellData[];
  message: string;
}
