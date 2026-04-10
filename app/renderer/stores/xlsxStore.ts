import { create } from 'zustand';
import type { XlsxData, CellSelection, CellData, SheetData } from '../../shared/types';
import { useChatStore } from './chatStore';

function colLetter(col: number): string {
  let s = '';
  let c = col;
  while (c >= 0) {
    s = String.fromCharCode(65 + (c % 26)) + s;
    c = Math.floor(c / 26) - 1;
  }
  return s;
}

function baseName(fp: string): string {
  return fp.replace(/\\/g, '/').split('/').pop() || fp;
}

interface XlsxState {
  data: XlsxData | null;
  activeSheet: number;
  selection: CellSelection | null;
  selectedCells: CellData[];
  annotationOpen: boolean;
  filePath: string | null;
  errors: CellError[];

  loadFile: (filePath: string) => Promise<void>;
  setActiveSheet: (index: number) => void;
  setSelection: (sel: CellSelection | null) => void;
  toggleAnnotation: () => void;
  closeAnnotation: () => void;
  scanErrors: () => void;
  clear: () => void;
}

export interface CellError {
  row: number;
  col: number;
  address: string;
  type: 'variance' | 'formula_error' | 'verification';
  value: string | number | boolean | null;
  severity: 'error' | 'warning';
}

export const useXlsxStore = create<XlsxState>((set, get) => ({
  data: null,
  activeSheet: 0,
  selection: null,
  selectedCells: [],
  annotationOpen: false,
  filePath: null,
  errors: [],

  loadFile: async (filePath: string) => {
    console.log(`[xlsxStore] loadFile called: ${filePath}`);
    try {
      const data = await window.api.parseXlsx(filePath);

      // Check if the result contains an error
      if ((data as any).error) {
        const errMsg = (data as any).message || 'Unknown error';
        console.error(`[xlsxStore] Parse returned error:`, errMsg);
        useChatStore.getState().addSystemMessage(`Failed to load spreadsheet: ${errMsg}`);
        return;
      }

      // Validate data structure
      if (!data || !data.sheets || data.sheets.length === 0) {
        console.error(`[xlsxStore] Invalid data structure:`, data);
        useChatStore.getState().addSystemMessage(`Failed to load spreadsheet: No valid sheets found`);
        return;
      }

      console.log(`[xlsxStore] Loaded ${data.sheets.length} sheets`);
      set({ data, filePath, activeSheet: 0, selection: null, selectedCells: [], annotationOpen: false });

      // Scan for errors after loading
      setTimeout(() => {
        get().scanErrors();
        // Post load summary to chat
        const { errors } = get();
        const sheet = data.sheets[0];
        const sheetCount = data.sheets.length;
        const sheetNames = data.sheets.map((s: SheetData) => s.name).join(', ');
        const summary = [
          `Spreadsheet loaded: "${baseName(filePath)}"`,
          `${sheetCount} sheet${sheetCount > 1 ? 's' : ''} (${sheetNames}), ${sheet.rowCount} rows × ${sheet.colCount} cols`,
        ];
        if (errors.length > 0) {
          const varCount = errors.filter((e) => e.type === 'variance').length;
          const fmtCount = errors.filter((e) => e.type === 'formula_error').length;
          const verCount = errors.filter((e) => e.type === 'verification').length;
          const parts: string[] = [];
          if (varCount) parts.push(`${varCount} variance error${varCount > 1 ? 's' : ''}`);
          if (fmtCount) parts.push(`${fmtCount} formula error${fmtCount > 1 ? 's' : ''}`);
          if (verCount) parts.push(`${verCount} verification error${verCount > 1 ? 's' : ''}`);
          summary.push(`⚠ ${errors.length} error${errors.length > 1 ? 's' : ''} detected: ${parts.join(', ')}`);
        } else {
          summary.push('No errors detected');
        }
        useChatStore.getState().addSystemMessage(summary.join('. '));
      }, 0);
    } catch (err: any) {
      console.error(`[xlsxStore] loadFile exception:`, err);
      console.error(`[xlsxStore] Error details:`, {
        name: err?.name,
        message: err?.message,
        stack: err?.stack,
      });
      useChatStore.getState().addSystemMessage(`Failed to load spreadsheet: ${err?.message || 'Unknown error'}`);
    }
  },

  setActiveSheet: (index: number) => {
    const { data } = get();
    set({ activeSheet: index, selection: null, selectedCells: [] });
    if (data && data.sheets[index]) {
      useChatStore.getState().addSystemMessage(
        `Switched to sheet "${data.sheets[index].name}" (${data.sheets[index].rowCount} rows)`
      );
    }
    // Re-scan errors for new sheet
    setTimeout(() => get().scanErrors(), 0);
  },

  setSelection: (sel: CellSelection | null) => {
    if (!sel) {
      set({ selection: null, selectedCells: [] });
      return;
    }

    const { data, activeSheet } = get();
    if (!data) return;

    const sheet = data.sheets[activeSheet];
    const cells: CellData[] = [];

    const minRow = Math.min(sel.startRow, sel.endRow);
    const maxRow = Math.max(sel.startRow, sel.endRow);
    const minCol = Math.min(sel.startCol, sel.endCol);
    const maxCol = Math.max(sel.startCol, sel.endCol);

    for (let r = minRow; r <= maxRow; r++) {
      for (let c = minCol; c <= maxCol; c++) {
        if (sheet.rows[r]?.[c]) {
          cells.push(sheet.rows[r][c]);
        }
      }
    }

    set({ selection: sel, selectedCells: cells });

    // Log selection to chat — only for meaningful selections (not single empty cells)
    if (cells.length > 0) {
      const rangeStr = minRow === maxRow && minCol === maxCol
        ? `${colLetter(minCol)}${minRow + 1}`
        : `${colLetter(minCol)}${minRow + 1}:${colLetter(maxCol)}${maxRow + 1}`;

      // Summarize cell values for the LLM
      const nonEmpty = cells.filter((c) => c.type !== 'empty' && c.value != null);
      if (nonEmpty.length > 0) {
        const preview = nonEmpty.slice(0, 5).map((c) => {
          const addr = c.address || `${colLetter(c.col)}${c.row + 1}`;
          return `${addr}=${c.value}`;
        });
        const more = nonEmpty.length > 5 ? ` (+${nonEmpty.length - 5} more)` : '';
        useChatStore.getState().addSystemMessage(
          `Selected cells ${rangeStr}: ${preview.join(', ')}${more}`
        );
      }
    }
  },

  toggleAnnotation: () => set((state) => ({ annotationOpen: !state.annotationOpen })),
  closeAnnotation: () => set({ annotationOpen: false }),

  scanErrors: () => {
    const { data, activeSheet } = get();
    if (!data) return;

    const sheet = data.sheets[activeSheet];
    const errors: CellError[] = [];

    for (let r = 0; r < sheet.rows.length; r++) {
      for (let c = 0; c < sheet.rows[r].length; c++) {
        const cell = sheet.rows[r][c];
        if (!cell || cell.type === 'empty') continue;

        const val = String(cell.value || '');

        // Formula errors
        if (['#REF!', '#VALUE!', '#DIV/0!', '#NAME?', '#N/A'].includes(val)) {
          errors.push({
            row: r, col: c, address: cell.address,
            type: 'formula_error', value: cell.value, severity: 'error',
          });
        }

        // Variance column (R = col 17)
        if (c === 17 && cell.type === 'number' && typeof cell.value === 'number') {
          if (Math.abs(cell.value) > 0.001) {
            errors.push({
              row: r, col: c, address: cell.address,
              type: 'variance', value: cell.value, severity: 'error',
            });
          }
        }

        // Check for VARIANCE CHECK / GROUP VERIFICATION labels
        if (typeof cell.value === 'string') {
          if (cell.value.toUpperCase().includes('VARIANCE CHECK') || cell.value.toUpperCase().includes('GROUP VERIFICATION')) {
            // Check the P column (col 15) value on this row
            const checkCell = sheet.rows[r]?.[15];
            if (checkCell && typeof checkCell.value === 'number' && Math.abs(checkCell.value) > 0.001) {
              errors.push({
                row: r, col: 15, address: checkCell.address,
                type: 'verification', value: checkCell.value, severity: 'error',
              });
            }
          }
        }
      }
    }

    set({ errors });
  },

  clear: () => {
    set({
      data: null, activeSheet: 0, selection: null,
      selectedCells: [], annotationOpen: false, filePath: null, errors: [],
    });
  },
}));
