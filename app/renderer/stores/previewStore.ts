import { create } from 'zustand';

export type PreviewType = 'none' | 'xlsx' | 'pdf' | 'txt';

interface PreviewState {
  previewType: PreviewType;
  previewPath: string | null;
  textContent: string | null;
  pdfBase64: string | null;
  loading: boolean;

  openFile: (filePath: string) => Promise<void>;
  clear: () => void;
}

function getPreviewType(filePath: string): PreviewType {
  const ext = filePath.split('.').pop()?.toLowerCase();
  if (ext === 'xlsx' || ext === 'xls') return 'xlsx';
  if (ext === 'pdf') return 'pdf';
  if (['txt', 'log', 'md', 'json', 'yaml', 'yml', 'csv', 'xml', 'html', 'css', 'js', 'ts', 'py', 'ini', 'cfg', 'conf', 'env', 'sh', 'bat', 'toml'].includes(ext || '')) return 'txt';
  return 'none';
}

export const usePreviewStore = create<PreviewState>((set) => ({
  previewType: 'none',
  previewPath: null,
  textContent: null,
  pdfBase64: null,
  loading: false,

  openFile: async (filePath: string) => {
    const type = getPreviewType(filePath);

    if (type === 'xlsx') {
      // XLSX is handled by xlsxStore — just signal the type
      set({ previewType: 'xlsx', previewPath: filePath, textContent: null, pdfBase64: null, loading: false });
      return;
    }

    set({ previewType: type, previewPath: filePath, loading: true, textContent: null, pdfBase64: null });

    try {
      if (type === 'pdf') {
        const base64 = await window.api.readBinary(filePath);
        set({ pdfBase64: base64, loading: false });
      } else if (type === 'txt') {
        const content = await window.api.readFile(filePath);
        set({ textContent: content, loading: false });
      } else {
        set({ previewType: 'none', loading: false });
      }
    } catch (err) {
      console.error('Failed to load preview:', err);
      set({ loading: false, previewType: 'none' });
    }
  },

  clear: () => set({ previewType: 'none', previewPath: null, textContent: null, pdfBase64: null, loading: false }),
}));
