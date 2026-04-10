import { useFileStore } from '../stores/fileStore';
import { useXlsxStore } from '../stores/xlsxStore';

export function useFileSystem() {
  const {
    tree,
    selectedFiles,
    lastClickedPath,
    loadTree,
    toggleDir,
    selectFile,
    toggleFileSelection,
    selectFileRange,
    clearSelection,
    refresh,
  } = useFileStore();

  const loadXlsx = useXlsxStore((s) => s.loadFile);

  const openFile = async (filePath: string) => {
    selectFile(filePath);
    const ext = filePath.split('.').pop()?.toLowerCase();
    if (ext === 'xlsx' || ext === 'xls') {
      await loadXlsx(filePath);
    }
  };

  return {
    tree,
    selectedFiles,
    lastClickedPath,
    loadTree,
    toggleDir,
    openFile,
    selectFile,
    toggleFileSelection,
    selectFileRange,
    clearSelection,
    refresh,
  };
}
