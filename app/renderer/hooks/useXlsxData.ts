import { useXlsxStore } from '../stores/xlsxStore';

export function useXlsxData() {
  const {
    data,
    activeSheet,
    selection,
    selectedCells,
    annotationOpen,
    errors,
    filePath,
    loadFile,
    setActiveSheet,
    setSelection,
    toggleAnnotation,
    closeAnnotation,
    clear,
  } = useXlsxStore();

  const currentSheet = data?.sheets[activeSheet] ?? null;

  return {
    data,
    currentSheet,
    activeSheet,
    selection,
    selectedCells,
    annotationOpen,
    errors,
    filePath,
    loadFile,
    setActiveSheet,
    setSelection,
    toggleAnnotation,
    closeAnnotation,
    clear,
  };
}
