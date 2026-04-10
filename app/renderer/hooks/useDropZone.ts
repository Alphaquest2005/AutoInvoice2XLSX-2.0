import { useState, useCallback } from 'react';

interface UseDropZoneOptions {
  accept?: string[];
  onDrop: (files: File[]) => void;
}

export function useDropZone({ accept, onDrop }: UseDropZoneOptions) {
  const [isDragging, setIsDragging] = useState(false);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(true);
  }, []);

  const handleDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragging(false);
  }, []);

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      e.stopPropagation();
      setIsDragging(false);

      const files = Array.from(e.dataTransfer.files);
      const filtered = accept
        ? files.filter((f) => accept.some((ext) => f.name.toLowerCase().endsWith(ext)))
        : files;

      if (filtered.length > 0) {
        onDrop(filtered);
      }
    },
    [accept, onDrop]
  );

  return {
    isDragging,
    dragProps: {
      onDragOver: handleDragOver,
      onDragLeave: handleDragLeave,
      onDrop: handleDrop,
    },
  };
}
