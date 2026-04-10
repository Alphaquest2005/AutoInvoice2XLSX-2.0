import React, { useState, useCallback } from 'react';
import { Upload } from 'lucide-react';

interface Props {
  onFileDrop: (files: File[]) => void;
  children: React.ReactNode;
  accept?: string[];
  className?: string;
}

export function DropZone({ onFileDrop, children, accept, className = '' }: Props) {
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
        onFileDrop(filtered);
      }
    },
    [accept, onFileDrop]
  );

  return (
    <div
      onDragOver={handleDragOver}
      onDragLeave={handleDragLeave}
      onDrop={handleDrop}
      className={`relative ${className}`}
    >
      {children}
      {isDragging && (
        <div className="absolute inset-0 z-40 bg-accent/10 border-2 border-dashed border-accent rounded-lg flex items-center justify-center">
          <div className="text-center text-accent">
            <Upload size={24} className="mx-auto mb-2" />
            <p className="text-sm font-medium">Drop files here</p>
          </div>
        </div>
      )}
    </div>
  );
}
