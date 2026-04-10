import React, { useRef, useEffect, useState } from 'react';
import { ChevronRight, ChevronDown, Lock, ExternalLink } from 'lucide-react';
import { FileIcon } from './FileIcon';
import { useFileStore } from '../../stores/fileStore';
import type { FileNode } from '../../../shared/types';

interface Props {
  node: FileNode;
  depth: number;
  onFileClick: (path: string, e: React.MouseEvent) => void;
  onContextMenu?: (e: React.MouseEvent, node: FileNode) => void;
  renamingPath?: string | null;
  onRenameCommit?: (oldPath: string, newName: string) => void;
  onRenameCancel?: () => void;
}

export function FileTreeNode({
  node,
  depth,
  onFileClick,
  onContextMenu,
  renamingPath,
  onRenameCommit,
  onRenameCancel,
}: Props) {
  const expandedDirs = useFileStore((s) => s.expandedDirs);
  const toggleDir = useFileStore((s) => s.toggleDir);
  const selectedFiles = useFileStore((s) => s.selectedFiles);
  const focusedPath = useFileStore((s) => s.focusedPath);

  const isDir = node.type === 'directory';
  const isExpanded = expandedDirs.has(node.path);
  const isSelected = selectedFiles.has(node.path);
  const isFocused = focusedPath === node.path;
  const isRenaming = renamingPath === node.path;

  const [renameValue, setRenameValue] = useState(node.name);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (isRenaming && inputRef.current) {
      setRenameValue(node.name);
      inputRef.current.focus();
      // Select filename without extension
      const dotIdx = node.name.lastIndexOf('.');
      inputRef.current.setSelectionRange(0, dotIdx > 0 ? dotIdx : node.name.length);
    }
  }, [isRenaming, node.name]);

  const handleClick = (e: React.MouseEvent) => {
    if (isRenaming) return;
    if (isDir) {
      toggleDir(node.path);
    } else {
      onFileClick(node.path, e);
    }
  };

  const handleDoubleClick = (e: React.MouseEvent) => {
    if (isRenaming || isDir) return;
    e.preventDefault();
    e.stopPropagation();
    // Open PDF files (and other externally-handled types) with default system app
    const ext = node.extension?.toLowerCase();
    if (ext === 'pdf' || ext === 'doc' || ext === 'docx') {
      window.api?.openExternal(node.path);
    }
  };

  const handleRightClick = (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    onContextMenu?.(e, node);
  };

  const commitRename = () => {
    const trimmed = renameValue.trim();
    if (trimmed && trimmed !== node.name) {
      onRenameCommit?.(node.path, trimmed);
    } else {
      onRenameCancel?.();
    }
  };

  const handleRenameKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      commitRename();
    } else if (e.key === 'Escape') {
      e.preventDefault();
      onRenameCancel?.();
    }
  };

  return (
    <>
      <div
        onClick={handleClick}
        onDoubleClick={handleDoubleClick}
        onContextMenu={handleRightClick}
        data-path={node.path}
        className={`flex items-center gap-1 py-1 px-2 cursor-pointer text-xs hover:bg-surface-700/50 transition-colors ${
          isSelected ? 'bg-surface-700/70 text-surface-100' : 'text-surface-300'
        } ${isFocused ? 'ring-1 ring-blue-500 ring-inset' : ''}`}
        style={{ paddingLeft: `${depth * 14 + 4}px` }}
      >
        {/* Expand/collapse icon */}
        <span className="w-4 h-4 flex items-center justify-center flex-shrink-0">
          {isDir ? (
            isExpanded ? (
              <ChevronDown size={12} />
            ) : (
              <ChevronRight size={12} />
            )
          ) : null}
        </span>

        {/* File icon */}
        <FileIcon extension={node.extension} isDir={isDir} isOpen={isExpanded} />

        {/* Name or rename input */}
        {isRenaming ? (
          <input
            ref={inputRef}
            value={renameValue}
            onChange={(e) => setRenameValue(e.target.value)}
            onBlur={commitRename}
            onKeyDown={handleRenameKeyDown}
            className="flex-1 bg-surface-700 text-surface-100 px-1 py-0 rounded text-xs outline-none border border-blue-500 min-w-0"
            onClick={(e) => e.stopPropagation()}
          />
        ) : (
          <span className={`truncate flex-1 flex items-center gap-1 ${node.isNew ? 'font-bold text-surface-100' : ''}`}>
            {node.name}
            {node.isNew && <span className="w-1.5 h-1.5 rounded-full bg-blue-400 flex-shrink-0" />}
            {node.isLink && <ExternalLink size={10} className="text-blue-400 flex-shrink-0" />}
            {node.locked && <Lock size={10} className="text-surface-500 flex-shrink-0" />}
          </span>
        )}

        {/* Size badge */}
        {!isDir && !isRenaming && node.size !== undefined && (
          <span className="text-[10px] text-surface-500 flex-shrink-0">
            {formatSize(node.size)}
          </span>
        )}
      </div>

      {/* Children */}
      {isDir && isExpanded && node.children?.map((child) => (
        <FileTreeNode
          key={child.path}
          node={child}
          depth={depth + 1}
          onFileClick={onFileClick}
          onContextMenu={onContextMenu}
          renamingPath={renamingPath}
          onRenameCommit={onRenameCommit}
          onRenameCancel={onRenameCancel}
        />
      ))}
    </>
  );
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes}B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)}K`;
  return `${(bytes / (1024 * 1024)).toFixed(1)}M`;
}
