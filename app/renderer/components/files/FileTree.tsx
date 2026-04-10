import React from 'react';
import { FileTreeNode } from './FileTreeNode';
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

export function FileTree({
  node,
  depth,
  onFileClick,
  onContextMenu,
  renamingPath,
  onRenameCommit,
  onRenameCancel,
}: Props) {
  // Root level (depth 0): skip the root directory node, render its children directly
  if (depth === 0 && node.type === 'directory') {
    return (
      <div>
        {node.children?.map((child) => (
          <FileTreeNode
            key={child.path}
            node={child}
            depth={1}
            onFileClick={onFileClick}
            onContextMenu={onContextMenu}
            renamingPath={renamingPath}
            onRenameCommit={onRenameCommit}
            onRenameCancel={onRenameCancel}
          />
        ))}
      </div>
    );
  }

  // Non-root: render a single FileTreeNode (which handles its own child recursion)
  return (
    <FileTreeNode
      node={node}
      depth={depth}
      onFileClick={onFileClick}
      onContextMenu={onContextMenu}
      renamingPath={renamingPath}
      onRenameCommit={onRenameCommit}
      onRenameCancel={onRenameCancel}
    />
  );
}
