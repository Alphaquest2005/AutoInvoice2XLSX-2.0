import React, { useState, useRef, useCallback, useEffect } from 'react';
import { FolderOpen, RefreshCw, Plus, FileCode, ChevronRight, Search, X } from 'lucide-react';
import { FileTree } from './FileTree';
import { ContextMenu } from './ContextMenu';
import { RecentFiles } from './RecentFiles';
import { useFileStore } from '../../stores/fileStore';
import { useXlsxStore } from '../../stores/xlsxStore';
import { usePreviewStore } from '../../stores/previewStore';
import { useChatStore } from '../../stores/chatStore';
import type { FileNode } from '../../../shared/types';

function fileName(fullPath: string): string {
  const parts = fullPath.replace(/\\/g, '/').split('/');
  return parts[parts.length - 1];
}

interface ContextMenuState {
  x: number;
  y: number;
  node: FileNode;
  clickedOnSelected: boolean;
}

/** Flatten the visible tree into an ordered list of paths (files AND expanded dirs). */
function flattenVisibleItems(node: FileNode, expandedDirs: Set<string>): string[] {
  const result: string[] = [];
  function walk(n: FileNode) {
    result.push(n.path);
    if (n.type === 'directory' && expandedDirs.has(n.path) && n.children) {
      for (const child of n.children) {
        walk(child);
      }
    }
  }
  if (node.children) {
    for (const child of node.children) {
      walk(child);
    }
  }
  return result;
}

/** Flatten visible items but only return files (for range selection). */
function flattenVisibleFiles(node: FileNode, expandedDirs: Set<string>): string[] {
  return flattenVisibleItems(node, expandedDirs).filter(p => {
    // Check if path is a file by looking for last segment with extension
    const name = p.replace(/\\/g, '/').split('/').pop() || '';
    return name.includes('.');
  });
}

/** Filter tree to only include nodes whose name matches the query (case-insensitive).
 *  Directories are kept if they contain any matching descendants. */
function filterTree(node: FileNode, query: string): FileNode | null {
  const q = query.toLowerCase();
  if (node.type === 'file') {
    const name = node.name || node.path.replace(/\\/g, '/').split('/').pop() || '';
    return name.toLowerCase().includes(q) ? node : null;
  }
  // Directory: filter children recursively
  const filteredChildren = (node.children || [])
    .map((child) => filterTree(child, query))
    .filter((c): c is FileNode => c !== null);
  if (filteredChildren.length === 0) return null;
  return { ...node, children: filteredChildren };
}

export function FileBrowser() {
  const tree = useFileStore((s) => s.tree);
  const currentPath = useFileStore((s) => s.currentPath);
  const navigateTo = useFileStore((s) => s.navigateTo);
  const refresh = useFileStore((s) => s.refresh);
  const expandedDirs = useFileStore((s) => s.expandedDirs);
  const toggleDir = useFileStore((s) => s.toggleDir);
  const selectedFiles = useFileStore((s) => s.selectedFiles);
  const lastClickedPath = useFileStore((s) => s.lastClickedPath);
  const focusedPath = useFileStore((s) => s.focusedPath);
  const selectFile = useFileStore((s) => s.selectFile);
  const toggleFileSelection = useFileStore((s) => s.toggleFileSelection);
  const selectFileRange = useFileStore((s) => s.selectFileRange);
  const clearSelection = useFileStore((s) => s.clearSelection);
  const setFocusedPath = useFileStore((s) => s.setFocusedPath);
  const clipboard = useFileStore((s) => s.clipboard);
  const setClipboard = useFileStore((s) => s.setClipboard);
  const clearClipboard = useFileStore((s) => s.clearClipboard);
  const loadXlsx = useXlsxStore((s) => s.loadFile);
  const openPreview = usePreviewStore((s) => s.openFile);
  const addSystemMessage = useChatStore((s) => s.addSystemMessage);

  const [contextMenu, setContextMenu] = useState<ContextMenuState | null>(null);
  const [renamingPath, setRenamingPath] = useState<string | null>(null);
  const [pathInput, setPathInput] = useState(currentPath);
  const [searchQuery, setSearchQuery] = useState('');
  const treeContainerRef = useRef<HTMLDivElement>(null);
  const pathInputRef = useRef<HTMLInputElement>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);

  // Sync pathInput with currentPath when it changes externally
  useEffect(() => {
    setPathInput(currentPath);
  }, [currentPath]);

  // Auto-expand all directories in filtered tree when searching
  useEffect(() => {
    if (!searchQuery || !tree) return;
    const filtered = filterTree(tree, searchQuery);
    if (!filtered) return;
    const dirs: string[] = [];
    const collectDirs = (node: FileNode) => {
      if (node.type === 'directory') {
        dirs.push(node.path);
        node.children?.forEach(collectDirs);
      }
    };
    collectDirs(filtered);
    if (dirs.length > 0) {
      const current = useFileStore.getState().expandedDirs;
      const missing = dirs.filter((d) => !current.has(d));
      if (missing.length > 0) {
        const next = new Set(current);
        missing.forEach((d) => next.add(d));
        useFileStore.setState({ expandedDirs: next });
      }
    }
  }, [searchQuery, tree]);

  // Handle path input submission
  const handlePathSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (pathInput && pathInput !== currentPath) {
      await navigateTo(pathInput);
    }
  };

  // Handle browse folder button
  const handleBrowseFolder = async () => {
    if (!window.api) return;
    const folder = await window.api.openFolderDialog?.();
    if (folder) {
      await navigateTo(folder);
    }
  };

  // Get node type from tree for a given path
  const getNodeType = useCallback((path: string): 'file' | 'directory' | null => {
    if (!tree) return null;
    const findNode = (node: FileNode): FileNode | null => {
      if (node.path === path) return node;
      if (node.children) {
        for (const child of node.children) {
          const found = findNode(child);
          if (found) return found;
        }
      }
      return null;
    };
    const node = findNode(tree);
    return node?.type || null;
  }, [tree]);

  const openFilePreview = useCallback(async (filePath: string) => {
    const ext = filePath.split('.').pop()?.toLowerCase();
    if (ext === 'xlsx' || ext === 'xls') {
      await loadXlsx(filePath);
      await openPreview(filePath);
      addSystemMessage(`Opened "${fileName(filePath)}" in spreadsheet viewer`);
    } else if (ext === 'pdf') {
      await openPreview(filePath);
      addSystemMessage(`Opened "${fileName(filePath)}" in PDF viewer`);
    } else if (['txt', 'log', 'md', 'json', 'yaml', 'yml', 'csv', 'xml', 'html', 'css', 'js', 'ts', 'py', 'ini', 'cfg', 'conf', 'env', 'sh', 'bat', 'toml'].includes(ext || '')) {
      await openPreview(filePath);
      addSystemMessage(`Opened "${fileName(filePath)}" in text viewer`);
    } else {
      addSystemMessage(`Selected file: ${fileName(filePath)}`);
    }
  }, [loadXlsx, openPreview, addSystemMessage]);

  // Keyboard navigation handler
  const handleKeyDown = useCallback((e: React.KeyboardEvent) => {
    if (!tree || renamingPath) return;

    const allItems = flattenVisibleItems(tree, expandedDirs);
    if (allItems.length === 0) return;

    const currentIdx = focusedPath ? allItems.indexOf(focusedPath) : -1;

    switch (e.key) {
      case 'ArrowDown': {
        e.preventDefault();
        const nextIdx = currentIdx < allItems.length - 1 ? currentIdx + 1 : 0;
        const nextPath = allItems[nextIdx];
        if (e.shiftKey) {
          // Shift+Down: extend selection
          const newSelection = new Set(selectedFiles);
          newSelection.add(nextPath);
          selectFileRange(Array.from(newSelection));
        } else {
          selectFile(nextPath);
        }
        setFocusedPath(nextPath);
        // Scroll focused item into view
        const el = treeContainerRef.current?.querySelector(`[data-path="${CSS.escape(nextPath)}"]`);
        el?.scrollIntoView({ block: 'nearest' });
        break;
      }
      case 'ArrowUp': {
        e.preventDefault();
        const prevIdx = currentIdx > 0 ? currentIdx - 1 : allItems.length - 1;
        const prevPath = allItems[prevIdx];
        if (e.shiftKey) {
          // Shift+Up: extend selection
          const newSelection = new Set(selectedFiles);
          newSelection.add(prevPath);
          selectFileRange(Array.from(newSelection));
        } else {
          selectFile(prevPath);
        }
        setFocusedPath(prevPath);
        const el = treeContainerRef.current?.querySelector(`[data-path="${CSS.escape(prevPath)}"]`);
        el?.scrollIntoView({ block: 'nearest' });
        break;
      }
      case 'ArrowRight': {
        e.preventDefault();
        if (focusedPath && getNodeType(focusedPath) === 'directory') {
          if (!expandedDirs.has(focusedPath)) {
            toggleDir(focusedPath);
          }
        }
        break;
      }
      case 'ArrowLeft': {
        e.preventDefault();
        if (focusedPath && getNodeType(focusedPath) === 'directory') {
          if (expandedDirs.has(focusedPath)) {
            toggleDir(focusedPath);
          }
        }
        break;
      }
      case 'Enter': {
        e.preventDefault();
        if (focusedPath) {
          const nodeType = getNodeType(focusedPath);
          if (nodeType === 'directory') {
            toggleDir(focusedPath);
          } else if (nodeType === 'file') {
            selectFile(focusedPath);
            openFilePreview(focusedPath);
          }
        }
        break;
      }
      case ' ': {
        e.preventDefault();
        if (focusedPath) {
          toggleFileSelection(focusedPath);
        }
        break;
      }
      case 'a':
      case 'A': {
        if (e.ctrlKey || e.metaKey) {
          e.preventDefault();
          // Select all visible files
          const allFiles = flattenVisibleFiles(tree, expandedDirs);
          selectFileRange(allFiles);
        }
        break;
      }
    }
  }, [tree, expandedDirs, focusedPath, selectedFiles, renamingPath, selectFile, selectFileRange,
      toggleFileSelection, setFocusedPath, toggleDir, getNodeType, openFilePreview]);

  const handleFileClick = async (filePath: string, e: React.MouseEvent) => {
    if (e.shiftKey && lastClickedPath && tree) {
      // Shift+Click: range-select
      const ordered = flattenVisibleFiles(tree, expandedDirs);
      const anchorIdx = ordered.indexOf(lastClickedPath);
      const targetIdx = ordered.indexOf(filePath);
      if (anchorIdx !== -1 && targetIdx !== -1) {
        const start = Math.min(anchorIdx, targetIdx);
        const end = Math.max(anchorIdx, targetIdx);
        selectFileRange(ordered.slice(start, end + 1));
      } else {
        selectFile(filePath);
      }
    } else if (e.ctrlKey || e.metaKey) {
      // Ctrl/Cmd+Click: toggle individual
      toggleFileSelection(filePath);
    } else {
      // Plain click: single-select + open viewer
      selectFile(filePath);
      await openFilePreview(filePath);
    }
  };

  const handleOpenFile = async () => {
    if (!window.api) return;
    const file = await window.api.openFileDialog();
    if (file) {
      selectFile(file);
      await openFilePreview(file);
    }
  };

  const handleContextMenu = (e: React.MouseEvent, node: FileNode) => {
    const clickedOnSelected = selectedFiles.has(node.path);
    if (!clickedOnSelected) {
      selectFile(node.path);
    }
    setContextMenu({ x: e.clientX, y: e.clientY, node, clickedOnSelected });
  };

  const closeContextMenu = () => setContextMenu(null);

  // ─── Context-menu action handlers ──────────────────────

  const handleOpen = () => {
    if (!contextMenu) return;
    selectFile(contextMenu.node.path);
    openFilePreview(contextMenu.node.path);
  };

  const handleRename = () => {
    if (!contextMenu) return;
    setRenamingPath(contextMenu.node.path);
  };

  const handleRenameCommit = async (oldPath: string, newName: string) => {
    if (!window.api) return;
    const lastSep = Math.max(oldPath.lastIndexOf('/'), oldPath.lastIndexOf('\\'));
    const parentDir = oldPath.substring(0, lastSep);
    const sep = oldPath.includes('\\') ? '\\' : '/';
    const newPath = parentDir + sep + newName;
    await window.api.renameFile(oldPath, newPath);
    await addSystemMessage(`Renamed "${fileName(oldPath)}" to "${newName}"`);
    setRenamingPath(null);
    await refresh();
  };

  const handleRenameCancel = () => setRenamingPath(null);

  const handleCopy = () => {
    if (!contextMenu) return;
    if (contextMenu.clickedOnSelected && selectedFiles.size > 0) {
      const paths = Array.from(selectedFiles);
      setClipboard(paths);
      addSystemMessage(`Copied ${paths.length} file(s) to clipboard: ${paths.map(fileName).join(', ')}`);
    } else {
      setClipboard([contextMenu.node.path]);
      addSystemMessage(`Copied "${fileName(contextMenu.node.path)}" to clipboard`);
    }
  };

  const handlePaste = async () => {
    if (!contextMenu || !clipboard || !window.api) return;
    const targetDir = contextMenu.node.path;
    const sep = targetDir.includes('\\') ? '\\' : '/';
    const pastedNames: string[] = [];
    for (const srcPath of clipboard.paths) {
      const srcName = srcPath.substring(
        Math.max(srcPath.lastIndexOf('/'), srcPath.lastIndexOf('\\')) + 1
      );
      const destPath = targetDir + sep + srcName;
      await window.api.copyFileTo(srcPath, destPath);
      pastedNames.push(srcName);
    }
    await addSystemMessage(`Pasted ${pastedNames.length} file(s) into ${fileName(targetDir)}: ${pastedNames.join(', ')}`);
    clearClipboard();
    await refresh();
  };

  const handleDelete = async () => {
    if (!contextMenu || !window.api) return;

    let pathsToDelete: string[];
    if (contextMenu.clickedOnSelected && selectedFiles.size > 1) {
      pathsToDelete = Array.from(selectedFiles);
      const ok = window.confirm(`Delete ${pathsToDelete.length} items? This cannot be undone.`);
      if (!ok) return;
    } else {
      const ok = window.confirm(`Delete "${contextMenu.node.name}"? This cannot be undone.`);
      if (!ok) return;
      pathsToDelete = [contextMenu.node.path];
    }

    for (const p of pathsToDelete) {
      await window.api.deleteFile(p);
    }

    // Clear XLSX viewer if the loaded file (or its parent directory) was deleted
    const loadedXlsx = useXlsxStore.getState().filePath;
    if (loadedXlsx) {
      const norm = (s: string) => s.replace(/\\/g, '/');
      const loadedNorm = norm(loadedXlsx);
      const wasDeleted = pathsToDelete.some((p) => {
        const pNorm = norm(p);
        return loadedNorm === pNorm || loadedNorm.startsWith(pNorm + '/');
      });
      if (wasDeleted) {
        useXlsxStore.getState().clear();
      }
    }

    const names = pathsToDelete.map(fileName);
    await addSystemMessage(`Deleted ${names.length} item(s): ${names.join(', ')}`);
    clearSelection();
    await refresh();
  };

  const handleNewFile = async () => {
    if (!contextMenu || !window.api) return;
    const name = window.prompt('New file name:', 'untitled.txt');
    if (!name) return;
    const sep = contextMenu.node.path.includes('\\') ? '\\' : '/';
    await window.api.createFile(contextMenu.node.path + sep + name);
    await addSystemMessage(`Created new file "${name}" in ${fileName(contextMenu.node.path)}`);
    await refresh();
  };

  const handleNewFolder = async () => {
    if (!contextMenu || !window.api) return;
    const name = window.prompt('New folder name:', 'New Folder');
    if (!name) return;
    const sep = contextMenu.node.path.includes('\\') ? '\\' : '/';
    await window.api.createFolder(contextMenu.node.path + sep + name);
    await addSystemMessage(`Created new folder "${name}" in ${fileName(contextMenu.node.path)}`);
    await refresh();
  };

  const handleShowInExplorer = () => {
    if (!contextMenu || !window.api) return;
    window.api.showInExplorer(contextMenu.node.path);
  };

  const handleRunPipeline = () => {
    if (!contextMenu || !window.api) return;
    addSystemMessage(`Running pipeline on "${fileName(contextMenu.node.path)}"...`);
    window.api.runPipeline(contextMenu.node.path);
  };

  const handleRunFolderPipeline = () => {
    if (!contextMenu || !window.api) return;
    addSystemMessage(`Running pipeline on all files in "${fileName(contextMenu.node.path)}"...`);
    window.api.runFolderPipeline(contextMenu.node.path);
  };

  const handleRunFolderBatchPipeline = () => {
    if (!contextMenu || !window.api) return;
    addSystemMessage(`Processing each PDF in "${fileName(contextMenu.node.path)}" as individual shipment...`);
    window.api.runFolderBatchPipeline(contextMenu.node.path);
  };

  const handleOpenInViewer = () => {
    if (!contextMenu) return;
    selectFile(contextMenu.node.path);
    openFilePreview(contextMenu.node.path);
  };

  const handleValidate = () => {
    if (!contextMenu || !window.api) return;
    addSystemMessage(`Validating "${fileName(contextMenu.node.path)}"...`);
    window.api.runPipeline(contextMenu.node.path, undefined, 'validate');
  };

  const handleCombine = async () => {
    if (!contextMenu || !window.api) return;
    const xlsxFiles = Array.from(selectedFiles).filter(
      (p) => p.toLowerCase().endsWith('.xlsx') || p.toLowerCase().endsWith('.xls')
    );
    if (xlsxFiles.length < 2) {
      window.alert('Please select at least 2 XLSX files to combine.');
      return;
    }
    addSystemMessage(`Combining ${xlsxFiles.length} XLSX files...`);
    try {
      const result = await window.api.combineXlsx(xlsxFiles);
      if (result.success && result.outputPath) {
        addSystemMessage(`Combined files saved to: ${fileName(result.outputPath)}`);
        await refresh();
        await loadXlsx(result.outputPath);
      } else {
        addSystemMessage(`Failed to combine files: ${result.error || 'Unknown error'}`);
      }
    } catch (err) {
      addSystemMessage(`Error combining files: ${err}`);
    }
  };

  // Import ASYCUDA XML files from context menu (selected files)
  const handleImportAsycuda = async () => {
    if (!contextMenu || !window.api) return;

    const xmlFiles = contextMenu.clickedOnSelected && selectedFiles.size > 0
      ? Array.from(selectedFiles).filter((p) => p.toLowerCase().endsWith('.xml'))
      : [contextMenu.node.path].filter((p) => p.toLowerCase().endsWith('.xml'));

    if (xmlFiles.length === 0) {
      window.alert('Please select XML files to import.');
      return;
    }

    addSystemMessage(`Importing ${xmlFiles.length} ASYCUDA XML file(s)...`);
    try {
      if (xmlFiles.length === 1) {
        const result = await window.api.importAsycudaXml(xmlFiles[0]);
        if (result.success) {
          let msg = `Imported ${result.imported || 0} classifications from ${fileName(xmlFiles[0])}`;
          if (result.corrected && result.corrected > 0) {
            msg += `. ${result.corrected} corrections made.`;
          }
          addSystemMessage(msg);
        } else {
          addSystemMessage(`Import failed: ${result.error || 'Unknown error'}`);
        }
      } else {
        const result = await window.api.importAsycudaMultiple(xmlFiles);
        let msg = `Imported ${result.total_imported} classifications from ${result.successful}/${result.total_files} files`;
        if (result.total_corrected > 0) {
          msg += `. ${result.total_corrected} corrections made.`;
        }
        addSystemMessage(msg);
      }
    } catch (err) {
      addSystemMessage(`Error importing ASYCUDA XML: ${err}`);
    }
  };

  // Import ASYCUDA XML files via file dialog
  const handleImportAsycudaDialog = async () => {
    if (!window.api) return;
    const files = await window.api.browseAsycudaXml();
    if (!files || files.length === 0) return;

    addSystemMessage(`Importing ${files.length} ASYCUDA XML file(s)...`);
    try {
      if (files.length === 1) {
        const result = await window.api.importAsycudaXml(files[0]);
        if (result.success) {
          let msg = `Imported ${result.imported || 0} classifications from ${fileName(files[0])}`;
          if (result.corrected && result.corrected > 0) {
            msg += `. ${result.corrected} corrections made.`;
          }
          addSystemMessage(msg);
        } else {
          addSystemMessage(`Import failed: ${result.error || 'Unknown error'}`);
        }
      } else {
        const result = await window.api.importAsycudaMultiple(files);
        let msg = `Imported ${result.total_imported} classifications from ${result.successful}/${result.total_files} files`;
        if (result.total_corrected > 0) {
          msg += `. ${result.total_corrected} corrections made.`;
        }
        addSystemMessage(msg);
      }
    } catch (err) {
      addSystemMessage(`Error importing ASYCUDA XML: ${err}`);
    }
  };

  // Generate costing sheet from ASYCUDA XML
  const handleGenerateCostingSheet = async () => {
    if (!contextMenu || !window.api) return;

    const xmlPath = contextMenu.node.path;
    if (!xmlPath.toLowerCase().endsWith('.xml')) {
      window.alert('Please select an XML file.');
      return;
    }

    addSystemMessage(`Generating costing sheet from ${fileName(xmlPath)}...`);
    try {
      const result = await window.api.generateCostingSheet(xmlPath);
      if (result.success) {
        addSystemMessage(`Costing sheet generated: ${fileName(result.output_path || '')}`);
      } else {
        addSystemMessage(`Error: ${result.error || 'Unknown error'}`);
      }
    } catch (err) {
      addSystemMessage(`Error generating costing sheet: ${err}`);
    }
  };

  const selectedCount = contextMenu
    ? (contextMenu.clickedOnSelected ? selectedFiles.size : 1)
    : 1;

  // Check if all selected files are XLSX
  const allSelectedAreXlsx = selectedFiles.size > 1 && Array.from(selectedFiles).every(
    (p) => p.toLowerCase().endsWith('.xlsx') || p.toLowerCase().endsWith('.xls')
  );

  // Check if all selected files are XML
  const allSelectedAreXml = selectedFiles.size > 0 && Array.from(selectedFiles).every(
    (p) => p.toLowerCase().endsWith('.xml')
  );

  return (
    <div className="h-full flex flex-col bg-surface-900">
      {/* Header */}
      <div className="h-9 px-3 flex items-center justify-between border-b border-surface-700 bg-surface-800/50">
        <span className="text-xs font-medium text-surface-300">Files</span>
        <div className="flex items-center gap-1">
          <button
            onClick={handleImportAsycudaDialog}
            className="p-1 text-surface-400 hover:text-surface-100 transition-colors"
            title="Import ASYCUDA XML"
          >
            <FileCode size={13} />
          </button>
          <button
            onClick={handleOpenFile}
            className="p-1 text-surface-400 hover:text-surface-100 transition-colors"
            title="Open File (Ctrl+O)"
          >
            <Plus size={13} />
          </button>
          <button
            onClick={refresh}
            className="p-1 text-surface-400 hover:text-surface-100 transition-colors"
            title="Refresh"
          >
            <RefreshCw size={13} />
          </button>
        </div>
      </div>

      {/* Path Bar */}
      <form onSubmit={handlePathSubmit} className="px-2 py-1.5 border-b border-surface-700 bg-surface-850">
        <div className="flex items-center gap-1">
          <button
            type="button"
            onClick={handleBrowseFolder}
            className="p-1 text-surface-400 hover:text-surface-100 transition-colors flex-shrink-0"
            title="Browse folder"
          >
            <FolderOpen size={14} />
          </button>
          <input
            ref={pathInputRef}
            type="text"
            value={pathInput}
            onChange={(e) => setPathInput(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Escape') {
                setPathInput(currentPath);
                pathInputRef.current?.blur();
              }
            }}
            placeholder="Enter path..."
            className="flex-1 bg-surface-800 text-surface-200 text-xs px-2 py-1 rounded border border-surface-600 focus:border-blue-500 focus:outline-none"
          />
          <button
            type="submit"
            className="p-1 text-surface-400 hover:text-surface-100 transition-colors flex-shrink-0"
            title="Go to path"
          >
            <ChevronRight size={14} />
          </button>
        </div>
      </form>

      {/* Search Bar */}
      <div className="px-2 py-1 border-b border-surface-700 bg-surface-850">
        <div className="flex items-center gap-1">
          <Search size={13} className="text-surface-400 flex-shrink-0" />
          <input
            ref={searchInputRef}
            type="text"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Escape') {
                setSearchQuery('');
                searchInputRef.current?.blur();
              }
            }}
            placeholder="Search files..."
            className="flex-1 bg-surface-800 text-surface-200 text-xs px-2 py-1 rounded border border-surface-600 focus:border-blue-500 focus:outline-none"
          />
          {searchQuery && (
            <button
              onClick={() => setSearchQuery('')}
              className="p-0.5 text-surface-400 hover:text-surface-100 transition-colors flex-shrink-0"
              title="Clear search"
            >
              <X size={13} />
            </button>
          )}
        </div>
      </div>

      {/* Tree */}
      <div
        ref={treeContainerRef}
        tabIndex={0}
        onKeyDown={handleKeyDown}
        className="flex-1 overflow-y-auto py-1 outline-none focus:ring-1 focus:ring-blue-500/30 focus:ring-inset"
      >
        {tree ? (
          <FileTree
            node={searchQuery ? (filterTree(tree, searchQuery) || { ...tree, children: [] }) : tree}
            depth={0}
            onFileClick={handleFileClick}
            onContextMenu={handleContextMenu}
            renamingPath={renamingPath}
            onRenameCommit={handleRenameCommit}
            onRenameCancel={handleRenameCancel}
          />
        ) : (
          <div className="flex flex-col items-center justify-center h-full text-surface-500 p-4">
            <FolderOpen size={32} className="mb-2 opacity-30" />
            <p className="text-xs text-center">No workspace loaded</p>
          </div>
        )}
      </div>

      {/* Recent Files */}
      <RecentFiles onFileClick={(path) => { selectFile(path); }} />

      {/* Context Menu */}
      {contextMenu && (
        <ContextMenu
          x={contextMenu.x}
          y={contextMenu.y}
          node={contextMenu.node}
          hasClipboard={!!clipboard}
          selectedCount={selectedCount}
          onClose={closeContextMenu}
          onOpen={handleOpen}
          onRename={handleRename}
          onCopy={handleCopy}
          onPaste={handlePaste}
          onDelete={handleDelete}
          onNewFile={handleNewFile}
          onNewFolder={handleNewFolder}
          onShowInExplorer={handleShowInExplorer}
          onRunPipeline={handleRunPipeline}
          onRunFolderPipeline={handleRunFolderPipeline}
          onRunFolderBatchPipeline={handleRunFolderBatchPipeline}
          onOpenInViewer={handleOpenInViewer}
          onValidate={handleValidate}
          onCombine={handleCombine}
          onImportAsycuda={handleImportAsycuda}
          onGenerateCostingSheet={handleGenerateCostingSheet}
          allSelectedAreXlsx={allSelectedAreXlsx}
          allSelectedAreXml={allSelectedAreXml}
        />
      )}
    </div>
  );
}
