import React, { useEffect, useRef } from 'react';
import {
  Trash2,
  Pencil,
  Copy,
  ClipboardPaste,
  FolderPlus,
  FilePlus,
  ExternalLink,
  FileSpreadsheet,
  Play,
  CheckCircle,
  Layers,
  Combine,
  FileCode,
  Calculator,
} from 'lucide-react';
import type { FileNode } from '../../../shared/types';

export interface ContextMenuAction {
  label: string;
  icon: React.ReactNode;
  onClick: () => void;
  separator?: false;
  disabled?: boolean;
}

export interface ContextMenuSeparator {
  separator: true;
}

export type ContextMenuItem = ContextMenuAction | ContextMenuSeparator;

interface Props {
  x: number;
  y: number;
  node: FileNode;
  hasClipboard: boolean;
  selectedCount: number;
  onClose: () => void;
  onOpen: () => void;
  onRename: () => void;
  onCopy: () => void;
  onPaste: () => void;
  onDelete: () => void;
  onNewFile: () => void;
  onNewFolder: () => void;
  onShowInExplorer: () => void;
  onRunPipeline?: () => void;
  onRunFolderPipeline?: () => void;
  onRunFolderBatchPipeline?: () => void;
  onOpenInViewer?: () => void;
  onValidate?: () => void;
  onCombine?: () => void;
  onImportAsycuda?: () => void;
  onGenerateCostingSheet?: () => void;
  allSelectedAreXlsx?: boolean;
  allSelectedAreXml?: boolean;
}

export function ContextMenu({
  x,
  y,
  node,
  hasClipboard,
  selectedCount,
  onClose,
  onOpen,
  onRename,
  onCopy,
  onPaste,
  onDelete,
  onNewFile,
  onNewFolder,
  onShowInExplorer,
  onRunPipeline,
  onRunFolderPipeline,
  onRunFolderBatchPipeline,
  onOpenInViewer,
  onValidate,
  onCombine,
  onImportAsycuda,
  onGenerateCostingSheet,
  allSelectedAreXlsx,
  allSelectedAreXml,
}: Props) {
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        onClose();
      }
    };
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('mousedown', handleClickOutside);
    document.addEventListener('keydown', handleEscape);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
      document.removeEventListener('keydown', handleEscape);
    };
  }, [onClose]);

  // Clamp position so menu stays within viewport
  useEffect(() => {
    if (menuRef.current) {
      const rect = menuRef.current.getBoundingClientRect();
      const vw = window.innerWidth;
      const vh = window.innerHeight;
      if (rect.right > vw) {
        menuRef.current.style.left = `${vw - rect.width - 4}px`;
      }
      if (rect.bottom > vh) {
        menuRef.current.style.top = `${vh - rect.height - 4}px`;
      }
    }
  }, [x, y]);

  const isDir = node.type === 'directory';
  const ext = node.extension?.toLowerCase();
  const isTxt = ext === 'txt';
  const isPdf = ext === 'pdf';
  const isXlsx = ext === 'xlsx' || ext === 'xls';
  const isXml = ext === 'xml';
  const multi = selectedCount > 1;

  const items: ContextMenuItem[] = [];

  if (isDir) {
    items.push(
      { label: 'New File', icon: <FilePlus size={13} />, onClick: () => { onNewFile(); onClose(); } },
      { label: 'New Folder', icon: <FolderPlus size={13} />, onClick: () => { onNewFolder(); onClose(); } },
    );
    if (hasClipboard) {
      items.push({ label: 'Paste', icon: <ClipboardPaste size={13} />, onClick: () => { onPaste(); onClose(); } });
    }
    // Run Pipeline on Folder
    if (onRunFolderPipeline) {
      items.push(
        { separator: true },
        { label: 'Run Pipeline on Folder', icon: <Play size={13} />, onClick: () => { onRunFolderPipeline(); onClose(); } },
      );
    }
    if (onRunFolderBatchPipeline) {
      items.push(
        { label: 'Process Each PDF as Shipment', icon: <Play size={13} />, onClick: () => { onRunFolderBatchPipeline(); onClose(); } },
      );
    }
    if (!node.locked) {
      items.push(
        { separator: true },
        { label: 'Rename', icon: <Pencil size={13} />, onClick: () => { onRename(); onClose(); } },
        { label: 'Delete', icon: <Trash2 size={13} />, onClick: () => { onDelete(); onClose(); } },
      );
    }
    items.push(
      { separator: true },
      { label: 'Show in Explorer', icon: <ExternalLink size={13} />, onClick: () => { onShowInExplorer(); onClose(); } },
    );
  } else {
    // Single-target actions: only show when one file selected
    if (!multi) {
      items.push(
        { label: 'Open', icon: <FileSpreadsheet size={13} />, onClick: () => { onOpen(); onClose(); } },
        { label: 'Rename', icon: <Pencil size={13} />, onClick: () => { onRename(); onClose(); } },
      );
    }

    items.push(
      { label: multi ? `Copy (${selectedCount})` : 'Copy', icon: <Copy size={13} />, onClick: () => { onCopy(); onClose(); } },
      { label: multi ? `Delete (${selectedCount})` : 'Delete', icon: <Trash2 size={13} />, onClick: () => { onDelete(); onClose(); } },
      { separator: true },
      { label: 'Show in Explorer', icon: <ExternalLink size={13} />, onClick: () => { onShowInExplorer(); onClose(); } },
    );

    // Pipeline/XLSX actions: single selection only
    if (!multi) {
      if ((isTxt || isPdf) && onRunPipeline) {
        items.push(
          { separator: true },
          { label: 'Run Pipeline', icon: <Play size={13} />, onClick: () => { onRunPipeline(); onClose(); } },
        );
      }

      if (isXlsx) {
        items.push({ separator: true });
        if (onOpenInViewer) {
          items.push({ label: 'Open in Viewer', icon: <Layers size={13} />, onClick: () => { onOpenInViewer(); onClose(); } });
        }
        if (onRunPipeline) {
          items.push({ label: 'Re-run Pipeline', icon: <Play size={13} />, onClick: () => { onRunPipeline(); onClose(); } });
        }
        if (onValidate) {
          items.push({ label: 'Validate', icon: <CheckCircle size={13} />, onClick: () => { onValidate(); onClose(); } });
        }
      }
    }

    // Combine option: show when multiple XLSX files are selected
    if (multi && allSelectedAreXlsx && onCombine) {
      items.push(
        { separator: true },
        { label: `Combine (${selectedCount})`, icon: <Combine size={13} />, onClick: () => { onCombine(); onClose(); } },
      );
    }

    // Import ASYCUDA option: show for XML files (single or multiple)
    if (isXml || (multi && allSelectedAreXml)) {
      if (onImportAsycuda) {
        items.push(
          { separator: true },
          {
            label: multi ? `Import ASYCUDA (${selectedCount})` : 'Import ASYCUDA',
            icon: <FileCode size={13} />,
            onClick: () => { onImportAsycuda(); onClose(); }
          },
        );
      }
    }

    // Generate Costing Sheet option: show for single XML file only
    if (!multi && isXml && onGenerateCostingSheet) {
      items.push(
        {
          label: 'Generate Costing Sheet',
          icon: <Calculator size={13} />,
          onClick: () => { onGenerateCostingSheet(); onClose(); }
        },
      );
    }
  }

  return (
    <div
      ref={menuRef}
      className="fixed z-50 min-w-[180px] bg-surface-800 border border-surface-600 rounded-md shadow-xl py-1 text-xs"
      style={{ left: x, top: y }}
    >
      {items.map((item, i) => {
        if (item.separator) {
          return <div key={`sep-${i}`} className="h-px bg-surface-600 my-1" />;
        }
        return (
          <button
            key={item.label}
            onClick={item.onClick}
            disabled={item.disabled}
            className="w-full flex items-center gap-2 px-3 py-1.5 text-left text-surface-200 hover:bg-surface-700 hover:text-surface-50 disabled:opacity-40 disabled:pointer-events-none transition-colors"
          >
            <span className="w-4 h-4 flex items-center justify-center text-surface-400">{item.icon}</span>
            {item.label}
          </button>
        );
      })}
    </div>
  );
}
