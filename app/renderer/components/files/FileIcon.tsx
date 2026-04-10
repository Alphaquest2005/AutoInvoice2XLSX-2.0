import React from 'react';
import {
  FileText,
  FileSpreadsheet,
  FileJson,
  FileCode,
  File,
  Folder,
  FolderOpen,
  FileType,
  Settings,
} from 'lucide-react';

interface Props {
  extension?: string;
  isDir?: boolean;
  isOpen?: boolean;
}

const iconMap: Record<string, { icon: React.ElementType; color: string }> = {
  pdf: { icon: FileText, color: 'text-red-400' },
  xlsx: { icon: FileSpreadsheet, color: 'text-green-400' },
  xls: { icon: FileSpreadsheet, color: 'text-green-400' },
  json: { icon: FileJson, color: 'text-yellow-400' },
  yaml: { icon: Settings, color: 'text-purple-400' },
  yml: { icon: Settings, color: 'text-purple-400' },
  py: { icon: FileCode, color: 'text-blue-400' },
  ts: { icon: FileCode, color: 'text-blue-300' },
  tsx: { icon: FileCode, color: 'text-blue-300' },
  js: { icon: FileCode, color: 'text-yellow-300' },
  md: { icon: FileType, color: 'text-surface-400' },
  txt: { icon: FileText, color: 'text-surface-400' },
};

export function FileIcon({ extension, isDir, isOpen }: Props) {
  if (isDir) {
    const Icon = isOpen ? FolderOpen : Folder;
    return <Icon size={14} className="text-yellow-500/70 flex-shrink-0" />;
  }

  const ext = extension?.toLowerCase() || '';
  const mapping = iconMap[ext];

  if (mapping) {
    const Icon = mapping.icon;
    return <Icon size={14} className={`${mapping.color} flex-shrink-0`} />;
  }

  return <File size={14} className="text-surface-500 flex-shrink-0" />;
}
