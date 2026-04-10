import React, { useState } from 'react';
import { Clock, ChevronUp, ChevronDown } from 'lucide-react';
import { FileIcon } from './FileIcon';

interface Props {
  onFileClick: (path: string) => void;
}

// Placeholder - in production, would load from SQLite via IPC
const recentFiles: { name: string; path: string; type: string; openedAt: string }[] = [];

export function RecentFiles({ onFileClick }: Props) {
  const [expanded, setExpanded] = useState(true);

  if (recentFiles.length === 0 && !expanded) return null;

  return (
    <div className="border-t border-surface-700">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full h-7 px-3 flex items-center justify-between text-xs text-surface-400 hover:text-surface-200 hover:bg-surface-800/50"
      >
        <span className="flex items-center gap-1.5">
          <Clock size={11} />
          Recent Files
        </span>
        {expanded ? <ChevronDown size={11} /> : <ChevronUp size={11} />}
      </button>

      {expanded && (
        <div className="max-h-36 overflow-y-auto">
          {recentFiles.length === 0 ? (
            <p className="text-[10px] text-surface-500 px-3 py-2">No recent files</p>
          ) : (
            recentFiles.map((file, i) => (
              <div
                key={i}
                onClick={() => onFileClick(file.path)}
                className="flex items-center gap-2 px-3 py-1 cursor-pointer text-xs text-surface-300 hover:bg-surface-700/50"
              >
                <FileIcon extension={file.type} />
                <span className="truncate flex-1">{file.name}</span>
                <span className="text-[10px] text-surface-500 flex-shrink-0">
                  {new Date(file.openedAt).toLocaleDateString()}
                </span>
              </div>
            ))
          )}
        </div>
      )}
    </div>
  );
}
