import React from 'react';
import { User, Bot, Info, Wrench, CheckCircle, XCircle } from 'lucide-react';
import { MarkdownRenderer, FILE_PATH_RE, linkifyFilePaths } from '../common/MarkdownRenderer';
import { useFileStore } from '../../stores/fileStore';
import { usePreviewStore } from '../../stores/previewStore';
import { useXlsxStore } from '../../stores/xlsxStore';
import type { Message } from '../../../shared/types';

interface Props {
  message: Message;
  isStreaming?: boolean;
}

/** Detect quoted filenames like "file.pdf" or "file.xlsx" in system messages. */
const QUOTED_FILE_RE = /\u201c([^"\u201d]+\.\w{1,5})\u201d|"([^"]+\.\w{1,5})"/g;

/** Linkify both full file paths and quoted filenames in system message text. */
function linkifySystemMessage(text: string): React.ReactNode[] {
  // First try full file paths
  FILE_PATH_RE.lastIndex = 0;
  if (FILE_PATH_RE.test(text)) {
    FILE_PATH_RE.lastIndex = 0;
    return linkifyFilePaths(text);
  }

  // Then try quoted filenames — render them as clickable highlights
  const parts: React.ReactNode[] = [];
  let lastIndex = 0;
  QUOTED_FILE_RE.lastIndex = 0;

  for (const match of text.matchAll(QUOTED_FILE_RE)) {
    const matchStart = match.index!;
    const fileName = match[1] || match[2];
    if (matchStart > lastIndex) {
      parts.push(text.slice(lastIndex, matchStart));
    }
    parts.push(
      <QuotedFileLink key={matchStart} fileName={fileName} />
    );
    lastIndex = matchStart + match[0].length;
  }

  if (lastIndex < text.length) {
    parts.push(text.slice(lastIndex));
  }

  return parts.length > 0 ? parts : [text];
}

/** A clickable quoted filename — finds the file in the tree and opens preview. */
function QuotedFileLink({ fileName }: { fileName: string }) {
  const selectFile = useFileStore((s) => s.selectFile);
  const openPreview = usePreviewStore((s) => s.openFile);
  const loadXlsx = useXlsxStore((s) => s.loadFile);
  const tree = useFileStore((s) => s.tree);

  const handleClick = async (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();

    // Try to find the full path in the file tree
    const fullPath = tree ? findFileInNode(tree, fileName) : null;
    if (fullPath) {
      selectFile(fullPath);
      const ext = fullPath.split('.').pop()?.toLowerCase();
      if (ext === 'xlsx' || ext === 'xls') {
        await loadXlsx(fullPath);
      }
      await openPreview(fullPath);
    }
  };

  return (
    <a
      href="#"
      onClick={handleClick}
      className="text-accent hover:underline cursor-pointer"
      title={`Open ${fileName}`}
    >
      &quot;{fileName}&quot;
    </a>
  );
}

/** Recursively search a FileNode tree for a node matching the given filename. */
function findFileInNode(node: { path: string; type: string; children?: any[] }, fileName: string): string | null {
  if (node.type === 'file') {
    const nodeName = node.path.replace(/\\/g, '/').split('/').pop();
    if (nodeName === fileName) return node.path;
  }
  if (node.children) {
    for (const child of node.children) {
      const found = findFileInNode(child, fileName);
      if (found) return found;
    }
  }
  return null;
}

export function MessageBubble({ message, isStreaming }: Props) {
  const isUser = message.role === 'user';
  const isSystem = message.role === 'system';

  if (isSystem) {
    const isToolCall = message.content.startsWith('[Tool Call]');
    const isToolResult = message.content.startsWith('[Tool Result]');
    const isToolError = isToolResult && message.content.includes('ERROR:');

    if (isToolCall) {
      // Parse tool name and params
      const match = message.content.match(/\[Tool Call\]\s*(\w+)\((.*)?\)/s);
      const toolName = match?.[1] || 'tool';
      const params = match?.[2] || '';
      return (
        <div className="flex justify-center message-appear">
          <div className="flex items-start gap-2 max-w-[90%] rounded px-3 py-1.5 text-xs bg-blue-950/40 border border-blue-800/40 text-blue-300 font-mono">
            <Wrench size={12} className="flex-shrink-0 mt-0.5 text-blue-400" />
            <div className="min-w-0">
              <span className="font-semibold text-blue-200">{toolName}</span>
              {params && (
                <span className="text-blue-400/80 break-all">({params})</span>
              )}
            </div>
          </div>
        </div>
      );
    }

    if (isToolResult) {
      const resultContent = message.content.replace(/^\[Tool Result\]\s*/, '');
      const Icon = isToolError ? XCircle : CheckCircle;
      const colors = isToolError
        ? 'bg-red-950/40 border-red-800/40 text-red-300'
        : 'bg-emerald-950/40 border-emerald-800/40 text-emerald-300';
      const iconColor = isToolError ? 'text-red-400' : 'text-emerald-400';
      return (
        <div className="flex justify-center message-appear">
          <div className={`flex items-start gap-2 max-w-[90%] rounded px-3 py-1.5 text-xs font-mono ${colors} border`}>
            <Icon size={12} className={`flex-shrink-0 mt-0.5 ${iconColor}`} />
            <span className="break-all whitespace-pre-wrap">{resultContent}</span>
          </div>
        </div>
      );
    }

    // Regular system message with clickable file paths and quoted filenames
    return (
      <div className="flex justify-center message-appear">
        <div className="flex items-start gap-2 max-w-[90%] rounded px-3 py-1.5 text-xs bg-surface-800/60 border border-surface-700/50 text-surface-400">
          <Info size={12} className="flex-shrink-0 mt-0.5 text-surface-500" />
          <span>{linkifySystemMessage(message.content)}</span>
        </div>
      </div>
    );
  }

  return (
    <div className={`flex gap-2.5 message-appear ${isUser ? 'flex-row-reverse' : ''}`}>
      {/* Avatar */}
      <div
        className={`w-7 h-7 rounded-full flex items-center justify-center flex-shrink-0 ${
          isUser ? 'bg-accent/20 text-accent' : 'bg-surface-700 text-surface-300'
        }`}
      >
        {isUser ? <User size={14} /> : <Bot size={14} />}
      </div>

      {/* Content */}
      <div
        className={`max-w-[85%] rounded-lg px-3 py-2 text-sm ${
          isUser
            ? 'bg-accent/10 border border-accent/20 text-surface-100'
            : 'bg-surface-800 border border-surface-700 text-surface-200'
        }`}
      >
        <MarkdownRenderer content={message.content} />
        {isStreaming && (
          <span className="inline-block w-1.5 h-4 bg-accent ml-0.5 animate-pulse" />
        )}
      </div>
    </div>
  );
}
