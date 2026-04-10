import React from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import { CodeBlock } from './CodeBlock';
import { usePreviewStore } from '../../stores/previewStore';
import { useXlsxStore } from '../../stores/xlsxStore';
import { useFileStore } from '../../stores/fileStore';

interface Props {
  content: string;
}

/** Detect file paths in text and make them clickable. */
export const FILE_PATH_RE = /(?:(?:[A-Za-z]:)?[\\/])?(?:workspace|output|emails|data|config|pipeline)[\\/][^\s"'`<>|*?,;()[\]{}]+\.\w{1,5}/g;

export function FileLink({ path: filePath }: { path: string }) {
  const openPreview = usePreviewStore((s) => s.openFile);
  const loadXlsx = useXlsxStore((s) => s.loadFile);
  const selectFile = useFileStore((s) => s.selectFile);

  const handleClick = async (e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    selectFile(filePath);
    const ext = filePath.split('.').pop()?.toLowerCase();
    if (ext === 'xlsx' || ext === 'xls') {
      await loadXlsx(filePath);
    }
    await openPreview(filePath);
  };

  const displayName = filePath.replace(/\\/g, '/').split('/').pop() || filePath;

  return (
    <a
      href="#"
      onClick={handleClick}
      className="text-accent hover:underline cursor-pointer inline-flex items-center gap-0.5"
      title={filePath}
    >
      {displayName}
    </a>
  );
}

/** Replace file paths in a text node with clickable FileLink components. */
export function linkifyFilePaths(text: string): React.ReactNode[] {
  const parts: React.ReactNode[] = [];
  let lastIndex = 0;

  for (const match of text.matchAll(FILE_PATH_RE)) {
    const matchStart = match.index!;
    if (matchStart > lastIndex) {
      parts.push(text.slice(lastIndex, matchStart));
    }
    parts.push(<FileLink key={matchStart} path={match[0]} />);
    lastIndex = matchStart + match[0].length;
  }

  if (lastIndex < text.length) {
    parts.push(text.slice(lastIndex));
  }

  return parts.length > 0 ? parts : [text];
}

/** Recursively process React children to linkify file paths in text nodes. */
function processChildren(children: React.ReactNode): React.ReactNode {
  return React.Children.map(children, (child) => {
    if (typeof child === 'string') {
      const linked = linkifyFilePaths(child);
      return linked.length === 1 && linked[0] === child ? child : <>{linked}</>;
    }
    return child;
  });
}

export function MarkdownRenderer({ content }: Props) {
  return (
    <ReactMarkdown
      remarkPlugins={[remarkGfm]}
      className="prose prose-invert prose-sm max-w-none"
      components={{
        code({ node, className, children, ...props }) {
          const match = /language-(\w+)/.exec(className || '');
          const isInline = !match && !String(children).includes('\n');

          if (isInline) {
            // Linkify file paths inside inline code
            const text = String(children);
            if (FILE_PATH_RE.test(text)) {
              FILE_PATH_RE.lastIndex = 0;
              return (
                <code className="px-1 py-0.5 text-xs bg-surface-700 rounded font-mono" {...props}>
                  {linkifyFilePaths(text)}
                </code>
              );
            }
            return (
              <code className="px-1 py-0.5 text-xs bg-surface-700 rounded font-mono text-accent" {...props}>
                {children}
              </code>
            );
          }

          return (
            <CodeBlock language={match?.[1] || 'text'}>
              {String(children).replace(/\n$/, '')}
            </CodeBlock>
          );
        },
        table({ children }) {
          return (
            <div className="overflow-x-auto my-2">
              <table className="text-xs border-collapse border border-surface-600 w-full">
                {children}
              </table>
            </div>
          );
        },
        th({ children }) {
          return (
            <th className="border border-surface-600 bg-surface-700 px-2 py-1 text-left text-xs font-medium">
              {children}
            </th>
          );
        },
        td({ children }) {
          return (
            <td className="border border-surface-600 px-2 py-1 text-xs">
              {processChildren(children)}
            </td>
          );
        },
        p({ children }) {
          return <p className="mb-2 last:mb-0 leading-relaxed">{processChildren(children)}</p>;
        },
        li({ children }) {
          return <li>{processChildren(children)}</li>;
        },
        ul({ children }) {
          return <ul className="list-disc list-inside mb-2 space-y-0.5">{children}</ul>;
        },
        ol({ children }) {
          return <ol className="list-decimal list-inside mb-2 space-y-0.5">{children}</ol>;
        },
        a({ href, children }) {
          return (
            <a href={href} className="text-accent hover:underline" target="_blank" rel="noopener">
              {children}
            </a>
          );
        },
        blockquote({ children }) {
          return (
            <blockquote className="border-l-2 border-surface-500 pl-3 my-2 text-surface-400 italic">
              {children}
            </blockquote>
          );
        },
      }}
    >
      {content}
    </ReactMarkdown>
  );
}
