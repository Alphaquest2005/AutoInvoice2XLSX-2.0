import React, { useState } from 'react';
import { Prism as SyntaxHighlighter } from 'react-syntax-highlighter';
import { oneDark } from 'react-syntax-highlighter/dist/esm/styles/prism';
import { Copy, Check } from 'lucide-react';

interface Props {
  language: string;
  children: string;
}

export function CodeBlock({ language, children }: Props) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(children);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="relative group my-2 rounded-md overflow-hidden">
      {/* Language label + copy button */}
      <div className="flex items-center justify-between px-3 py-1 bg-surface-800 border-b border-surface-700">
        <span className="text-[10px] text-surface-500 uppercase">{language}</span>
        <button
          onClick={handleCopy}
          className="text-surface-500 hover:text-surface-200 opacity-0 group-hover:opacity-100 transition-opacity"
        >
          {copied ? <Check size={12} className="text-green-400" /> : <Copy size={12} />}
        </button>
      </div>

      <SyntaxHighlighter
        language={language}
        style={oneDark}
        customStyle={{
          margin: 0,
          padding: '12px',
          fontSize: '12px',
          lineHeight: '1.5',
          background: '#1e293b',
        }}
        showLineNumbers={children.split('\n').length > 5}
        lineNumberStyle={{ color: '#475569', fontSize: '10px' }}
      >
        {children}
      </SyntaxHighlighter>
    </div>
  );
}
