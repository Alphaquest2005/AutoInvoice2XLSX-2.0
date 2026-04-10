import React, { useRef, useEffect, useCallback, useMemo, memo } from 'react';
import { Trash2 } from 'lucide-react';
import { useConsoleStore, type LogLevel, type LogSource } from '../../stores/consoleStore';

const LEVEL_COLORS: Record<LogLevel, string> = {
  info: 'text-surface-300',
  success: 'text-green-400',
  error: 'text-red-400',
  warn: 'text-amber-400',
  progress: 'text-blue-400',
};

const SOURCE_LABELS: Record<LogSource, string> = {
  pipeline: 'PIP',
  email: 'EML',
  extraction: 'EXT',
  system: 'SYS',
};

const SOURCE_COLORS: Record<LogSource, string> = {
  pipeline: 'text-violet-400',
  email: 'text-cyan-400',
  extraction: 'text-orange-400',
  system: 'text-surface-500',
};

const FILTER_OPTIONS: Array<{ value: LogSource | 'all'; label: string }> = [
  { value: 'all', label: 'All' },
  { value: 'pipeline', label: 'Pipeline' },
  { value: 'email', label: 'Email' },
  { value: 'extraction', label: 'Extraction' },
];

function formatTime(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

// Memoized individual log entry — avoids re-rendering unchanged rows
const LogEntry = memo(function LogEntry({ entry }: { entry: { id: string; timestamp: string; level: LogLevel; source: LogSource; message: string } }) {
  return (
    <div className="flex gap-1.5 min-w-0">
      <span className="text-surface-600 shrink-0">{formatTime(entry.timestamp)}</span>
      <span className={`shrink-0 w-6 text-center ${SOURCE_COLORS[entry.source]}`}>
        {SOURCE_LABELS[entry.source]}
      </span>
      <span className={`break-all ${LEVEL_COLORS[entry.level]}`}>
        {entry.message}
      </span>
    </div>
  );
});

export const ConsolePanel = memo(function ConsolePanel() {
  const entries = useConsoleStore((s) => s.entries);
  const filter = useConsoleStore((s) => s.filter);
  const setFilter = useConsoleStore((s) => s.setFilter);
  const clear = useConsoleStore((s) => s.clear);

  const listRef = useRef<HTMLDivElement>(null);
  const bottomRef = useRef<HTMLDivElement>(null);
  const isNearBottom = useRef(true);

  const handleScroll = useCallback(() => {
    const el = listRef.current;
    if (!el) return;
    isNearBottom.current = el.scrollHeight - el.scrollTop - el.clientHeight < 80;
  }, []);

  // Memoize filtered list so it only recalculates when entries or filter change
  const filtered = useMemo(
    () => (filter === 'all' ? entries : entries.filter((e) => e.source === filter)),
    [entries, filter],
  );

  useEffect(() => {
    if (isNearBottom.current) {
      bottomRef.current?.scrollIntoView({ behavior: 'auto' });
    }
  }, [filtered.length]);

  return (
    <div className="h-full flex flex-col bg-surface-950">
      {/* Filter bar */}
      <div className="h-8 px-2 flex items-center gap-1 border-b border-surface-800 shrink-0">
        {FILTER_OPTIONS.map((opt) => (
          <button
            key={opt.value}
            onClick={() => setFilter(opt.value)}
            className={`px-2 py-0.5 text-[10px] font-medium rounded transition-colors ${
              filter === opt.value
                ? 'bg-accent/20 text-accent'
                : 'text-surface-500 hover:text-surface-300 hover:bg-surface-800'
            }`}
          >
            {opt.label}
          </button>
        ))}
        <div className="flex-1" />
        <button
          onClick={clear}
          className="p-1 text-surface-600 hover:text-surface-300 transition-colors"
          title="Clear console"
        >
          <Trash2 size={12} />
        </button>
      </div>

      {/* Log list */}
      <div
        ref={listRef}
        onScroll={handleScroll}
        className="flex-1 overflow-y-auto overflow-x-hidden px-2 py-1 font-mono text-xs leading-5"
      >
        {filtered.length === 0 ? (
          <div className="flex items-center justify-center h-full text-surface-600 text-xs">
            <p>Logs will appear when the pipeline or email processor runs.</p>
          </div>
        ) : (
          filtered.map((entry) => <LogEntry key={entry.id} entry={entry} />)
        )}
        <div ref={bottomRef} />
      </div>
    </div>
  );
});
