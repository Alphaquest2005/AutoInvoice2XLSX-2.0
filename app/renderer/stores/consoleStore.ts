import { create } from 'zustand';

export type LogLevel = 'info' | 'success' | 'error' | 'warn' | 'progress';
export type LogSource = 'pipeline' | 'email' | 'extraction' | 'system';

export interface ConsoleEntry {
  id: string;
  timestamp: string;
  level: LogLevel;
  source: LogSource;
  message: string;
}

interface ConsoleState {
  entries: ConsoleEntry[];
  maxEntries: number;
  filter: LogSource | 'all';
  hasUnread: boolean;
  lastEmailCheck: number;        // timestamp (ms) of last email check
  emailPollInterval: number;     // poll interval in ms (60s)

  addEntry: (level: LogLevel, source: LogSource, message: string) => void;
  clear: () => void;
  setFilter: (filter: LogSource | 'all') => void;
  clearUnread: () => void;
  setLastEmailCheck: () => void;
}

// ── Batched entry flushing ──
// Instead of triggering a React re-render on every single log line,
// buffer incoming entries and flush them into state at most every 150ms.
let pendingEntries: ConsoleEntry[] = [];
let flushTimer: ReturnType<typeof setTimeout> | null = null;
const FLUSH_INTERVAL = 150; // ms

function scheduleFlush() {
  if (flushTimer) return; // already scheduled
  flushTimer = setTimeout(() => {
    flushTimer = null;
    if (pendingEntries.length === 0) return;
    const batch = pendingEntries;
    pendingEntries = [];
    useConsoleStore.setState((state) => {
      const merged = [...state.entries, ...batch];
      const trimmed = merged.length > state.maxEntries
        ? merged.slice(merged.length - state.maxEntries)
        : merged;
      return { entries: trimmed, hasUnread: true };
    });
  }, FLUSH_INTERVAL);
}

export const useConsoleStore = create<ConsoleState>((set) => ({
  entries: [],
  maxEntries: 2000,
  filter: 'all',
  hasUnread: false,
  lastEmailCheck: 0,
  emailPollInterval: 60000,

  addEntry: (level, source, message) => {
    const entry: ConsoleEntry = {
      id: `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      timestamp: new Date().toISOString(),
      level,
      source,
      message,
    };
    pendingEntries.push(entry);
    scheduleFlush();
  },

  clear: () => {
    pendingEntries = [];
    set({ entries: [] });
  },

  setFilter: (filter) => set({ filter }),

  clearUnread: () => set({ hasUnread: false }),

  setLastEmailCheck: () => set({ lastEmailCheck: Date.now() }),
}));
