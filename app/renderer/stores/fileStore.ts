import { create } from 'zustand';
import type { FileNode, RecentFile } from '../../shared/types';

interface FileState {
  tree: FileNode | null;
  currentPath: string;
  expandedDirs: Set<string>;
  selectedFiles: Set<string>;
  lastClickedPath: string | null;
  focusedPath: string | null;
  recentFiles: RecentFile[];
  clipboard: { paths: string[] } | null;

  loadTree: (dirPath?: string) => Promise<void>;
  setCurrentPath: (path: string) => void;
  navigateTo: (path: string) => Promise<void>;
  toggleDir: (path: string) => void;
  selectFile: (path: string) => void;
  toggleFileSelection: (path: string) => void;
  selectFileRange: (paths: string[]) => void;
  clearSelection: () => void;
  setFocusedPath: (path: string | null) => void;
  refresh: () => Promise<void>;
  setClipboard: (paths: string[]) => void;
  clearClipboard: () => void;
}

export const useFileStore = create<FileState>((set, get) => ({
  tree: null,
  currentPath: '',
  expandedDirs: new Set(),
  selectedFiles: new Set(),
  lastClickedPath: null,
  focusedPath: null,
  recentFiles: [],
  clipboard: null,

  loadTree: async (dirPath?: string) => {
    if (!window.api) return;
    const tree = await window.api.getFileTree(dirPath);
    set({ tree, currentPath: tree?.path || dirPath || '' });
  },

  setCurrentPath: (path: string) => {
    set({ currentPath: path });
  },

  navigateTo: async (path: string) => {
    if (!window.api) return;
    try {
      const tree = await window.api.getFileTree(path);
      if (tree) {
        set({ tree, currentPath: tree.path, expandedDirs: new Set(), selectedFiles: new Set() });
      }
    } catch (err) {
      console.error('Failed to navigate to path:', path, err);
    }
  },

  toggleDir: (path: string) => {
    set((state) => {
      const next = new Set(state.expandedDirs);
      if (next.has(path)) {
        next.delete(path);
      } else {
        next.add(path);
      }
      return { expandedDirs: next };
    });
  },

  selectFile: (path: string) => {
    set({ selectedFiles: new Set([path]), lastClickedPath: path, focusedPath: path });
  },

  toggleFileSelection: (path: string) => {
    set((state) => {
      const next = new Set(state.selectedFiles);
      if (next.has(path)) {
        next.delete(path);
      } else {
        next.add(path);
      }
      return { selectedFiles: next, lastClickedPath: path };
    });
  },

  selectFileRange: (paths: string[]) => {
    set({ selectedFiles: new Set(paths) });
  },

  clearSelection: () => {
    set({ selectedFiles: new Set(), lastClickedPath: null });
  },

  setFocusedPath: (path: string | null) => {
    set({ focusedPath: path });
  },

  refresh: async () => {
    if (!window.api) return;
    const { tree } = get();
    if (tree) {
      const updated = await window.api.getFileTree(tree.path);
      set({ tree: updated });
    }
  },

  setClipboard: (paths: string[]) => set({ clipboard: { paths } }),
  clearClipboard: () => set({ clipboard: null }),
}));
