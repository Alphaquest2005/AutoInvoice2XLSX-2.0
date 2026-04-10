import { create } from 'zustand';
import type { AppSettings } from '../../shared/types';

interface SettingsState {
  settings: AppSettings;
  settingsOpen: boolean;
  loaded: boolean;

  loadSettings: () => Promise<void>;
  updateSettings: (partial: Partial<AppSettings>) => Promise<void>;
  setApiKey: (key: string) => Promise<void>;
  toggleSettings: () => void;
}

const defaultSettings: AppSettings = {
  apiKey: '',
  baseUrl: 'https://api.z.ai/api/anthropic',
  model: 'glm-5',  // SSOT: src/autoinvoice/domain/models/settings.py
  workspacePath: '',
  theme: 'dark',
  enabledStages: ['extract', 'parse', 'classify', 'validate_codes', 'group', 'generate_xlsx', 'verify', 'learn'],
};

export const useSettingsStore = create<SettingsState>((set, get) => ({
  settings: defaultSettings,
  settingsOpen: false,
  loaded: false,

  loadSettings: async () => {
    if (!window.api) return;
    const settings = await window.api.getSettings();
    set({ settings: { ...defaultSettings, ...settings }, loaded: true });

    // Apply theme
    const theme = settings.theme || 'dark';
    if (theme === 'dark') {
      document.documentElement.classList.add('dark');
    } else if (theme === 'light') {
      document.documentElement.classList.remove('dark');
    }
  },

  updateSettings: async (partial: Partial<AppSettings>) => {
    if (!window.api) return;
    const current = get().settings;
    const updated = { ...current, ...partial };
    await window.api.saveSettings(partial);
    set({ settings: updated });

    // Apply theme change
    if (partial.theme) {
      if (partial.theme === 'dark') {
        document.documentElement.classList.add('dark');
      } else {
        document.documentElement.classList.remove('dark');
      }
    }
  },

  setApiKey: async (key: string) => {
    if (!window.api) return;
    await window.api.setApiKey(key);
    set((state) => ({ settings: { ...state.settings, apiKey: key } }));
  },

  toggleSettings: () => set((state) => ({ settingsOpen: !state.settingsOpen })),
}));
