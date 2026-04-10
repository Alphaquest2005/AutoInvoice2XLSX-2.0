import React, { useState, useEffect } from 'react';
import { X, Eye, EyeOff } from 'lucide-react';
import { useSettingsStore } from '../../stores/settingsStore';

export function SettingsModal() {
  const { settings, updateSettings, setApiKey, toggleSettings } = useSettingsStore();
  const [apiKey, setLocalApiKey] = useState('');
  const [showKey, setShowKey] = useState(false);

  useEffect(() => {
    if (!window.api) return;
    window.api.getApiKey().then((key) => setLocalApiKey(key));
  }, []);

  const handleSaveKey = async () => {
    await setApiKey(apiKey);
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60">
      <div className="bg-surface-800 rounded-lg border border-surface-600 w-[90vw] max-w-[500px] max-h-[80vh] overflow-y-auto shadow-2xl">
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-3 border-b border-surface-600">
          <h2 className="text-sm font-semibold text-surface-100">Settings</h2>
          <button onClick={toggleSettings} className="text-surface-400 hover:text-surface-100">
            <X size={16} />
          </button>
        </div>

        <div className="p-5 space-y-5">
          {/* API Key */}
          <div>
            <label className="block text-xs font-medium text-surface-300 mb-1.5">Z.AI API Key</label>
            <div className="flex gap-2">
              <div className="relative flex-1">
                <input
                  type={showKey ? 'text' : 'password'}
                  value={apiKey}
                  onChange={(e) => setLocalApiKey(e.target.value)}
                  placeholder="Enter your Z.AI API key"
                  className="w-full px-3 py-2 text-sm bg-surface-900 border border-surface-600 rounded text-surface-100 placeholder-surface-500 focus:outline-none focus:border-accent"
                />
                <button
                  onClick={() => setShowKey(!showKey)}
                  className="absolute right-2 top-1/2 -translate-y-1/2 text-surface-400 hover:text-surface-200"
                >
                  {showKey ? <EyeOff size={14} /> : <Eye size={14} />}
                </button>
              </div>
              <button
                onClick={handleSaveKey}
                className="px-3 py-2 text-sm bg-accent hover:bg-accent-hover text-white rounded transition-colors"
              >
                Save
              </button>
            </div>
          </div>

          {/* Base URL */}
          <div>
            <label className="block text-xs font-medium text-surface-300 mb-1.5">API Base URL</label>
            <input
              type="text"
              value={settings.baseUrl}
              onChange={(e) => updateSettings({ baseUrl: e.target.value })}
              className="w-full px-3 py-2 text-sm bg-surface-900 border border-surface-600 rounded text-surface-100 focus:outline-none focus:border-accent"
            />
          </div>

          {/* Model Selection */}
          <div>
            <label className="block text-xs font-medium text-surface-300 mb-1.5">Model</label>
            <select
              value={settings.model}
              onChange={(e) => updateSettings({ model: e.target.value })}
              className="w-full px-3 py-2 text-sm bg-surface-900 border border-surface-600 rounded text-surface-100 focus:outline-none focus:border-accent"
            >
              <option value="glm-4.7">GLM-4.7 (Default)</option>
              <option value="glm-4.5-air">GLM-4.5 Air (Fast)</option>
            </select>
          </div>

          {/* Theme */}
          <div>
            <label className="block text-xs font-medium text-surface-300 mb-1.5">Theme</label>
            <select
              value={settings.theme}
              onChange={(e) => updateSettings({ theme: e.target.value as 'light' | 'dark' })}
              className="w-full px-3 py-2 text-sm bg-surface-900 border border-surface-600 rounded text-surface-100 focus:outline-none focus:border-accent"
            >
              <option value="dark">Dark</option>
              <option value="light">Light</option>
            </select>
          </div>

          {/* Workspace Path */}
          <div>
            <label className="block text-xs font-medium text-surface-300 mb-1.5">Workspace Directory</label>
            <div className="flex gap-2">
              <input
                type="text"
                value={settings.workspacePath}
                readOnly
                className="flex-1 px-3 py-2 text-sm bg-surface-900 border border-surface-600 rounded text-surface-400"
              />
              <button
                onClick={async () => {
                  const dir = await window.api.openFileDialog([{ name: 'Directories', extensions: ['*'] }]);
                  if (dir) updateSettings({ workspacePath: dir });
                }}
                className="px-3 py-2 text-sm bg-surface-700 hover:bg-surface-600 text-surface-200 rounded transition-colors"
              >
                Browse
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
