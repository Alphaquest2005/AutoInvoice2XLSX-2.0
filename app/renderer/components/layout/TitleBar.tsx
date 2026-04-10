import React, { useState, useEffect } from 'react';
import { Minus, Square, X, Settings, MessageSquarePlus, ZoomIn, ZoomOut, Loader2, Mail } from 'lucide-react';
import { useChatStore } from '../../stores/chatStore';
import { useSettingsStore } from '../../stores/settingsStore';
import { ClientManager } from './ClientManager';

type ShutdownState = 'none' | 'waiting' | 'closing';

export function TitleBar() {
  const createConversation = useChatStore((s) => s.createConversation);
  const toggleSidebar = useChatStore((s) => s.toggleSidebar);
  const activeConversationId = useChatStore((s) => s.activeConversationId);
  const isStreaming = useChatStore((s) => s.isStreaming);
  const toggleSettings = useSettingsStore((s) => s.toggleSettings);
  const [zoomLevel, setZoomLevel] = useState(100);
  const [shutdownState, setShutdownState] = useState<ShutdownState>('none');
  const [shutdownMessage, setShutdownMessage] = useState('');
  const [showClientManager, setShowClientManager] = useState(false);

  // Fetch initial zoom level
  useEffect(() => {
    window.api?.getZoomLevel().then((level) => {
      setZoomLevel(Math.round(level * 100));
    });
  }, []);

  // Listen for shutdown status updates
  useEffect(() => {
    const cleanup = window.api?.onShutdownStatus((status) => {
      if (status.status === 'waiting') {
        setShutdownState('waiting');
        setShutdownMessage(status.message);
      } else if (status.status === 'closing') {
        setShutdownState('closing');
        setShutdownMessage(status.message);
      } else if (status.status === 'cancelled') {
        setShutdownState('none');
        setShutdownMessage('');
      }
    });
    return cleanup;
  }, []);

  const handleZoomIn = () => {
    window.api?.zoomIn();
    setZoomLevel((prev) => Math.min(prev + 10, 200));
  };

  const handleZoomOut = () => {
    window.api?.zoomOut();
    setZoomLevel((prev) => Math.max(prev - 10, 50));
  };

  const handleZoomReset = () => {
    window.api?.zoomReset();
    setZoomLevel(100);
  };

  const handleClose = () => {
    if (shutdownState === 'waiting') {
      // Already waiting, force close on second click
      window.api?.forceShutdown(activeConversationId);
    } else {
      // Request graceful shutdown
      window.api?.requestShutdown(activeConversationId);
    }
  };

  const handleCancelShutdown = () => {
    window.api?.cancelShutdown();
    setShutdownState('none');
    setShutdownMessage('');
  };

  return (
    <div className="h-9 flex items-center bg-surface-950 border-b border-surface-700 select-none">
      {/* Drag region */}
      <div className="flex-1 title-bar-drag flex items-center px-3 h-full">
        <span className="text-xs font-semibold text-surface-400 tracking-wide">
          AutoInvoice2XLSX
        </span>
      </div>

      {/* Action buttons */}
      <div className="flex items-center title-bar-no-drag">
        {/* Zoom controls */}
        <div className="flex items-center border-r border-surface-700 mr-1">
          <button
            onClick={handleZoomOut}
            className="h-9 w-8 flex items-center justify-center text-surface-400 hover:text-surface-100 hover:bg-surface-700 transition-colors"
            title="Zoom Out (Ctrl+-)"
          >
            <ZoomOut size={13} />
          </button>
          <button
            onClick={handleZoomReset}
            className="h-9 px-1 flex items-center justify-center text-surface-400 hover:text-surface-100 hover:bg-surface-700 transition-colors text-xs font-mono min-w-[40px]"
            title="Reset Zoom (Ctrl+0)"
          >
            {zoomLevel}%
          </button>
          <button
            onClick={handleZoomIn}
            className="h-9 w-8 flex items-center justify-center text-surface-400 hover:text-surface-100 hover:bg-surface-700 transition-colors"
            title="Zoom In (Ctrl++)"
          >
            <ZoomIn size={13} />
          </button>
        </div>

        <button
          onClick={() => createConversation()}
          className="h-9 w-9 flex items-center justify-center text-surface-400 hover:text-surface-100 hover:bg-surface-700 transition-colors"
          title="New Conversation (Ctrl+N)"
        >
          <MessageSquarePlus size={14} />
        </button>
        <button
          onClick={toggleSidebar}
          className="h-9 w-9 flex items-center justify-center text-surface-400 hover:text-surface-100 hover:bg-surface-700 transition-colors"
          title="Chat History"
        >
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
            <rect x="3" y="3" width="7" height="7" /><rect x="14" y="3" width="7" height="7" />
            <rect x="3" y="14" width="7" height="7" /><rect x="14" y="14" width="7" height="7" />
          </svg>
        </button>
        <button
          onClick={() => setShowClientManager(true)}
          className="h-9 w-9 flex items-center justify-center text-surface-400 hover:text-surface-100 hover:bg-surface-700 transition-colors"
          title="Email Processing Clients"
        >
          <Mail size={14} />
        </button>
        <button
          onClick={toggleSettings}
          className="h-9 w-9 flex items-center justify-center text-surface-400 hover:text-surface-100 hover:bg-surface-700 transition-colors"
          title="Settings"
        >
          <Settings size={14} />
        </button>

        {/* Shutdown status indicator */}
        {shutdownState !== 'none' && (
          <div className="flex items-center gap-2 px-2 text-xs text-amber-400 border-r border-surface-700">
            <Loader2 size={12} className="animate-spin" />
            <span>{shutdownMessage}</span>
            {shutdownState === 'waiting' && (
              <button
                onClick={handleCancelShutdown}
                className="text-surface-400 hover:text-surface-100 underline"
              >
                Cancel
              </button>
            )}
          </div>
        )}

        {/* Window controls */}
        <button
          onClick={() => window.api?.minimizeWindow()}
          className="h-9 w-11 flex items-center justify-center text-surface-400 hover:text-surface-100 hover:bg-surface-700 transition-colors"
        >
          <Minus size={14} />
        </button>
        <button
          onClick={() => window.api?.maximizeWindow()}
          className="h-9 w-11 flex items-center justify-center text-surface-400 hover:text-surface-100 hover:bg-surface-700 transition-colors"
        >
          <Square size={11} />
        </button>
        <button
          onClick={handleClose}
          className={`h-9 w-11 flex items-center justify-center transition-colors ${
            shutdownState === 'waiting'
              ? 'text-amber-400 hover:text-amber-100 hover:bg-amber-600'
              : 'text-surface-400 hover:text-surface-100 hover:bg-red-600'
          }`}
          title={shutdownState === 'waiting' ? 'Click again to force close' : 'Close (waits for LLM)'}
        >
          <X size={14} />
        </button>
      </div>

      {/* Client Manager Modal */}
      {showClientManager && <ClientManager onClose={() => setShowClientManager(false)} />}
    </div>
  );
}
