import React, { useState, useEffect } from 'react';
import { Panel, PanelGroup, PanelResizeHandle } from 'react-resizable-panels';
import { ChatPane } from '../chat/ChatPane';
import { FileBrowser } from '../files/FileBrowser';
import { PreviewPane } from '../preview/PreviewPane';
import { ConversationSidebar } from '../chat/ConversationSidebar';
import { useChatStore } from '../../stores/chatStore';

function useWindowSize() {
  const [size, setSize] = useState({ width: window.innerWidth, height: window.innerHeight });
  useEffect(() => {
    const onResize = () => setSize({ width: window.innerWidth, height: window.innerHeight });
    window.addEventListener('resize', onResize);
    return () => window.removeEventListener('resize', onResize);
  }, []);
  return size;
}

export function PanelLayout() {
  const sidebarOpen = useChatStore((s) => s.sidebarOpen);
  const { width } = useWindowSize();

  // Stack vertically below 768px
  const isNarrow = width < 768;

  return (
    <div className="h-full flex min-w-0 min-h-0">
      {/* Conversation sidebar (overlay) */}
      {sidebarOpen && <ConversationSidebar />}

      {/* Main panel layout */}
      <PanelGroup
        direction={isNarrow ? 'vertical' : 'horizontal'}
        className="flex-1 min-w-0 min-h-0"
        autoSaveId="main-panels"
      >
        {/* Chat Pane */}
        <Panel defaultSize={isNarrow ? 40 : 30} minSize={isNarrow ? 15 : 20} collapsible>
          <div className="h-full min-w-0 min-h-0 overflow-hidden">
            <ChatPane />
          </div>
        </Panel>

        <PanelResizeHandle className={isNarrow ? 'h-1 resize-handle' : 'w-1 resize-handle'} />

        {/* File Browser Pane */}
        <Panel defaultSize={isNarrow ? 30 : 25} minSize={isNarrow ? 10 : 15} collapsible>
          <div className="h-full min-w-0 min-h-0 overflow-hidden">
            <FileBrowser />
          </div>
        </Panel>

        <PanelResizeHandle className={isNarrow ? 'h-1 resize-handle' : 'w-1 resize-handle'} />

        {/* Preview Pane */}
        <Panel defaultSize={isNarrow ? 30 : 45} minSize={isNarrow ? 15 : 25}>
          <div className="h-full min-w-0 min-h-0 overflow-hidden">
            <PreviewPane />
          </div>
        </Panel>
      </PanelGroup>
    </div>
  );
}
