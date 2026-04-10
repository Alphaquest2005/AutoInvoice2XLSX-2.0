import React, { useEffect, useState } from 'react';
import { PanelLayout } from './components/layout/PanelLayout';
import { TitleBar } from './components/layout/TitleBar';
import { StatusBar } from './components/layout/StatusBar';
import { DragDropOverlay } from './components/common/DragDropOverlay';
import { SettingsModal } from './components/layout/SettingsModal';
import { useChatStore } from './stores/chatStore';
import { useFileStore } from './stores/fileStore';
import { usePipelineStore } from './stores/pipelineStore';
import { useXlsxStore } from './stores/xlsxStore';
import { useSettingsStore } from './stores/settingsStore';
import { useConsoleStore } from './stores/consoleStore';
import type { LogLevel, LogSource } from './stores/consoleStore';
import type { SessionState, Message } from '../shared/types';

export default function App() {
  const loadConversations = useChatStore((s) => s.loadConversations);
  const selectConversation = useChatStore((s) => s.selectConversation);
  const sendMessage = useChatStore((s) => s.sendMessage);
  const handleStreamChunk = useChatStore((s) => s.handleStreamChunk);
  const loadTree = useFileStore((s) => s.loadTree);
  const handleProgress = usePipelineStore((s) => s.handleProgress);
  const handleComplete = usePipelineStore((s) => s.handleComplete);
  const handleError = usePipelineStore((s) => s.handleError);
  const handleExtractionProgress = usePipelineStore((s) => s.handleExtractionProgress);
  const handleExtractionComplete = usePipelineStore((s) => s.handleExtractionComplete);
  const handleExtractionError = usePipelineStore((s) => s.handleExtractionError);
  const loadSettings = useSettingsStore((s) => s.loadSettings);
  const settingsOpen = useSettingsStore((s) => s.settingsOpen);

  const [pendingSession, setPendingSession] = useState<SessionState | null>(null);

  useEffect(() => {
    // ─── Global Error Handlers for Renderer ──────────────────────────────────
    const handleGlobalError = (event: ErrorEvent) => {
      console.error('═══════════════════════════════════════════════════════════');
      console.error('[RENDERER] GLOBAL ERROR:');
      console.error('  Message:', event.message);
      console.error('  Filename:', event.filename);
      console.error('  Line:', event.lineno, 'Col:', event.colno);
      console.error('  Error:', event.error);
      if (event.error?.stack) {
        console.error('  Stack:', event.error.stack);
      }
      console.error('═══════════════════════════════════════════════════════════');
      // Prevent default browser error handling (don't show alert)
      event.preventDefault();
    };

    const handleUnhandledRejection = (event: PromiseRejectionEvent) => {
      console.error('═══════════════════════════════════════════════════════════');
      console.error('[RENDERER] UNHANDLED PROMISE REJECTION:');
      console.error('  Reason:', event.reason);
      if (event.reason instanceof Error) {
        console.error('  Name:', event.reason.name);
        console.error('  Message:', event.reason.message);
        console.error('  Stack:', event.reason.stack);
      }
      console.error('═══════════════════════════════════════════════════════════');
      event.preventDefault();
    };

    window.addEventListener('error', handleGlobalError);
    window.addEventListener('unhandledrejection', handleUnhandledRejection);

    if (!window.api) {
      console.error('window.api is not available - preload script may have failed to load');
      return;
    }

    // Load initial data
    loadSettings();
    loadConversations();
    loadTree();

    // Check for pending session (app was closed while LLM was working)
    window.api.getPendingSession().then((session) => {
      if (session && session.wasLlmBusy) {
        console.log('[resume] Found pending session:', session);
        setPendingSession(session);
      }
    });

    // ─── Console log helper ───
    const log = (level: LogLevel, source: LogSource, msg: string) =>
      useConsoleStore.getState().addEntry(level, source, msg);

    // Register IPC listeners
    const unsubStream = window.api.onStreamChunk(handleStreamChunk);
    const unsubProgress = window.api.onPipelineProgress((progress: any) => {
      handleProgress(progress);
      const msg = progress.message?.trim();
      if (msg && !msg.startsWith('REPORT:JSON:')) {
        log('progress', 'pipeline', msg);
      }
    });
    const unsubComplete = window.api.onPipelineComplete((report: any) => {
      handleComplete(report);
      loadTree(); // Refresh file tree to show generated XLSX

      if (report.blPipeline) {
        const msg = report.status === 'success'
          ? `BL pipeline completed: ${report.invoiceCount ?? 0} invoice(s) processed, email ${report.emailSent ? 'sent' : 'not sent'}${report.failures ? `, ${report.failures} failure(s)` : ''}`
          : `BL pipeline failed: ${report.errors?.join('; ') || 'unknown error'}`;
        log(report.status === 'success' ? 'success' : 'error', 'email', msg);
        loadTree(); // Refresh file tree for new shipment output
      } else if (report.batch) {
        const msg = report.status === 'success'
          ? `Batch pipeline completed: ${report.success}/${report.total} files processed successfully. Output: ${report.output}`
          : `Batch pipeline finished: ${report.success} succeeded, ${report.failed} failed out of ${report.total}. ${report.errors?.join('; ') || ''}`;
        log(report.status === 'success' ? 'success' : 'error', 'pipeline', msg);
      } else if (report.status === 'success' && report.output) {
        useXlsxStore.getState().loadFile(report.output);
        log('success', 'pipeline', `Pipeline completed successfully. Output: ${report.output}`);
      } else if (report.status === 'failed' && report.errors?.length) {
        log('error', 'pipeline', `Pipeline failed: ${report.errors.join('; ')}`);
      }
    });
    const unsubError = window.api.onPipelineError((error) => {
      handleError(error);
      log('error', 'pipeline', `Pipeline error: ${error}`);
    });
    const unsubFiles = window.api.onFileChanged(() => {
      loadTree();
    });
    const unsubExtProgress = window.api.onExtractionProgress((progress) => {
      handleExtractionProgress(progress);
      if (progress.message) {
        log('progress', 'extraction', progress.message);
      }
    });
    const unsubExtComplete = window.api.onExtractionComplete((result) => {
      handleExtractionComplete(result);
      loadTree();
      if (result.status === 'success') {
        log('success', 'extraction', `Extraction complete: ${result.method || 'unknown method'}`);
        if (result.output) {
          usePipelineStore.getState().run(result.output);
        }
      } else {
        log('error', 'extraction', `Extraction failed`);
      }
    });
    const unsubExtError = window.api.onExtractionError((error) => {
      handleExtractionError(error);
      log('error', 'extraction', `Extraction error: ${error}`);
    });

    // ─── Email pipeline progress → console ───
    const unsubEmailProgress = window.api.onEmailProgress?.((data: { clientId: string; message: string }) => {
      const msg = data.message.trim();
      if (!msg || msg.startsWith('REPORT:JSON:')) return;
      log('info', 'email', msg);
      // Track when email check completes (for status bar countdown)
      if (msg.includes('unseen email')) {
        useConsoleStore.getState().setLastEmailCheck();
      }
    });

    // Auto-load XLSX when LLM's pipeline tool produces output
    const unsubAutoLoad = window.api.onXlsxAutoLoad((filePath) => {
      console.log('Auto-loading XLSX from LLM pipeline:', filePath);
      useXlsxStore.getState().loadFile(filePath).catch((err) => {
        console.warn('Auto-load XLSX failed (file may not exist):', err.message);
      });
      loadTree();
    });

    // ─── Serialized LLM auto-fix queue ───
    // Prevents race conditions when multiple auto-fix events fire simultaneously.
    // Each handler queues its work; only one runs at a time.
    let autoFixBusy = false;
    const autoFixQueue: Array<() => Promise<void>> = [];

    const enqueueAutoFix = (task: () => Promise<void>) => {
      autoFixQueue.push(task);
      drainAutoFixQueue();
    };

    const drainAutoFixQueue = async () => {
      if (autoFixBusy || autoFixQueue.length === 0) return;
      autoFixBusy = true;
      while (autoFixQueue.length > 0) {
        const task = autoFixQueue.shift()!;
        try { await task(); } catch (err) { console.error('[auto-fix] Task failed:', err); }
      }
      autoFixBusy = false;
    };

    /** Wait for LLM streaming to finish */
    const waitForIdle = () => new Promise<void>((resolve) => {
      const check = () => {
        if (!useChatStore.getState().isStreaming) return resolve();
        setTimeout(check, 500);
      };
      check();
    });

    /** Create conversation, send prompt, wait for completion. Returns after LLM finishes. */
    const runAutoFix = async (systemMsg: string, prompt: string): Promise<void> => {
      await useChatStore.getState().createConversation();
      const convId = useChatStore.getState().activeConversationId;
      if (!convId) { console.error('[auto-fix] No conversation created'); return; }

      await useChatStore.getState().addSystemMessage(systemMsg);
      await waitForIdle();

      // Add user message to local state + set streaming flag (mirrors chatStore.sendMessage)
      const userMsg: Message = {
        id: Date.now().toString(),
        conversationId: convId,
        role: 'user',
        content: prompt,
        createdAt: new Date().toISOString(),
      };
      useChatStore.setState((state) => ({
        messages: [...state.messages, userMsg],
        isStreaming: true,
        streamingContent: '',
        streamingToolUse: null,
      }));
      window.api?.notifyLlmBusy(true);

      // Send directly via IPC with explicit conversationId to avoid race on activeConversationId
      window.api.sendMessage(convId, prompt);

      // Wait for isStreaming to flip true, then wait for it to finish
      await new Promise((r) => setTimeout(r, 2000));
      await waitForIdle();
    };

    // ─── Pipeline failure auto-fix via LLM ───
    const unsubFailures = window.api.onPipelineFailures?.(async (data) => {
      const { failures, inputDir, prompt } = data;
      log('warn', 'pipeline', `${failures.length} invoice(s) failed to import. LLM auto-fix initiated.`);

      enqueueAutoFix(async () => {
        await runAutoFix(
          `WARNING: ${failures.length} invoice(s) failed to import. LLM auto-fix initiated.`,
          prompt,
        );
        log('info', 'pipeline', 'LLM fix complete. Re-running full pipeline...');
        window.api.rerunBLPipeline(inputDir);
      });
    });

    // ─── Validation issues auto-fix via LLM ───
    const unsubValidation = window.api.onValidationIssues?.(async (data) => {
      const { validation, inputDir: valInputDir, prompt } = data;
      log('warn', 'pipeline', `${validation.unfixed} validation issue(s) unfixed. LLM auto-fix initiated.`);

      enqueueAutoFix(async () => {
        await runAutoFix(
          `VALIDATION: ${validation.unfixed} issue(s) remain after pipeline auto-fix. LLM attempting resolution...`,
          prompt,
        );
        log('info', 'pipeline', 'LLM validation fix complete.');
      });
    });

    // ─── Checklist failure auto-fix via LLM ───
    const unsubChecklist = window.api.onChecklistFailed?.(async (data) => {
      const { checklist, inputDir: chkInputDir, prompt } = data;

      const blockerDetails = checklist.failures
        ?.map((f: any) => `${f.severity === 'block' ? 'BLOCK' : 'WARN'}: ${f.check} — ${f.message}`)
        .join('; ') || 'unknown';
      log('error', 'pipeline', `Email BLOCKED: ${checklist.blocker_count} blocker(s). LLM auto-fix initiated.`);
      log('info', 'pipeline', `Checklist failures: ${blockerDetails}`);
      log('info', 'pipeline', `Email params: ${data.emailParamsPath}`);
      log('info', 'pipeline', `Output dir: ${data.outputDir || chkInputDir}`);

      enqueueAutoFix(async () => {
        log('info', 'pipeline', 'Sending fix prompt to LLM...');
        await runAutoFix(
          `EMAIL BLOCKED: ${checklist.blocker_count} pre-send checklist blocker(s) detected. LLM attempting to fix...`,
          prompt,
        );
        log('info', 'pipeline', 'LLM checklist fix complete. Re-running pipeline to re-validate and send...');
        window.api.rerunBLPipeline(chkInputDir);
      });
    });

    // Keyboard shortcuts
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.ctrlKey && e.key === 'n') {
        e.preventDefault();
        useChatStore.getState().createConversation();
      }
    };
    window.addEventListener('keydown', handleKeyDown);

    return () => {
      unsubStream();
      unsubProgress();
      unsubComplete();
      unsubError();
      unsubFiles();
      unsubExtProgress();
      unsubExtComplete();
      unsubExtError();
      unsubEmailProgress?.();
      unsubAutoLoad();
      unsubFailures?.();
      unsubValidation?.();
      unsubChecklist?.();
      window.removeEventListener('keydown', handleKeyDown);
      window.removeEventListener('error', handleGlobalError);
      window.removeEventListener('unhandledrejection', handleUnhandledRejection);
    };
  }, []);

  const handleResume = async () => {
    if (!pendingSession?.activeConversationId) {
      setPendingSession(null);
      window.api?.clearPendingSession();
      return;
    }

    // Select the conversation and send a resume message
    await selectConversation(pendingSession.activeConversationId);
    setTimeout(() => {
      sendMessage('Continue from where you left off. The app was closed while you were working.');
    }, 500);

    setPendingSession(null);
    window.api?.clearPendingSession();
  };

  const handleDismissResume = () => {
    setPendingSession(null);
    window.api?.clearPendingSession();
  };

  return (
    <div className="h-full flex flex-col bg-surface-900 text-surface-100 min-w-0 min-h-0 overflow-hidden">
      <TitleBar />

      {/* Resume Session Banner */}
      {pendingSession && (
        <div className="px-4 py-2 bg-amber-900/50 border-b border-amber-700 flex items-center justify-between">
          <div className="flex items-center gap-2 text-sm text-amber-200">
            <span>⚠️</span>
            <span>
              The app was closed while the LLM was working.
              {pendingSession.shutdownTime && (
                <span className="text-amber-400 ml-1">
                  ({new Date(pendingSession.shutdownTime).toLocaleTimeString()})
                </span>
              )}
            </span>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={handleResume}
              className="px-3 py-1 text-xs font-medium bg-amber-600 hover:bg-amber-500 text-white rounded transition-colors"
            >
              Resume
            </button>
            <button
              onClick={handleDismissResume}
              className="px-3 py-1 text-xs font-medium text-amber-400 hover:text-amber-200 transition-colors"
            >
              Dismiss
            </button>
          </div>
        </div>
      )}

      <DragDropOverlay>
        <div className="flex-1 overflow-hidden">
          <PanelLayout />
        </div>
      </DragDropOverlay>
      <StatusBar />
      {settingsOpen && <SettingsModal />}
    </div>
  );
}
