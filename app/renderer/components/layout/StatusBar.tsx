import React, { useState, useEffect } from 'react';
import { Wifi, WifiOff, Loader2, CheckCircle2, XCircle, Activity, FileText, Mail } from 'lucide-react';
import { usePipelineStore } from '../../stores/pipelineStore';
import { useSettingsStore } from '../../stores/settingsStore';
import { useXlsxStore } from '../../stores/xlsxStore';
import { useConsoleStore } from '../../stores/consoleStore';

export function StatusBar() {
  const pipelineState = usePipelineStore((s) => s.state);
  const currentStage = usePipelineStore((s) => s.currentStage);
  const progress = usePipelineStore((s) => s.progress);
  const extraction = usePipelineStore((s) => s.extraction);
  const extractionProgress = usePipelineStore((s) => s.extractionProgress);
  const extractionResult = usePipelineStore((s) => s.extractionResult);
  const extractionError = usePipelineStore((s) => s.extractionError);
  const settings = useSettingsStore((s) => s.settings);
  const errorCount = useXlsxStore((s) => s.errors.length);
  const lastEmailCheck = useConsoleStore((s) => s.lastEmailCheck);
  const emailPollInterval = useConsoleStore((s) => s.emailPollInterval);

  const hasApiKey = !!settings.apiKey;

  // Email check countdown
  const [countdown, setCountdown] = useState('');
  useEffect(() => {
    if (!lastEmailCheck) return;
    const tick = () => {
      const elapsed = Date.now() - lastEmailCheck;
      const remaining = Math.max(0, Math.ceil((emailPollInterval - elapsed) / 1000));
      setCountdown(`${remaining}s`);
    };
    tick();
    const id = setInterval(tick, 1000);
    return () => clearInterval(id);
  }, [lastEmailCheck, emailPollInterval]);

  return (
    <div className="h-6 flex items-center px-3 bg-surface-950 border-t border-surface-700 text-[11px] text-surface-400 select-none gap-4 min-w-0 overflow-hidden">
      {/* Connection status */}
      <div className="flex items-center gap-1">
        {hasApiKey ? (
          <>
            <Wifi size={11} className="text-green-400" />
            <span>Connected</span>
          </>
        ) : (
          <>
            <WifiOff size={11} className="text-red-400" />
            <span>No API Key</span>
          </>
        )}
      </div>

      {/* Model */}
      <div className="flex items-center gap-1">
        <Activity size={11} />
        <span>{settings.model || 'glm-4.7'}</span>
      </div>

      {/* Extraction status */}
      <div className="flex items-center gap-1">
        {extraction === 'extracting' && (
          <>
            <Loader2 size={11} className="animate-spin text-purple-400" />
            <span className="text-purple-400">
              {extractionProgress?.message || 'Extracting text...'}
              {extractionProgress?.item && extractionProgress?.total
                ? ` (${extractionProgress.item}/${extractionProgress.total})`
                : ''}
            </span>
          </>
        )}
        {extraction === 'success' && extractionResult && (
          <>
            <FileText size={11} className="text-green-400" />
            <span className="text-green-400">
              Text extracted ({extractionResult.method})
            </span>
          </>
        )}
        {extraction === 'error' && (
          <>
            <XCircle size={11} className="text-red-400" />
            <span className="text-red-400">Extraction failed</span>
          </>
        )}
      </div>

      {/* Pipeline status */}
      <div className="flex items-center gap-1">
        {pipelineState === 'running' && (
          <>
            <Loader2 size={11} className="animate-spin text-blue-400" />
            <span className="text-blue-400">
              {currentStage || 'Running'}
              {progress?.item !== undefined && progress?.total
                ? ` (${progress.item}/${progress.total})`
                : ''}
            </span>
          </>
        )}
        {pipelineState === 'success' && (
          <>
            <CheckCircle2 size={11} className="text-green-400" />
            <span className="text-green-400">Pipeline complete</span>
          </>
        )}
        {pipelineState === 'error' && (
          <>
            <XCircle size={11} className="text-red-400" />
            <span className="text-red-400">Pipeline error</span>
          </>
        )}
        {pipelineState === 'idle' && <span>Pipeline: idle</span>}
      </div>

      {/* Email check countdown */}
      {lastEmailCheck > 0 && (
        <div className="flex items-center gap-1">
          <Mail size={11} />
          <span>Next check: {countdown}</span>
        </div>
      )}

      {/* Spacer */}
      <div className="flex-1" />

      {/* XLSX errors */}
      {errorCount > 0 && (
        <div className="flex items-center gap-1 text-red-400">
          <XCircle size={11} />
          <span>{errorCount} error{errorCount !== 1 ? 's' : ''}</span>
        </div>
      )}
    </div>
  );
}
