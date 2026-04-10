import React, { useState, useCallback, useRef, useEffect } from 'react';
import { Upload, FileText, Table2, FileCode } from 'lucide-react';
import { useChatStore } from '../../stores/chatStore';
import { useXlsxStore } from '../../stores/xlsxStore';
import { usePipelineStore } from '../../stores/pipelineStore';
import { useFileStore } from '../../stores/fileStore';

interface Props {
  children: React.ReactNode;
}

function formatSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function shortPath(fullPath: string): string {
  const parts = fullPath.replace(/\\/g, '/').split('/');
  // Show last 3 segments: e.g. "workspace/input/2026-02-05/file.pdf"
  return parts.slice(-4).join('/');
}

export function DragDropOverlay({ children }: Props) {
  const [isDragging, setIsDragging] = useState(false);
  const [processingStatus, setProcessingStatus] = useState<string | null>(null);
  const dragCounter = useRef(0);
  const pendingTxtPath = useRef<string | null>(null);
  const pendingFileName = useRef<string | null>(null);
  const lastPipelineInput = useRef<string | null>(null);
  const loadXlsx = useXlsxStore((s) => s.loadFile);
  const extractText = usePipelineStore((s) => s.extractText);
  const runPipeline = usePipelineStore((s) => s.run);
  const extraction = usePipelineStore((s) => s.extraction);
  const extractionResult = usePipelineStore((s) => s.extractionResult);
  const extractionError = usePipelineStore((s) => s.extractionError);
  const pipelineState = usePipelineStore((s) => s.state);
  const pipelineReport = usePipelineStore((s) => s.report);
  const pipelineError = usePipelineStore((s) => s.error);
  const refresh = useFileStore((s) => s.refresh);
  const addSystemMessage = useChatStore((s) => s.addSystemMessage);
  const sendMessage = useChatStore((s) => s.sendMessage);

  // Auto-run pipeline when extraction completes
  useEffect(() => {
    if (extraction === 'success' && pendingTxtPath.current) {
      const txtPath = pendingTxtPath.current;
      const fileName = pendingFileName.current || 'file';
      pendingTxtPath.current = null;
      pendingFileName.current = null;
      console.log('DragDrop: extraction complete, auto-running pipeline on:', txtPath);
      addSystemMessage(`Text extraction complete for ${fileName}. Running CARICOM invoice pipeline on extracted text...`);
      setProcessingStatus('Running pipeline...');
      lastPipelineInput.current = txtPath;
      runPipeline(txtPath);
    } else if (extraction === 'error' && pendingTxtPath.current) {
      const fileName = pendingFileName.current || 'file';
      pendingTxtPath.current = null;
      pendingFileName.current = null;
      const errMsg = extractionError || 'Unknown extraction error';
      console.error('DragDrop: extraction failed:', errMsg);
      addSystemMessage(`Text extraction failed for ${fileName}: ${errMsg}`);
      setProcessingStatus(`Extraction failed: ${errMsg}`);
      setTimeout(() => setProcessingStatus(null), 8000);
    }
  }, [extraction, extractionResult, extractionError, runPipeline, addSystemMessage]);

  // Show status when pipeline completes (XLSX loading handled by App.tsx onPipelineComplete)
  useEffect(() => {
    if (pipelineState === 'success' && pipelineReport?.output) {
      const xlsxPath = pipelineReport.output;
      console.log('DragDrop: pipeline complete:', xlsxPath);
      setProcessingStatus('Done! Loading spreadsheet...');
      setTimeout(() => setProcessingStatus(null), 3000);
    } else if (pipelineState === 'error' && processingStatus) {
      const errMsg = pipelineError || 'Unknown pipeline error';
      const inputFile = lastPipelineInput.current;
      lastPipelineInput.current = null;
      console.error('DragDrop: pipeline failed:', errMsg);
      addSystemMessage(`Pipeline failed: ${errMsg}`);
      setProcessingStatus(`Pipeline failed — asking LLM to diagnose...`);
      setTimeout(() => setProcessingStatus(null), 8000);

      // Auto-trigger LLM to diagnose and fix
      setTimeout(() => {
        const inputInfo = inputFile ? `\nInput file: ${shortPath(inputFile)}` : '';
        sendMessage(
          `The pipeline just failed with this error:\n\`\`\`\n${errMsg}\n\`\`\`${inputInfo}\n\nPlease diagnose the issue. Read the input text file to understand its format, then read the relevant pipeline script (e.g. pipeline/text_parser.py) to find why parsing failed. Fix the script and re-run the pipeline.`
        );
      }, 500);
    }
  }, [pipelineState, pipelineReport, pipelineError, addSystemMessage, sendMessage]);

  const handleDrop = useCallback(
    async (e: DragEvent) => {
      e.preventDefault();
      e.stopPropagation();
      dragCounter.current = 0;
      setIsDragging(false);

      if (!window.api) {
        console.error('DragDrop: window.api not available');
        return;
      }

      const files = e.dataTransfer ? Array.from(e.dataTransfer.files) : [];
      console.log('DragDrop: dropped', files.length, 'files');

      if (files.length === 0) {
        console.warn('DragDrop: no files in drop event');
        return;
      }

      for (const file of files) {
        const ext = file.name.split('.').pop()?.toLowerCase();
        const filePath = window.api.getFilePath(file);

        console.log('DragDrop: file:', file.name, 'path:', filePath, 'ext:', ext, 'size:', file.size);

        if (!filePath) {
          console.warn('DragDrop: file has no path — getFilePath returned empty:', file.name);
          continue;
        }

        try {
          if (ext === 'pdf') {
            await addSystemMessage(`File dropped: ${file.name} (${formatSize(file.size)}, PDF). Copying to workspace and starting auto-conversion...`);
            setProcessingStatus(`Copying ${file.name} to workspace...`);
            const copied = await window.api.copyToWorkspace(filePath);
            console.log('DragDrop: copied PDF to workspace:', copied);
            await addSystemMessage(`Copied to ${shortPath(copied)}. Extracting text from PDF...`);

            // Auto-extract text from PDF
            const txtPath = copied.replace(/\.pdf$/i, '.txt');
            console.log('DragDrop: extracting text to:', txtPath);
            setProcessingStatus(`Extracting text from ${file.name}...`);
            pendingTxtPath.current = txtPath;
            pendingFileName.current = file.name;
            extractText(copied, txtPath);
          } else if (ext === 'txt') {
            await addSystemMessage(`File dropped: ${file.name} (${formatSize(file.size)}, TXT). Running pipeline...`);
            setProcessingStatus(`Processing ${file.name}...`);
            const copied = await window.api.copyToWorkspace(filePath);
            console.log('DragDrop: copied TXT, auto-running pipeline:', copied);
            setProcessingStatus(`Running pipeline on ${file.name}...`);
            lastPipelineInput.current = copied;
            runPipeline(copied);
          } else if (ext === 'xlsx' || ext === 'xls') {
            await addSystemMessage(`File dropped: ${file.name} (${formatSize(file.size)}, XLSX). Loading spreadsheet preview...`);
            setProcessingStatus(`Loading ${file.name}...`);
            const copied = await window.api.copyToWorkspace(filePath);
            console.log('DragDrop: copied XLSX to workspace:', copied);
            await loadXlsx(copied);
            await addSystemMessage(`Spreadsheet loaded: ${shortPath(copied)}`);
            setProcessingStatus(null);
          } else if (ext === 'json' || ext === 'yaml' || ext === 'yml') {
            const copied = await window.api.copyToWorkspace(filePath);
            console.log('DragDrop: copied config to workspace:', copied);
            let convId = useChatStore.getState().activeConversationId;
            if (!convId) {
              await useChatStore.getState().createConversation();
              convId = useChatStore.getState().activeConversationId;
            }
            if (convId) {
              useChatStore.getState().sendMessage(`Review this file: ${file.name}`, [copied]);
            }
          } else if (ext === 'xml') {
            // ASYCUDA XML file - copy to asycuda folder and auto-import
            await addSystemMessage(`File dropped: ${file.name} (${formatSize(file.size)}, XML). Copying to workspace and importing ASYCUDA data...`);
            setProcessingStatus(`Copying ${file.name} to asycuda folder...`);
            const copied = await window.api.copyToWorkspace(filePath, 'asycuda');
            console.log('DragDrop: copied XML to asycuda folder:', copied);

            // Auto-import the ASYCUDA classifications
            setProcessingStatus(`Importing ASYCUDA data from ${file.name}...`);
            try {
              const result = await window.api.importAsycudaXml(copied);
              if (result.success) {
                let msg = `Imported ${result.imported || 0} classifications from ${file.name}`;
                if (result.corrected && result.corrected > 0) {
                  msg += `. ${result.corrected} classification corrections applied.`;
                }
                await addSystemMessage(msg);
                setProcessingStatus(null);
              } else {
                await addSystemMessage(`ASYCUDA import failed for ${file.name}: ${result.error || 'Unknown error'}`);
                setProcessingStatus(`Import failed: ${result.error}`);
                setTimeout(() => setProcessingStatus(null), 5000);
              }
            } catch (importErr) {
              console.error('DragDrop: ASYCUDA import error:', importErr);
              await addSystemMessage(`Error importing ASYCUDA data: ${(importErr as Error).message}`);
              setProcessingStatus(null);
            }
          } else {
            console.warn('DragDrop: unsupported file type:', ext, 'for file:', file.name);
            await addSystemMessage(`Unsupported file type dropped: ${file.name} (.${ext})`);
          }
        } catch (err) {
          console.error('DragDrop: error processing file:', file.name, err);
          await addSystemMessage(`Error processing ${file.name}: ${(err as Error).message}`);
          setProcessingStatus(`Error: ${(err as Error).message}`);
          setTimeout(() => setProcessingStatus(null), 5000);
        }
      }

      refresh();
    },
    [loadXlsx, extractText, runPipeline, refresh, addSystemMessage]
  );

  // Use document-level listeners for reliable drag detection
  useEffect(() => {
    const onDragEnter = (e: DragEvent) => {
      e.preventDefault();
      dragCounter.current++;
      if (dragCounter.current === 1) {
        setIsDragging(true);
      }
    };

    const onDragOver = (e: DragEvent) => {
      e.preventDefault();
    };

    const onDragLeave = (e: DragEvent) => {
      e.preventDefault();
      dragCounter.current--;
      if (dragCounter.current <= 0) {
        dragCounter.current = 0;
        setIsDragging(false);
      }
    };

    const onDrop = (e: DragEvent) => {
      handleDrop(e);
    };

    document.addEventListener('dragenter', onDragEnter);
    document.addEventListener('dragover', onDragOver);
    document.addEventListener('dragleave', onDragLeave);
    document.addEventListener('drop', onDrop);

    return () => {
      document.removeEventListener('dragenter', onDragEnter);
      document.removeEventListener('dragover', onDragOver);
      document.removeEventListener('dragleave', onDragLeave);
      document.removeEventListener('drop', onDrop);
    };
  }, [handleDrop]);

  return (
    <div className="relative flex-1 flex flex-col overflow-hidden">
      {children}

      {/* Processing status bar */}
      {processingStatus && (
        <div className="absolute bottom-0 left-0 right-0 z-40 bg-accent/90 text-white text-sm px-4 py-2 flex items-center gap-2">
          <div className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
          {processingStatus}
        </div>
      )}

      {/* Drop overlay */}
      {isDragging && (
        <div className="absolute inset-0 z-50 bg-surface-900/80 backdrop-blur-sm flex items-center justify-center pointer-events-none">
          <div className="text-center">
            <Upload size={48} className="text-accent mx-auto mb-4 animate-bounce" />
            <p className="text-lg font-medium text-surface-100 mb-2">Drop files to process</p>
            <div className="flex flex-wrap justify-center gap-6 text-sm text-surface-400">
              <div className="flex items-center gap-2">
                <FileText size={16} className="text-red-400" />
                <span>PDF - Auto-convert to XLSX</span>
              </div>
              <div className="flex items-center gap-2">
                <Table2 size={16} className="text-green-400" />
                <span>XLSX - Preview spreadsheet</span>
              </div>
              <div className="flex items-center gap-2">
                <FileCode size={16} className="text-blue-400" />
                <span>XML - Import ASYCUDA data</span>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
