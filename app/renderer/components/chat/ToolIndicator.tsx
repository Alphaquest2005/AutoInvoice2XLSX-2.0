import React from 'react';
import { Wrench, Loader2 } from 'lucide-react';
import { useChatStore } from '../../stores/chatStore';

const toolLabels: Record<string, string> = {
  run_pipeline: 'Running pipeline...',
  read_file: 'Reading file...',
  write_file: 'Writing file...',
  edit_file: 'Editing file...',
  list_files: 'Listing files...',
  query_rules: 'Searching rules...',
  update_rules: 'Updating rules...',
  validate_xlsx: 'Validating spreadsheet...',
  verify_line_count: 'Verifying line count...',
  reclassify_items: 'Reclassifying items...',
  lookup_tariff: 'Searching tariff database...',
  web_search: 'Searching the web...',
  add_cet_entry: 'Adding CET entry...',
  cet_stats: 'Getting CET stats...',
  search_chat_history: 'Searching chat history...',
  extract_with_ocr: 'Extracting with OCR...',
  compare_ocr_results: 'Comparing OCR results...',
};

export function ToolIndicator() {
  const toolUse = useChatStore((s) => s.streamingToolUse);

  if (!toolUse) return null;

  const label = toolLabels[toolUse.name] || `Running ${toolUse.name}...`;

  return (
    <div className="px-3 py-1.5 flex items-center gap-2 bg-surface-800 border-t border-surface-700">
      <Loader2 size={12} className="animate-spin text-accent" />
      <div className="flex items-center gap-1.5 text-xs text-surface-400">
        <Wrench size={11} />
        <span className="tool-pulse">{label}</span>
      </div>
    </div>
  );
}
