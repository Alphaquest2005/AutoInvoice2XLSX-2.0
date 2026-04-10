"""
Pipeline state persistence and checkpoint/resume.

Saves intermediate results after each stage so retries can resume
from the last successful checkpoint instead of re-running everything.
"""

import json
import logging
import os
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class StageResult:
    """Result of a single pipeline stage."""
    name: str
    status: str  # 'success', 'error', 'skipped'
    data: Dict = field(default_factory=dict)
    error: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class PipelineState:
    """
    Full pipeline state that persists between runs.

    When a retry happens, we resume from the last successful stage
    instead of re-running the entire pipeline.
    """
    invoice_path: str = ""
    output_dir: str = ""

    # Extracted text - cached so OCR is never re-run on retry
    extracted_text: str = ""
    ocr_method: str = ""

    # Parsed data - cached so parsing is only re-run if format spec changes
    parsed_data: Optional[Dict] = None
    format_name: str = ""

    # Metadata extracted from invoice - frozen after first successful extraction
    invoice_number: Optional[str] = None
    invoice_total: Optional[float] = None
    supplier_name: Optional[str] = None

    # Classification results - frozen after first successful classification
    classified_items: Optional[List[Dict]] = None

    # Generated XLSX path
    xlsx_path: Optional[str] = None
    variance: Optional[float] = None

    # Format spec that was auto-generated (if any) - for cleanup on failure
    auto_format_spec_path: Optional[str] = None

    # Stage completion tracking
    completed_stages: List[str] = field(default_factory=list)
    stage_results: List[Dict] = field(default_factory=list)

    # Overall status
    status: str = "pending"  # pending, in_progress, success, failed
    errors: List[str] = field(default_factory=list)

    def is_stage_complete(self, stage_name: str) -> bool:
        """Check if a stage has already completed successfully."""
        return stage_name in self.completed_stages

    def mark_stage_complete(self, stage_name: str, data: Dict = None):
        """Mark a stage as successfully completed."""
        if stage_name not in self.completed_stages:
            self.completed_stages.append(stage_name)
        self.stage_results.append(asdict(StageResult(
            name=stage_name, status='success', data=data or {}
        )))

    def mark_stage_failed(self, stage_name: str, error: str):
        """Mark a stage as failed."""
        self.stage_results.append(asdict(StageResult(
            name=stage_name, status='error', error=error
        )))
        self.errors.append(f"{stage_name}: {error}")

    def save(self, path: str = None):
        """Persist state to disk."""
        if not path:
            if self.output_dir:
                path = os.path.join(self.output_dir, '_pipeline_state.json')
            else:
                return

        os.makedirs(os.path.dirname(path), exist_ok=True)

        # Convert to serializable dict (skip large text fields for file size)
        data = asdict(self)
        # Don't persist full extracted_text to state file (it's large)
        # Instead, save it separately if needed
        if len(data.get('extracted_text', '')) > 1000:
            text_path = os.path.join(os.path.dirname(path), '_extracted_text.txt')
            with open(text_path, 'w', encoding='utf-8') as f:
                f.write(data['extracted_text'])
            data['extracted_text'] = f'__file__:{text_path}'

        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, default=str)

    @classmethod
    def load(cls, path: str) -> Optional['PipelineState']:
        """Load state from disk. Returns None if no state file exists."""
        if not os.path.exists(path):
            return None

        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Restore extracted_text from separate file if needed
            ext_text = data.get('extracted_text', '')
            if isinstance(ext_text, str) and ext_text.startswith('__file__:'):
                text_path = ext_text[len('__file__:'):]
                if os.path.exists(text_path):
                    with open(text_path, 'r', encoding='utf-8') as f:
                        data['extracted_text'] = f.read()

            state = cls()
            for key, value in data.items():
                if hasattr(state, key):
                    setattr(state, key, value)
            return state

        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to load pipeline state from {path}: {e}")
            return None

    @classmethod
    def load_or_create(cls, output_dir: str, invoice_path: str) -> 'PipelineState':
        """Load existing state or create new one."""
        state_path = os.path.join(output_dir, '_pipeline_state.json')
        state = cls.load(state_path)
        if state and state.invoice_path == invoice_path:
            logger.info(f"Resuming from checkpoint: {state.completed_stages}")
            return state
        # New state
        return cls(invoice_path=invoice_path, output_dir=output_dir)
