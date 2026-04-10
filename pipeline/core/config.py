"""
Pipeline configuration - delegates to AppSettings SSOT.

SSOT for all defaults lives in: src/autoinvoice/domain/models/settings.py
This module loads runtime values from data/settings.json (shared with TypeScript).
NO hardcoded credentials or secret fallbacks.
"""

import json
import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class PipelineConfig:
    """Pipeline configuration. Defaults MUST match AppSettings SSOT."""

    # ─── LLM Settings (defaults from AppSettings SSOT) ────
    llm_api_key: str = ""
    llm_base_url: str = "https://api.z.ai/api/anthropic"
    llm_model: str = "glm-5"  # SSOT: src/autoinvoice/domain/models/settings.py

    # ─── Email Settings ─────────────────────────────────────
    smtp_host: str = "mail.auto-brokerage.com"
    smtp_port: int = 465
    email_sender: str = "documents.websource@auto-brokerage.com"
    email_sender_name: str = "Web Source-AutoBot"
    email_password: str = ""
    email_recipient: str = "shipments.websource@auto-brokerage.com"

    # ─── Thresholds ─────────────────────────────────────────
    variance_threshold: float = 0.50
    max_llm_variance_attempts: int = 2
    max_invoice_text_length: int = 10000
    max_invoice_pages: int = 5

    # ─── XLSX Column Mapping (1-indexed) ────────────────────
    col_quantity: int = 11
    col_description: int = 12
    col_unit_cost: int = 15
    col_total_cost: int = 16

    # ─── Paths ──────────────────────────────────────────────
    base_dir: str = ""
    formats_dir: str = ""
    auto_formats_dir: str = ""

    def __post_init__(self) -> None:
        if not self.base_dir:
            self.base_dir = os.path.abspath(
                os.path.join(os.path.dirname(__file__), '..', '..')
            )
        if not self.formats_dir:
            self.formats_dir = os.path.join(self.base_dir, 'config', 'formats')
        if not self.auto_formats_dir:
            self.auto_formats_dir = os.path.join(self.formats_dir, '_auto')


def load_config(base_dir: str = None) -> PipelineConfig:
    """Load config from data/settings.json with env-var overrides.

    Priority: env vars > data/settings.json > defaults (from AppSettings SSOT)
    """
    config = PipelineConfig()
    if base_dir:
        config.base_dir = base_dir
        config.formats_dir = os.path.join(base_dir, 'config', 'formats')
        config.auto_formats_dir = os.path.join(config.formats_dir, '_auto')

    # Load from settings.json (shared with TypeScript app/main/utils/settings.ts)
    for settings_path in [
        os.path.join(config.base_dir, 'data', 'settings.json'),
        os.path.join(config.base_dir, 'app', 'settings.json'),
        os.path.join(config.base_dir, 'settings.json'),
    ]:
        if os.path.exists(settings_path):
            try:
                with open(settings_path, 'r') as f:
                    settings = json.load(f)

                config.llm_api_key = settings.get('apiKey', config.llm_api_key)
                config.llm_base_url = settings.get('llmBaseUrl', config.llm_base_url) or settings.get('baseUrl', config.llm_base_url)
                config.llm_model = settings.get('llmModel', config.llm_model) or settings.get('model', config.llm_model)
                config.email_password = settings.get('emailPassword', config.email_password)
                break
            except (json.JSONDecodeError, IOError):
                pass

    # Environment variable overrides (no hardcoded fallbacks)
    config.llm_api_key = os.environ.get('LLM_API_KEY', config.llm_api_key)
    config.llm_base_url = os.environ.get('LLM_BASE_URL', config.llm_base_url)
    config.llm_model = os.environ.get('LLM_MODEL', config.llm_model)

    return config


# Module-level singleton
_config: Optional[PipelineConfig] = None


def get_config(base_dir: str = None) -> PipelineConfig:
    """Get or create the shared config instance."""
    global _config
    if _config is None or (base_dir and base_dir != _config.base_dir):
        _config = load_config(base_dir)
    return _config


def reset_config() -> None:
    """Reset the singleton (for testing)."""
    global _config
    _config = None
