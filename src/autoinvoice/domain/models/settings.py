"""Application settings - SSOT for all configuration values.

Every setting has exactly one definition here. No other module defines defaults.
Settings are loaded via pydantic-settings: env vars > settings.json > defaults here.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class AppSettings:
    """All application settings. Single source of truth.

    In production, use pydantic-settings to load from env/json.
    For the domain layer, this is a plain frozen dataclass to avoid
    coupling the domain to pydantic.
    """

    # LLM
    llm_api_key: str = ""
    llm_base_url: str = "https://api.z.ai/api/anthropic"
    llm_model: str = "glm-5"

    # Email
    smtp_host: str = ""
    smtp_port: int = 465
    email_sender: str = ""
    email_sender_name: str = ""
    email_password: str = ""
    email_recipient: str = ""

    # Thresholds
    variance_threshold: float = 0.50
    max_llm_variance_attempts: int = 2
    max_invoice_text_length: int = 10000
    max_invoice_pages: int = 5

    # Paths
    base_dir: str = ""
    workspace_path: str = ""

    # UI
    theme: str = "dark"
