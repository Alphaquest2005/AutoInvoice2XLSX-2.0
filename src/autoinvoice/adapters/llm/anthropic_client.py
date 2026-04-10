"""Anthropic LLM client adapter."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:
    from collections.abc import Callable


class AnthropicLlmClient:
    """LLM client adapter using Anthropic API (or compatible proxy like Z.AI)."""

    def __init__(self, api_key: str, base_url: str, model: str) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._model = model

    def complete(
        self,
        system: str,
        messages: list[dict[str, str]],
        tools: list[dict[str, Any]] | None = None,
    ) -> str:
        """Send a completion request and return the full response text."""
        headers = {
            "x-api-key": self._api_key,
            "content-type": "application/json",
            "anthropic-version": "2023-06-01",
        }
        payload: dict[str, Any] = {
            "model": self._model,
            "max_tokens": 4096,
            "system": system,
            "messages": messages,
        }
        if tools:
            payload["tools"] = tools

        response = httpx.post(
            f"{self._base_url}/v1/messages",
            headers=headers,
            json=payload,
            timeout=120.0,
        )
        response.raise_for_status()
        data = response.json()

        # Extract text from content blocks
        text_parts = [b["text"] for b in data.get("content", []) if b.get("type") == "text"]
        return "".join(text_parts)

    def stream(
        self,
        system: str,
        messages: list[dict[str, str]],
        on_text: Callable[[str], None],
        on_error: Callable[[str], None],
    ) -> None:
        """Stream a completion (simplified: calls complete and passes result)."""
        try:
            result = self.complete(system, messages)
            on_text(result)
        except Exception as e:
            on_error(str(e))
