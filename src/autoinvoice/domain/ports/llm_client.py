"""Port for LLM interaction."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from collections.abc import Callable


class LlmClientPort(Protocol):
    """Interface for communicating with a large language model."""

    def complete(
        self,
        system: str,
        messages: list[dict[str, str]],
        tools: list[dict[str, Any]] | None = None,
    ) -> str:
        """Send a completion request and return the full response.

        Args:
            system: System prompt.
            messages: Conversation messages.
            tools: Optional tool definitions for function calling.

        Returns:
            Model response text.
        """
        ...

    def stream(
        self,
        system: str,
        messages: list[dict[str, str]],
        on_text: Callable[[str], None],
        on_error: Callable[[str], None],
    ) -> None:
        """Stream a completion, invoking callbacks as tokens arrive.

        Args:
            system: System prompt.
            messages: Conversation messages.
            on_text: Called with each text chunk.
            on_error: Called if an error occurs during streaming.
        """
        ...
