"""In-memory fake for LlmClientPort."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


class FakeLlmClient:
    """Fake LLM client that returns canned responses in order."""

    def __init__(self, responses: list[str] | None = None) -> None:
        self._responses = list(responses) if responses else []

    def complete(
        self,
        system: str,
        messages: list[dict[str, str]],
        tools: list[dict[str, Any]] | None = None,
    ) -> str:
        """Pop and return the next canned response."""
        if not self._responses:
            raise RuntimeError("FakeLlmClient: no more canned responses")
        return self._responses.pop(0)

    def stream(
        self,
        system: str,
        messages: list[dict[str, str]],
        on_text: Callable[[str], None],
        on_error: Callable[[str], None],
    ) -> None:
        """Call on_text with the next canned response as a single chunk."""
        if not self._responses:
            on_error("FakeLlmClient: no more canned responses")
            return
        on_text(self._responses.pop(0))
