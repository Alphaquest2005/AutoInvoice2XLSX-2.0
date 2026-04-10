"""Tests for AnthropicLlmClient adapter."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from autoinvoice.adapters.llm.anthropic_client import AnthropicLlmClient


@pytest.fixture()
def client() -> AnthropicLlmClient:
    return AnthropicLlmClient(
        api_key="test-key",
        base_url="https://api.example.com",
        model="test-model",
    )


def _mock_response(
    status_code: int = 200,
    json_data: dict[str, Any] | None = None,
) -> MagicMock:
    """Build a fake httpx.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        import httpx

        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "error", request=MagicMock(), response=resp
        )
    return resp


class TestComplete:
    """Tests for the complete() method."""

    @patch("autoinvoice.adapters.llm.anthropic_client.httpx")
    def test_sends_correct_payload(self, mock_httpx: MagicMock, client: AnthropicLlmClient) -> None:
        mock_httpx.post.return_value = _mock_response(
            json_data={"content": [{"type": "text", "text": "ok"}]}
        )

        client.complete(
            system="sys prompt",
            messages=[{"role": "user", "content": "hello"}],
            tools=[{"name": "my_tool"}],
        )

        mock_httpx.post.assert_called_once()
        call_kwargs = mock_httpx.post.call_args
        payload = call_kwargs.kwargs["json"]

        assert payload["model"] == "test-model"
        assert payload["max_tokens"] == 4096
        assert payload["system"] == "sys prompt"
        assert payload["messages"] == [{"role": "user", "content": "hello"}]
        assert payload["tools"] == [{"name": "my_tool"}]

        headers = call_kwargs.kwargs["headers"]
        assert headers["x-api-key"] == "test-key"
        assert headers["anthropic-version"] == "2023-06-01"

    @patch("autoinvoice.adapters.llm.anthropic_client.httpx")
    def test_extracts_text_from_response(
        self, mock_httpx: MagicMock, client: AnthropicLlmClient
    ) -> None:
        mock_httpx.post.return_value = _mock_response(
            json_data={
                "content": [
                    {"type": "text", "text": "Hello "},
                    {"type": "tool_use", "id": "t1"},
                    {"type": "text", "text": "World"},
                ]
            }
        )

        result = client.complete("sys", [{"role": "user", "content": "hi"}])

        assert result == "Hello World"

    @patch("autoinvoice.adapters.llm.anthropic_client.httpx")
    def test_handles_error_response(
        self, mock_httpx: MagicMock, client: AnthropicLlmClient
    ) -> None:
        import httpx

        mock_httpx.post.return_value = _mock_response(status_code=500)
        mock_httpx.HTTPStatusError = httpx.HTTPStatusError

        with pytest.raises(httpx.HTTPStatusError):
            client.complete("sys", [{"role": "user", "content": "hi"}])

    @patch("autoinvoice.adapters.llm.anthropic_client.httpx")
    def test_no_tools_omits_tools_key(
        self, mock_httpx: MagicMock, client: AnthropicLlmClient
    ) -> None:
        mock_httpx.post.return_value = _mock_response(
            json_data={"content": [{"type": "text", "text": "ok"}]}
        )

        client.complete("sys", [{"role": "user", "content": "hi"}])

        payload = mock_httpx.post.call_args.kwargs["json"]
        assert "tools" not in payload

    @patch("autoinvoice.adapters.llm.anthropic_client.httpx")
    def test_empty_content_returns_empty_string(
        self, mock_httpx: MagicMock, client: AnthropicLlmClient
    ) -> None:
        mock_httpx.post.return_value = _mock_response(json_data={"content": []})

        result = client.complete("sys", [{"role": "user", "content": "hi"}])
        assert result == ""


class TestStream:
    """Tests for the stream() method."""

    @patch("autoinvoice.adapters.llm.anthropic_client.httpx")
    def test_calls_on_text_with_result(
        self, mock_httpx: MagicMock, client: AnthropicLlmClient
    ) -> None:
        mock_httpx.post.return_value = _mock_response(
            json_data={"content": [{"type": "text", "text": "streamed result"}]}
        )
        on_text = MagicMock()
        on_error = MagicMock()

        client.stream("sys", [{"role": "user", "content": "hi"}], on_text, on_error)

        on_text.assert_called_once_with("streamed result")
        on_error.assert_not_called()

    @patch("autoinvoice.adapters.llm.anthropic_client.httpx")
    def test_calls_on_error_on_failure(
        self, mock_httpx: MagicMock, client: AnthropicLlmClient
    ) -> None:
        import httpx

        mock_httpx.post.return_value = _mock_response(status_code=500)
        mock_httpx.HTTPStatusError = httpx.HTTPStatusError

        on_text = MagicMock()
        on_error = MagicMock()

        client.stream("sys", [{"role": "user", "content": "hi"}], on_text, on_error)

        on_text.assert_not_called()
        on_error.assert_called_once()
        assert "error" in on_error.call_args[0][0].lower() or len(on_error.call_args[0][0]) > 0
