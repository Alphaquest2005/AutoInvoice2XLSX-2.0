"""TDD tests for settings domain model."""

from __future__ import annotations

import pytest

from autoinvoice.domain.models.settings import AppSettings


class TestAppSettings:
    def test_default_values(self) -> None:
        s = AppSettings()
        assert s.llm_api_key == ""
        assert s.llm_base_url == "https://api.z.ai/api/anthropic"
        assert s.llm_model == "glm-5"
        assert s.smtp_host == ""
        assert s.smtp_port == 465
        assert s.variance_threshold == 0.50
        assert s.max_llm_variance_attempts == 2
        assert s.max_invoice_text_length == 10000
        assert s.max_invoice_pages == 5
        assert s.theme == "dark"

    def test_frozen_immutability(self) -> None:
        s = AppSettings()
        with pytest.raises(AttributeError):
            s.llm_model = "other"  # type: ignore[misc]

    def test_custom_values(self) -> None:
        s = AppSettings(
            llm_api_key="test-key",
            llm_model="custom-model",
            variance_threshold=1.0,
            workspace_path="/tmp/ws",
        )
        assert s.llm_api_key == "test-key"
        assert s.llm_model == "custom-model"
        assert s.variance_threshold == 1.0
        assert s.workspace_path == "/tmp/ws"

    def test_equality_by_value(self) -> None:
        s1 = AppSettings(llm_model="a")
        s2 = AppSettings(llm_model="a")
        assert s1 == s2

    def test_different_values_not_equal(self) -> None:
        s1 = AppSettings(llm_model="a")
        s2 = AppSettings(llm_model="b")
        assert s1 != s2
