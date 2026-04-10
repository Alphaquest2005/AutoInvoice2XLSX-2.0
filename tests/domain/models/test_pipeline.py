"""TDD tests for pipeline domain models."""

from __future__ import annotations

import pytest

from autoinvoice.domain.models.pipeline import PipelineReport, PipelineStatus, StageResult


class TestPipelineStatus:
    def test_values(self) -> None:
        assert PipelineStatus.IDLE == "idle"
        assert PipelineStatus.RUNNING == "running"
        assert PipelineStatus.SUCCESS == "success"
        assert PipelineStatus.ERROR == "error"


class TestStageResult:
    def test_create_success(self) -> None:
        result = StageResult(name="extract", status="success")
        assert result.name == "extract"
        assert result.status == "success"
        assert result.error is None
        assert result.duration_ms == 0

    def test_create_error(self) -> None:
        result = StageResult(name="classify", status="error", error="No rules matched")
        assert result.error == "No rules matched"

    def test_frozen(self) -> None:
        result = StageResult(name="X", status="success")
        with pytest.raises(AttributeError):
            result.status = "error"  # type: ignore[misc]


class TestPipelineReport:
    def test_create_empty(self) -> None:
        report = PipelineReport(stages=(), status=PipelineStatus.SUCCESS)
        assert len(report.stages) == 0
        assert report.status == PipelineStatus.SUCCESS
        assert report.errors == ()
        assert report.warnings == ()

    def test_with_stages(self) -> None:
        s1 = StageResult(name="extract", status="success", duration_ms=150)
        s2 = StageResult(name="classify", status="success", duration_ms=300)
        report = PipelineReport(stages=(s1, s2), status=PipelineStatus.SUCCESS)
        assert len(report.stages) == 2
        assert report.total_duration_ms == 450
