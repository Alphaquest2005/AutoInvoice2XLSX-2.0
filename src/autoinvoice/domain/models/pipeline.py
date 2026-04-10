"""Pipeline domain models - execution state and reporting."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class PipelineStatus(StrEnum):
    """Pipeline execution status."""

    IDLE = "idle"
    RUNNING = "running"
    SUCCESS = "success"
    ERROR = "error"


@dataclass(frozen=True)
class StageResult:
    """Result of executing a single pipeline stage."""

    name: str
    status: str  # 'success', 'error', 'skipped'
    error: str | None = None
    duration_ms: int = 0


@dataclass(frozen=True)
class PipelineReport:
    """Full pipeline execution report."""

    stages: tuple[StageResult, ...]
    status: PipelineStatus
    errors: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    @property
    def total_duration_ms(self) -> int:
        return sum(s.duration_ms for s in self.stages)
