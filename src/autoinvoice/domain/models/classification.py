"""Classification domain models - SSOT for tariff code and classification data."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from autoinvoice.domain.models.invoice import InvoiceItem


@dataclass(frozen=True)
class TariffCode:
    """8-digit CARICOM CET tariff code."""

    code: str

    def __post_init__(self) -> None:
        if not self.code.isdigit() or len(self.code) != 8:
            raise ValueError(f"Invalid tariff code: {self.code!r}")

    @property
    def chapter(self) -> str:
        """First 2 digits - HS chapter."""
        return self.code[:2]

    @property
    def heading(self) -> str:
        """First 4 digits - HS heading."""
        return self.code[:4]

    @property
    def subheading(self) -> str:
        """First 6 digits - HS subheading."""
        return self.code[:6]


@dataclass(frozen=True)
class Classification:
    """Result of classifying a single item."""

    item: InvoiceItem
    tariff_code: TariffCode
    confidence: float
    source: str  # 'rules', 'asycuda', 'llm', 'web'
    category: str = "PRODUCTS"


@dataclass(frozen=True)
class ClassificationResult:
    """Complete classification results for an invoice."""

    classifications: tuple[Classification, ...]
    unclassified_count: int = 0
    low_confidence_count: int = 0
