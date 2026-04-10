"""In-memory fake for ClassificationStorePort."""

from __future__ import annotations

from typing import Any


class FakeClassificationStore:
    """Fake classification store backed by in-memory dicts."""

    def __init__(self) -> None:
        self._classifications: dict[str, dict[str, Any]] = {}
        self._corrections: dict[str, list[dict[str, Any]]] = {}

    def get_classification(self, sku: str) -> dict[str, Any] | None:
        """Retrieve the stored classification for an SKU."""
        return self._classifications.get(sku)

    def save_classification(self, sku: str, code: str, source: str) -> None:
        """Persist a classification for an SKU in the in-memory dict."""
        self._classifications[sku] = {"code": code, "source": source}

    def get_corrections(self, sku: str) -> list[dict[str, Any]]:
        """Retrieve correction history for an SKU."""
        return self._corrections.get(sku, [])

    def save_correction(self, sku: str, old_code: str, new_code: str) -> None:
        """Record a tariff code correction for an SKU."""
        self._corrections.setdefault(sku, []).append(
            {
                "old_code": old_code,
                "new_code": new_code,
            }
        )

    def import_asycuda(self, classifications: list[dict[str, Any]]) -> dict[str, int]:
        """Bulk import classifications, tracking imported and skipped counts."""
        imported = 0
        skipped = 0
        for record in classifications:
            sku = record.get("sku", "")
            code = record.get("code", "")
            if not sku or not code:
                skipped += 1
                continue
            self._classifications[sku] = {"code": code, "source": "asycuda"}
            imported += 1
        return {"imported": imported, "skipped": skipped}
