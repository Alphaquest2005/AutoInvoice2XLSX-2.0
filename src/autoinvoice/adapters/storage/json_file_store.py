"""JSON file-backed classification store adapter."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


class JsonClassificationStore:
    """Implements ``ClassificationStorePort`` using a JSON file on disk.

    Parameters
    ----------
    base_dir:
        Root directory.  Data is stored at
        ``{base_dir}/data/assessed_classifications.json``.
    """

    def __init__(self, base_dir: str) -> None:
        self._base_dir = base_dir
        self._data_dir = os.path.join(base_dir, "data")
        self._classifications_path = os.path.join(self._data_dir, "assessed_classifications.json")

    # -- private helpers ----------------------------------------------------

    def _ensure_data_dir(self) -> None:
        Path(self._data_dir).mkdir(parents=True, exist_ok=True)

    def _load(self) -> dict[str, Any]:
        """Load the entire store file, returning an empty structure on miss."""
        if not Path(self._classifications_path).exists():
            return {"classifications": {}, "corrections": {}}
        try:
            with open(self._classifications_path, encoding="utf-8") as fh:
                data = json.load(fh)
        except (json.JSONDecodeError, OSError):
            return {"classifications": {}, "corrections": {}}
        # Ensure expected top-level keys.
        data.setdefault("classifications", {})
        data.setdefault("corrections", {})
        return dict(data)

    def _save(self, data: dict[str, Any]) -> None:
        self._ensure_data_dir()
        with open(self._classifications_path, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, ensure_ascii=False)

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(UTC).isoformat()

    # -- ClassificationStorePort --------------------------------------------

    def get_classification(self, sku: str) -> dict[str, Any] | None:
        """Retrieve the stored classification for *sku*."""
        data = self._load()
        result: dict[str, Any] | None = data["classifications"].get(sku)
        return result

    def save_classification(self, sku: str, code: str, source: str) -> None:
        """Persist a classification for *sku*."""
        data = self._load()
        data["classifications"][sku] = {
            "code": code,
            "source": source,
            "updated_at": self._now_iso(),
        }
        self._save(data)

    def get_corrections(self, sku: str) -> list[dict[str, Any]]:
        """Retrieve correction history for *sku*."""
        data = self._load()
        result: list[dict[str, Any]] = data["corrections"].get(sku, [])
        return result

    def save_correction(self, sku: str, old_code: str, new_code: str) -> None:
        """Record a tariff code correction for *sku*."""
        data = self._load()
        corrections_list: list[dict[str, Any]] = data["corrections"].setdefault(sku, [])
        corrections_list.append(
            {
                "old_code": old_code,
                "new_code": new_code,
                "corrected_at": self._now_iso(),
            }
        )
        self._save(data)

    def import_asycuda(self, classifications: list[dict[str, Any]]) -> dict[str, int]:
        """Bulk import ASYCUDA classification records.

        Returns a summary dict with ``imported``, ``updated``, and ``skipped``
        counts.
        """
        data = self._load()
        imported = 0
        updated = 0
        skipped = 0
        for record in classifications:
            sku = record.get("sku")
            code = record.get("hs_code") or record.get("code")
            if not sku or not code:
                skipped += 1
                continue
            source = record.get("source", "asycuda")
            existing = data["classifications"].get(sku)
            if existing is None:
                imported += 1
            else:
                updated += 1
            data["classifications"][sku] = {
                "code": code,
                "source": source,
                "updated_at": self._now_iso(),
            }
        self._save(data)
        return {"imported": imported, "updated": updated, "skipped": skipped}
