"""SQLite-backed code repository adapter."""

from __future__ import annotations

import sqlite3
from typing import Any

_SCHEMA = """\
CREATE TABLE IF NOT EXISTS cet_codes (
    hs_code TEXT PRIMARY KEY,
    description TEXT NOT NULL,
    duty_rate TEXT,
    chapter INTEGER,
    heading TEXT,
    section TEXT,
    unit TEXT,
    notes TEXT,
    source TEXT DEFAULT 'rules',
    enabled INTEGER DEFAULT 1,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cet_aliases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    hs_code TEXT NOT NULL,
    alias TEXT NOT NULL,
    source TEXT DEFAULT 'rules',
    enabled INTEGER DEFAULT 1,
    FOREIGN KEY (hs_code) REFERENCES cet_codes(hs_code)
);

CREATE TABLE IF NOT EXISTS asycuda_classifications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sku TEXT NOT NULL,
    hs_code TEXT NOT NULL,
    description TEXT,
    commercial_description TEXT,
    country_of_origin TEXT,
    import_id INTEGER,
    source_file TEXT,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cet_chapter ON cet_codes(chapter);
CREATE INDEX IF NOT EXISTS idx_cet_heading ON cet_codes(heading);
CREATE INDEX IF NOT EXISTS idx_cet_enabled ON cet_codes(enabled);
CREATE INDEX IF NOT EXISTS idx_aliases_code ON cet_aliases(hs_code);
CREATE INDEX IF NOT EXISTS idx_aliases_text ON cet_aliases(alias);
CREATE INDEX IF NOT EXISTS idx_asycuda_sku ON asycuda_classifications(sku);
CREATE INDEX IF NOT EXISTS idx_asycuda_hs ON asycuda_classifications(hs_code);
"""


class SqliteCodeRepository:
    """Implements ``CodeRepositoryPort`` backed by a SQLite database.

    Parameters
    ----------
    db_path:
        Path to the SQLite database file.
    invalid_codes:
        Mapping of invalid tariff codes to correction records.  Each value is
        a dict with at least a ``"correct_code"`` key.
    valid_codes:
        Optional pre-loaded set of known-valid codes (checked before hitting
        the database).
    """

    def __init__(
        self,
        db_path: str,
        invalid_codes: dict[str, str] | None = None,
        valid_codes: set[str] | None = None,
    ) -> None:
        self._db_path = db_path
        self._invalid_codes: dict[str, str] = invalid_codes or {}
        self._valid_codes: set[str] = valid_codes or set()
        self._ensure_schema()

    # -- private helpers ----------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        conn = self._connect()
        try:
            conn.executescript(_SCHEMA)
            conn.commit()
        finally:
            conn.close()

    # -- CodeRepositoryPort -------------------------------------------------

    def is_valid_code(self, code: str) -> bool:
        """Check whether *code* exists in the in-memory set or the database."""
        if code in self._valid_codes:
            return True
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT 1 FROM cet_codes WHERE hs_code = ? AND enabled = 1",
                (code,),
            ).fetchone()
            return row is not None
        finally:
            conn.close()

    def get_correction(self, invalid_code: str) -> str | None:
        """Return the corrected code for a known invalid code, or ``None``."""
        entry = self._invalid_codes.get(invalid_code)
        if entry is None:
            return None
        # Support both flat string values and dict values with 'correct_code'.
        if isinstance(entry, dict):
            return entry.get("correct_code")
        return entry

    def lookup_by_description(self, description: str) -> list[tuple[str, float]]:
        """Search ``cet_codes`` for rows whose description contains *description*.

        Returns a list of ``(hs_code, score)`` tuples.  The score is a simple
        containment heuristic (ratio of query length to description length).
        """
        if not description:
            return []
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT hs_code, description FROM cet_codes "
                "WHERE enabled = 1 AND description LIKE ? "
                "ORDER BY length(description)",
                (f"%{description}%",),
            ).fetchall()
            results: list[tuple[str, float]] = []
            for row in rows:
                desc_len = len(row["description"]) or 1
                score = round(len(description) / desc_len, 4)
                results.append((row["hs_code"], score))
            return results
        finally:
            conn.close()

    def get_assessed_classification(self, sku: str) -> dict[str, Any] | None:
        """Return the most recent ASYCUDA classification for *sku*, or ``None``."""
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT * FROM asycuda_classifications "
                "WHERE sku = ? ORDER BY created_at DESC LIMIT 1",
                (sku,),
            ).fetchone()
            if row is None:
                return None
            return dict(row)
        finally:
            conn.close()
