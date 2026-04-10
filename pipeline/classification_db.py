#!/usr/bin/env python3
"""
SQLite Classifications Database — single source of truth for tariff classifications.

Replaces JSON-file-based classification storage with a proper database.
Tracks every classification with full provenance (source, confidence, timestamp),
keeps officer (XML) entries separate from pipeline (XLSX) entries, and supports
the ASYCUDA comparison workflow.

Usage:
    # As a library
    from classification_db import init_db, upsert_classification, lookup_classification

    # CLI: migrate from JSON files
    python classification_db.py --migrate [--base-dir DIR]

    # CLI: show stats
    python classification_db.py --stats [--base-dir DIR]
"""

import json
import logging
import os
import re
import sqlite3
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

DEFAULT_DB_NAME = 'classifications.db'


# ── Normalization (shared with classifier.py) ─────────────────────────────────

_NOISE_WORDS = {
    'the', 'a', 'an', 'and', 'or', 'for', 'with', 'in', 'on', 'of', 'to',
    'by', 'at', 'per', 'from', 'is', 'are', 'was', 'no', 'not',
}


def normalize_description(text: str) -> str:
    """Normalize a product description for matching.

    Must produce identical output to classifier._normalize_for_assessed().
    """
    if not text:
        return ''
    text = text.lower().strip()
    text = re.sub(r'[^\w\s]', ' ', text)
    text = re.sub(r'\b\d+\b', '', text)  # remove standalone numbers
    words = text.split()
    words = [w for w in words if w not in _NOISE_WORDS and len(w) > 1]
    return ' '.join(words).strip()


# ── Schema ────────────────────────────────────────────────────────────────────

SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- 1. classifications: append-only history of every classification
CREATE TABLE IF NOT EXISTS classifications (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    description     TEXT NOT NULL,
    description_norm TEXT NOT NULL,
    sku             TEXT,
    tariff_code     TEXT NOT NULL,
    category        TEXT DEFAULT '',
    source          TEXT NOT NULL,
    confidence      REAL NOT NULL DEFAULT 0.0,
    notes           TEXT DEFAULT '',
    unverified      INTEGER DEFAULT 1,
    assessed_count  INTEGER,
    assessed_total  INTEGER,
    assessed_sources TEXT,
    inventory_refs  TEXT,
    is_active       INTEGER DEFAULT 1,
    superseded_by   INTEGER,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now')),
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now')),
    FOREIGN KEY (superseded_by) REFERENCES classifications(id)
);

-- 2. description_lookup: fast cache — one row per normalized description
CREATE TABLE IF NOT EXISTS description_lookup (
    description_norm TEXT PRIMARY KEY,
    tariff_code     TEXT NOT NULL,
    category        TEXT DEFAULT '',
    confidence      REAL NOT NULL DEFAULT 0.0,
    source          TEXT NOT NULL,
    classification_id INTEGER NOT NULL,
    sample_desc     TEXT DEFAULT '',
    notes           TEXT DEFAULT '',
    updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now')),
    FOREIGN KEY (classification_id) REFERENCES classifications(id)
);

-- 3. shipments
CREATE TABLE IF NOT EXISTS shipments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    bl_number       TEXT NOT NULL,
    consignee       TEXT DEFAULT '',
    supplier_code   TEXT DEFAULT '',
    supplier_name   TEXT DEFAULT '',
    document_type   TEXT DEFAULT '',
    origin_country  TEXT DEFAULT '',
    status          TEXT DEFAULT 'processed',
    processed_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now')),
    declared_at     TEXT,
    assessed_at     TEXT
);

-- 4. shipment_items: per-item comparison (pipeline vs officer)
CREATE TABLE IF NOT EXISTS shipment_items (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    shipment_id         INTEGER NOT NULL,
    item_number         INTEGER,
    sku                 TEXT DEFAULT '',
    description         TEXT NOT NULL,
    description_norm    TEXT NOT NULL,
    invoice_number      TEXT DEFAULT '',
    supplier_name       TEXT DEFAULT '',
    quantity            REAL,
    unit_cost           REAL,
    total_cost          REAL,
    pipeline_code       TEXT,
    pipeline_source     TEXT,
    pipeline_confidence REAL,
    pipeline_classification_id INTEGER,
    officer_code        TEXT,
    officer_description TEXT,
    officer_commercial_desc TEXT,
    match_status        TEXT,
    code_changed        INTEGER DEFAULT 0,
    taxes_json          TEXT,
    llm_review_json     TEXT,
    created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%f', 'now')),
    FOREIGN KEY (shipment_id) REFERENCES shipments(id),
    FOREIGN KEY (pipeline_classification_id) REFERENCES classifications(id)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_cls_desc_norm ON classifications(description_norm);
CREATE INDEX IF NOT EXISTS idx_cls_tariff ON classifications(tariff_code);
CREATE INDEX IF NOT EXISTS idx_cls_sku ON classifications(sku) WHERE sku IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_cls_source ON classifications(source);
CREATE INDEX IF NOT EXISTS idx_cls_active ON classifications(description_norm, is_active) WHERE is_active = 1;

CREATE INDEX IF NOT EXISTS idx_lookup_code ON description_lookup(tariff_code);

CREATE INDEX IF NOT EXISTS idx_shipment_bl ON shipments(bl_number);

CREATE INDEX IF NOT EXISTS idx_si_shipment ON shipment_items(shipment_id);
CREATE INDEX IF NOT EXISTS idx_si_sku ON shipment_items(sku) WHERE sku != '';
CREATE INDEX IF NOT EXISTS idx_si_desc_norm ON shipment_items(description_norm);
CREATE INDEX IF NOT EXISTS idx_si_code_changed ON shipment_items(code_changed) WHERE code_changed = 1;
CREATE INDEX IF NOT EXISTS idx_si_pipeline_code ON shipment_items(pipeline_code);
CREATE INDEX IF NOT EXISTS idx_si_officer_code ON shipment_items(officer_code) WHERE officer_code IS NOT NULL;

-- Views
CREATE VIEW IF NOT EXISTS v_disagreements AS
SELECT
    s.bl_number,
    si.item_number,
    si.sku,
    si.description,
    si.pipeline_code,
    si.pipeline_source,
    si.pipeline_confidence,
    si.officer_code,
    si.officer_description,
    si.match_status,
    si.taxes_json,
    si.llm_review_json,
    s.processed_at
FROM shipment_items si
JOIN shipments s ON s.id = si.shipment_id
WHERE si.code_changed = 1
  AND si.officer_code IS NOT NULL
ORDER BY s.processed_at DESC, si.item_number;

CREATE VIEW IF NOT EXISTS v_shipment_summary AS
SELECT
    s.id AS shipment_id,
    s.bl_number,
    s.consignee,
    s.status,
    s.processed_at,
    s.assessed_at,
    COUNT(si.id) AS total_items,
    SUM(CASE WHEN si.officer_code IS NOT NULL THEN 1 ELSE 0 END) AS assessed_items,
    SUM(CASE WHEN si.match_status = 'exact_match' THEN 1 ELSE 0 END) AS exact_matches,
    SUM(CASE WHEN si.code_changed = 1 THEN 1 ELSE 0 END) AS disagreements,
    ROUND(
        100.0 * SUM(CASE WHEN si.match_status = 'exact_match' THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN si.officer_code IS NOT NULL THEN 1 ELSE 0 END), 0),
        1
    ) AS accuracy_pct
FROM shipments s
LEFT JOIN shipment_items si ON si.shipment_id = s.id
GROUP BY s.id;

CREATE VIEW IF NOT EXISTS v_source_accuracy AS
SELECT
    si.pipeline_source,
    COUNT(*) AS total_assessed,
    SUM(CASE WHEN si.match_status = 'exact_match' THEN 1 ELSE 0 END) AS exact_matches,
    SUM(CASE WHEN si.code_changed = 1 THEN 1 ELSE 0 END) AS corrections,
    ROUND(
        100.0 * SUM(CASE WHEN si.match_status = 'exact_match' THEN 1 ELSE 0 END)
        / NULLIF(COUNT(*), 0),
        1
    ) AS accuracy_pct,
    ROUND(AVG(si.pipeline_confidence), 3) AS avg_confidence
FROM shipment_items si
WHERE si.officer_code IS NOT NULL
GROUP BY si.pipeline_source
ORDER BY accuracy_pct DESC;

CREATE VIEW IF NOT EXISTS v_llm_corrections AS
SELECT
    si.description,
    si.sku,
    si.pipeline_code AS llm_code,
    si.officer_code AS corrected_code,
    si.pipeline_confidence AS llm_confidence,
    si.officer_description,
    si.llm_review_json,
    s.bl_number,
    s.processed_at
FROM shipment_items si
JOIN shipments s ON s.id = si.shipment_id
WHERE si.pipeline_source IN ('llm_classification', 'web_search_duckduckgo', 'web_search_hts_gov')
  AND si.code_changed = 1
  AND si.officer_code IS NOT NULL
ORDER BY s.processed_at DESC;
"""


# ── Database init ─────────────────────────────────────────────────────────────

def get_db_path(base_dir: str = '.') -> str:
    """Get the classifications database path."""
    return os.path.join(base_dir, 'data', DEFAULT_DB_NAME)


def get_connection(db_path: str) -> sqlite3.Connection:
    """Get a database connection with standard settings."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(db_path: str):
    """Create the database and schema if they don't exist."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = get_connection(db_path)
    conn.executescript(SCHEMA_SQL)
    conn.close()
    logger.info(f"Database initialized: {db_path}")


# ── CRUD operations ───────────────────────────────────────────────────────────

def upsert_classification(
    db_path: str,
    description: str,
    tariff_code: str,
    source: str,
    confidence: float = 0.0,
    category: str = '',
    sku: str = None,
    notes: str = '',
    unverified: int = 1,
    assessed_count: int = None,
    assessed_total: int = None,
    assessed_sources: list = None,
    inventory_refs: list = None,
) -> int:
    """Insert a classification and update the lookup cache.

    If a classification already exists for this description_norm with a different
    tariff code, the old one is marked superseded and the new one becomes active.

    Returns the classification ID.
    """
    desc_norm = normalize_description(description)
    if not desc_norm or not tariff_code:
        return -1

    conn = get_connection(db_path)
    try:
        now = datetime.now().isoformat()

        # Check if there's an existing active classification for this description
        existing = conn.execute(
            "SELECT id, tariff_code FROM classifications "
            "WHERE description_norm = ? AND is_active = 1 "
            "ORDER BY confidence DESC LIMIT 1",
            (desc_norm,)
        ).fetchone()

        # If same code already active, just update confidence if higher
        if existing and existing['tariff_code'] == tariff_code:
            if confidence > 0:
                conn.execute(
                    "UPDATE classifications SET confidence = MAX(confidence, ?), "
                    "updated_at = ? WHERE id = ?",
                    (confidence, now, existing['id'])
                )
                conn.execute(
                    "UPDATE description_lookup SET confidence = MAX(confidence, ?), "
                    "updated_at = ? WHERE description_norm = ?",
                    (confidence, now, desc_norm)
                )
            conn.commit()
            return existing['id']

        # Insert new classification
        cursor = conn.execute(
            "INSERT INTO classifications "
            "(description, description_norm, sku, tariff_code, category, source, "
            " confidence, notes, unverified, assessed_count, assessed_total, "
            " assessed_sources, inventory_refs, is_active, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
            (description, desc_norm, sku, tariff_code, category, source,
             confidence, notes, unverified, assessed_count, assessed_total,
             json.dumps(assessed_sources) if assessed_sources else None,
             json.dumps(inventory_refs) if inventory_refs else None,
             now, now)
        )
        new_id = cursor.lastrowid

        # Supersede old classification if it existed with different code
        if existing:
            conn.execute(
                "UPDATE classifications SET is_active = 0, superseded_by = ?, "
                "updated_at = ? WHERE id = ?",
                (new_id, now, existing['id'])
            )

        # Upsert description_lookup
        conn.execute(
            "INSERT OR REPLACE INTO description_lookup "
            "(description_norm, tariff_code, category, confidence, source, "
            " classification_id, sample_desc, notes, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (desc_norm, tariff_code, category, confidence, source,
             new_id, description, notes, now)
        )

        conn.commit()
        return new_id

    except Exception as e:
        conn.rollback()
        logger.error(f"upsert_classification failed: {e}")
        return -1
    finally:
        conn.close()


def lookup_classification(db_path: str, description: str) -> Optional[Dict]:
    """Fast lookup by normalized description. Returns dict or None."""
    desc_norm = normalize_description(description)
    if not desc_norm:
        return None

    conn = get_connection(db_path)
    try:
        row = conn.execute(
            "SELECT tariff_code, category, confidence, source, sample_desc, notes "
            "FROM description_lookup WHERE description_norm = ?",
            (desc_norm,)
        ).fetchone()

        if row:
            return {
                'code': row['tariff_code'],
                'category': row['category'],
                'confidence': row['confidence'],
                'source': row['source'],
                'sample_desc': row['sample_desc'],
                'notes': row['notes'],
            }
        return None
    finally:
        conn.close()


def bulk_insert_classifications(db_path: str, records: List[Dict]) -> int:
    """Bulk insert classifications (for migration). Returns count inserted."""
    conn = get_connection(db_path)
    inserted = 0
    try:
        now = datetime.now().isoformat()
        for rec in records:
            desc_norm = rec.get('description_norm') or normalize_description(rec.get('description', ''))
            if not desc_norm or not rec.get('tariff_code'):
                continue

            conn.execute(
                "INSERT INTO classifications "
                "(description, description_norm, sku, tariff_code, category, source, "
                " confidence, notes, unverified, assessed_count, assessed_total, "
                " assessed_sources, inventory_refs, is_active, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (rec.get('description', desc_norm), desc_norm, rec.get('sku'),
                 rec['tariff_code'], rec.get('category', ''), rec.get('source', 'unknown'),
                 rec.get('confidence', 0.0), rec.get('notes', ''),
                 rec.get('unverified', 1),
                 rec.get('assessed_count'), rec.get('assessed_total'),
                 json.dumps(rec['assessed_sources']) if rec.get('assessed_sources') else None,
                 json.dumps(rec['inventory_refs']) if rec.get('inventory_refs') else None,
                 rec.get('is_active', 1), now, now)
            )
            inserted += 1

            if inserted % 5000 == 0:
                conn.commit()
                print(f"  ... {inserted} records inserted")

        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"bulk_insert failed at record {inserted}: {e}")
    finally:
        conn.close()

    return inserted


def rebuild_lookup_table(db_path: str) -> int:
    """Rebuild description_lookup from active classifications.

    For each description_norm, picks the highest-confidence active classification.
    """
    conn = get_connection(db_path)
    try:
        conn.execute("DELETE FROM description_lookup")

        count = conn.execute("""
            INSERT INTO description_lookup
                (description_norm, tariff_code, category, confidence, source,
                 classification_id, sample_desc, notes, updated_at)
            SELECT
                c.description_norm,
                c.tariff_code,
                c.category,
                c.confidence,
                c.source,
                c.id,
                c.description,
                c.notes,
                c.updated_at
            FROM classifications c
            WHERE c.is_active = 1
            AND c.id = (
                SELECT c2.id FROM classifications c2
                WHERE c2.description_norm = c.description_norm
                  AND c2.is_active = 1
                ORDER BY c2.confidence DESC, c2.id DESC
                LIMIT 1
            )
        """).rowcount

        conn.commit()
        return count
    finally:
        conn.close()


# ── Shipment operations ───────────────────────────────────────────────────────

def record_shipment(db_path: str, bl_number: str, items: List[Dict],
                    consignee: str = '', supplier_name: str = '') -> int:
    """Record a shipment and its pipeline-classified items. Returns shipment_id."""
    conn = get_connection(db_path)
    try:
        now = datetime.now().isoformat()

        # Upsert shipment (update if exists for re-processing)
        existing = conn.execute(
            "SELECT id FROM shipments WHERE bl_number = ?", (bl_number,)
        ).fetchone()

        if existing:
            shipment_id = existing['id']
            conn.execute(
                "UPDATE shipments SET consignee = ?, supplier_name = ?, "
                "processed_at = ?, status = 'processed' WHERE id = ?",
                (consignee, supplier_name, now, shipment_id)
            )
            # Clear old items for re-processing
            conn.execute("DELETE FROM shipment_items WHERE shipment_id = ?", (shipment_id,))
        else:
            cursor = conn.execute(
                "INSERT INTO shipments (bl_number, consignee, supplier_name, processed_at) "
                "VALUES (?, ?, ?, ?)",
                (bl_number, consignee, supplier_name, now)
            )
            shipment_id = cursor.lastrowid

        # Insert items
        for i, item in enumerate(items):
            desc_norm = normalize_description(item.get('description', ''))
            conn.execute(
                "INSERT INTO shipment_items "
                "(shipment_id, item_number, sku, description, description_norm, "
                " invoice_number, supplier_name, quantity, unit_cost, total_cost, "
                " pipeline_code, pipeline_source, pipeline_confidence) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (shipment_id, i + 1,
                 item.get('sku', ''), item.get('description', ''), desc_norm,
                 item.get('invoice', ''), item.get('supplier_name', ''),
                 item.get('quantity'), item.get('cost'), item.get('total_cost'),
                 item.get('tariff_code', ''), item.get('source', ''),
                 item.get('confidence'))
            )

        conn.commit()
        return shipment_id

    except Exception as e:
        conn.rollback()
        logger.error(f"record_shipment failed: {e}")
        return -1
    finally:
        conn.close()


def import_officer_codes(db_path: str, shipment_id: int,
                         officer_items: List[Dict]) -> Dict:
    """Import officer classifications from ASYCUDA XML and compute match status.

    officer_items: list of dicts with keys:
        sku, description, commodity_code, officer_description, taxes_json

    Matches officer items to shipment_items by sku or description.
    Returns summary dict.
    """
    conn = get_connection(db_path)
    try:
        now = datetime.now().isoformat()

        # Load existing pipeline items for this shipment
        pipeline_items = conn.execute(
            "SELECT id, sku, description, description_norm, pipeline_code "
            "FROM shipment_items WHERE shipment_id = ?",
            (shipment_id,)
        ).fetchall()

        # Build indexes for matching
        by_sku = {}
        by_desc = {}
        for row in pipeline_items:
            if row['sku']:
                by_sku[row['sku'].lower()] = row
            if row['description_norm']:
                by_desc[row['description_norm']] = row

        matched = 0
        exact = 0
        changed = 0
        unmatched = 0

        for oitem in officer_items:
            officer_code = oitem.get('commodity_code', '')
            sku = (oitem.get('sku') or '').lower()
            desc = oitem.get('description', '')
            desc_norm = normalize_description(oitem.get('commercial_desc', desc))

            # Match by sku first, then description
            pipeline_row = None
            if sku and sku in by_sku:
                pipeline_row = by_sku.pop(sku)
            elif desc_norm and desc_norm in by_desc:
                pipeline_row = by_desc.pop(desc_norm)
            else:
                # Fuzzy match
                for key, row in list(by_desc.items()):
                    tokens_a = set(desc_norm.split())
                    tokens_b = set(key.split())
                    if tokens_a and tokens_b:
                        overlap = len(tokens_a & tokens_b) / max(len(tokens_a), len(tokens_b))
                        if overlap >= 0.60:
                            pipeline_row = row
                            del by_desc[key]
                            break

            if pipeline_row is None:
                unmatched += 1
                continue

            pipeline_code = pipeline_row['pipeline_code'] or ''
            code_is_changed = 1 if officer_code != pipeline_code else 0

            if officer_code == pipeline_code:
                status = 'exact_match'
                exact += 1
            elif officer_code[:6] == pipeline_code[:6]:
                status = 'subheading_match'
            elif officer_code[:4] == pipeline_code[:4]:
                status = 'heading_match'
            else:
                status = 'disagree'

            if code_is_changed:
                changed += 1

            conn.execute(
                "UPDATE shipment_items SET "
                "officer_code = ?, officer_description = ?, "
                "officer_commercial_desc = ?, match_status = ?, "
                "code_changed = ?, taxes_json = ? "
                "WHERE id = ?",
                (officer_code, oitem.get('officer_description', ''),
                 oitem.get('commercial_desc', ''), status,
                 code_is_changed, oitem.get('taxes_json'),
                 pipeline_row['id'])
            )
            matched += 1

        # Update shipment status
        conn.execute(
            "UPDATE shipments SET status = 'assessed', assessed_at = ? WHERE id = ?",
            (now, shipment_id)
        )

        conn.commit()
        return {
            'matched': matched,
            'exact': exact,
            'changed': changed,
            'unmatched_officer': unmatched,
        }

    except Exception as e:
        conn.rollback()
        logger.error(f"import_officer_codes failed: {e}")
        return {'error': str(e)}
    finally:
        conn.close()


def get_comparison(db_path: str, bl_number: str) -> Dict:
    """Get full comparison results for a shipment."""
    conn = get_connection(db_path)
    try:
        shipment = conn.execute(
            "SELECT * FROM shipments WHERE bl_number = ?", (bl_number,)
        ).fetchone()
        if not shipment:
            return {'error': f'Shipment {bl_number} not found'}

        items = conn.execute(
            "SELECT * FROM shipment_items WHERE shipment_id = ? ORDER BY item_number",
            (shipment['id'],)
        ).fetchall()

        summary = conn.execute(
            "SELECT * FROM v_shipment_summary WHERE bl_number = ?", (bl_number,)
        ).fetchone()

        return {
            'shipment': dict(shipment),
            'items': [dict(i) for i in items],
            'summary': dict(summary) if summary else {},
        }
    finally:
        conn.close()


def get_stats(db_path: str) -> Dict:
    """Get database statistics."""
    conn = get_connection(db_path)
    try:
        stats = {}
        for table in ['classifications', 'description_lookup', 'shipments', 'shipment_items']:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            stats[table] = count

        # Source breakdown
        sources = conn.execute(
            "SELECT source, COUNT(*) as cnt FROM classifications "
            "WHERE is_active = 1 GROUP BY source ORDER BY cnt DESC"
        ).fetchall()
        stats['sources'] = {r['source']: r['cnt'] for r in sources}

        # Shipment count
        stats['shipments_assessed'] = conn.execute(
            "SELECT COUNT(*) FROM shipments WHERE status = 'assessed'"
        ).fetchone()[0]

        return stats
    finally:
        conn.close()


# ── Migration ─────────────────────────────────────────────────────────────────

def migrate_from_json(db_path: str, base_dir: str):
    """Seed the database from existing JSON files."""
    init_db(db_path)

    print("=== Migrating to SQLite classifications database ===")
    print()

    # 1. Import assessed classifications
    assessed_path = os.path.join(base_dir, 'data', 'assessed_classifications.json')
    if os.path.exists(assessed_path):
        print(f"Loading assessed classifications from {os.path.basename(assessed_path)}...")
        with open(assessed_path, 'r', encoding='utf-8') as f:
            assessed_data = json.load(f)

        entries = assessed_data.get('entries', assessed_data)
        if '_metadata' in entries:
            del entries['_metadata']

        records = []
        for desc_norm, entry in entries.items():
            if desc_norm == '_metadata':
                continue
            records.append({
                'description': entry.get('sample_desc', desc_norm),
                'description_norm': desc_norm,
                'tariff_code': entry['code'],
                'category': entry.get('category', ''),
                'source': 'assessed_exact',
                'confidence': entry.get('confidence', 0.0),
                'notes': f"count={entry.get('count', 0)}/{entry.get('total', 0)}",
                'unverified': 0,  # Assessed = verified by officer
                'assessed_count': entry.get('count'),
                'assessed_total': entry.get('total'),
                'assessed_sources': entry.get('sources', []),
                'inventory_refs': entry.get('inventory_refs', []),
                'is_active': 1,
            })

        count = bulk_insert_classifications(db_path, records)
        print(f"  Imported {count} assessed classifications")
    else:
        print(f"  SKIP: {assessed_path} not found")

    # 2. Import HS lookup cache
    cache_path = os.path.join(base_dir, 'data', 'hs_lookup_cache.json')
    if os.path.exists(cache_path):
        print(f"\nLoading cache from {os.path.basename(cache_path)}...")
        with open(cache_path, 'r', encoding='utf-8') as f:
            cache_data = json.load(f)

        # Check which cache entries conflict with assessed data already in DB
        conn = get_connection(db_path)
        assessed_lookup = {}
        for row in conn.execute("SELECT description_norm, tariff_code FROM classifications WHERE source = 'assessed_exact' AND is_active = 1"):
            assessed_lookup[row['description_norm']] = row['tariff_code']
        conn.close()

        records = []
        skipped_conflict = 0
        skipped_asycuda = 0

        for key, entry in cache_data.items():
            if key.startswith('__asycuda_'):
                skipped_asycuda += 1
                continue

            desc = entry.get('original_description', key)
            desc_norm = normalize_description(desc)
            code = entry.get('code', '')

            if not desc_norm or not code:
                continue

            # Check for heading-level conflict with assessed data
            if desc_norm in assessed_lookup:
                if code[:4] != assessed_lookup[desc_norm][:4]:
                    skipped_conflict += 1
                    continue

            source = entry.get('source', 'web_search')
            is_verified = entry.get('verified_against_assessed', False)

            records.append({
                'description': desc,
                'description_norm': desc_norm,
                'tariff_code': code,
                'category': entry.get('category', ''),
                'source': source,
                'confidence': entry.get('confidence', 0.5),
                'notes': entry.get('notes', ''),
                'unverified': 0 if is_verified else 1,
                'is_active': 1,
            })

        count = bulk_insert_classifications(db_path, records)
        print(f"  Imported {count} cache entries")
        print(f"  Skipped {skipped_conflict} conflicting with assessed data")
        print(f"  Skipped {skipped_asycuda} asycuda tax entries")
    else:
        print(f"  SKIP: {cache_path} not found")

    # 3. Build lookup table
    print("\nRebuilding description_lookup table...")
    lookup_count = rebuild_lookup_table(db_path)
    print(f"  {lookup_count} entries in lookup table")

    # Print summary
    stats = get_stats(db_path)
    print()
    print("=== Migration complete ===")
    print(f"  classifications:     {stats['classifications']}")
    print(f"  description_lookup:  {stats['description_lookup']}")
    print(f"  shipments:           {stats['shipments']}")
    print(f"  shipment_items:      {stats['shipment_items']}")
    print()
    print("  Source breakdown:")
    for source, cnt in stats.get('sources', {}).items():
        print(f"    {source}: {cnt}")


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Classifications SQLite database')
    parser.add_argument('--migrate', action='store_true',
                        help='Migrate from JSON files to SQLite')
    parser.add_argument('--stats', action='store_true',
                        help='Show database statistics')
    parser.add_argument('--base-dir',
                        default=os.path.join(os.path.dirname(__file__), '..'),
                        help='Project base directory')

    args = parser.parse_args()
    base_dir = os.path.abspath(args.base_dir)
    db_path = get_db_path(base_dir)

    if args.migrate:
        migrate_from_json(db_path, base_dir)
    elif args.stats:
        if not os.path.exists(db_path):
            print(f"Database not found: {db_path}")
            print("Run with --migrate first.")
            sys.exit(1)
        stats = get_stats(db_path)
        print("=== Classifications Database Stats ===")
        print(f"  Path: {db_path}")
        print(f"  Size: {os.path.getsize(db_path) / 1024 / 1024:.1f} MB")
        print()
        for table, count in stats.items():
            if isinstance(count, dict):
                continue
            print(f"  {table}: {count}")
        print()
        print("  Active sources:")
        for source, cnt in stats.get('sources', {}).items():
            print(f"    {source}: {cnt}")
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
