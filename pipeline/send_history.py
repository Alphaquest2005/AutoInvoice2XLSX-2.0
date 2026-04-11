"""
Persistent log of shipment emails the pipeline has sent.

Used to detect and resend shipments whose output would change if reprocessed
(e.g., after a supplier DB correction). See run.py --resend / --resend-stale.

The history file lives at data/send_history.json and is a simple append-only
log (the most recent entry per waybill wins). Each entry captures enough to
re-run the pipeline against the original input and compare the new email
params against the ones that were sent.
"""
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
HISTORY_PATH = os.path.join(BASE_DIR, 'data', 'send_history.json')
SUPPLIERS_PATH = os.path.join(BASE_DIR, 'data', 'suppliers.json')


def _sha256_file(path: str) -> str:
    if not path or not os.path.isfile(path):
        return ''
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


def params_hash(params: Dict) -> str:
    """
    Hash of the semantically-meaningful email params.

    Attachment absolute paths are normalized to basenames so the same
    shipment reprocessed into a different output_dir still compares equal
    when nothing substantive changed.
    """
    canon = {k: v for k, v in params.items() if k != 'attachment_paths'}
    canon['attachment_names'] = sorted(
        os.path.basename(p) for p in (params.get('attachment_paths') or [])
    )
    blob = json.dumps(canon, sort_keys=True, ensure_ascii=False).encode('utf-8')
    return hashlib.sha256(blob).hexdigest()


def load_history() -> Dict:
    """Load send history JSON. Returns {'version': '1.0', 'sends': [...]}."""
    if not os.path.isfile(HISTORY_PATH):
        return {'version': '1.0', 'sends': []}
    try:
        with open(HISTORY_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if 'sends' not in data:
            data['sends'] = []
        return data
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"send_history load failed ({e}); treating as empty")
        return {'version': '1.0', 'sends': []}


def save_history(history: Dict) -> None:
    os.makedirs(os.path.dirname(HISTORY_PATH), exist_ok=True)
    tmp_path = HISTORY_PATH + '.tmp'
    with open(tmp_path, 'w', encoding='utf-8') as f:
        json.dump(history, f, indent=2, ensure_ascii=False)
    os.replace(tmp_path, HISTORY_PATH)


def record_send(
    waybill: str,
    subject: str,
    source_input: str,
    source_mode: str,
    output_dir: str,
    params: Dict,
    attachments: List[str],
) -> None:
    """Append a successful send to send_history.json (one entry per waybill)."""
    if not waybill:
        logger.debug("record_send skipped: missing waybill")
        return
    entry = {
        'waybill': waybill,
        'subject': subject,
        'source_input': os.path.abspath(source_input) if source_input else '',
        'source_mode': source_mode,
        'output_dir': os.path.abspath(output_dir) if output_dir else '',
        'params_hash': params_hash(params),
        'suppliers_hash': _sha256_file(SUPPLIERS_PATH),
        'attachments': [os.path.abspath(a) for a in (attachments or [])],
        'params': params,
        'sent_at': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
    }
    history = load_history()
    history['sends'] = [
        e for e in history['sends'] if e.get('waybill') != waybill
    ]
    history['sends'].append(entry)
    save_history(history)
    logger.info(f"send history recorded: {waybill} -> {HISTORY_PATH}")


def find_by_waybill(waybill: str) -> Optional[Dict]:
    """Return the most recent history entry for a given waybill, or None."""
    history = load_history()
    for entry in reversed(history['sends']):
        if entry.get('waybill') == waybill:
            return entry
    return None


def all_entries() -> List[Dict]:
    return list(load_history().get('sends', []))
