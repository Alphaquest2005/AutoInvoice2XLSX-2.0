"""
LLM-based format spec generation for unknown invoice formats.

Lifecycle:
- Auto-generated specs go to config/formats/_auto/
- On successful import (XLSX + variance resolved): promote to config/formats/
- On failure or discard: delete from _auto/
"""

import logging
import os
import re
import shutil
from datetime import datetime
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# Constant prompt - no variance from construction
SYSTEM_PROMPT = """You are an invoice format specification generator. Your task is to analyze invoice text and generate a YAML format spec.

The format spec structure is:
```yaml
name: supplier_name
version: "1.0"
description: "Brief description"

detect:
  all_of:
    - "Required text 1"
  any_of:
    - "Optional text 1"

metadata:
  invoice_number:
    patterns:
      - 'regex_pattern_with_(capture_group)'
    required: true
  date:
    patterns:
      - 'date_regex'
  supplier_name:
    value: "Static Name"
  country_code:
    value: "US"
  total:
    patterns:
      - 'total_regex'
    type: currency

items:
  strategy: line
  line:
    pattern: 'regex_to_match_item_lines_with_(description)_and_(price)_and_optional_(quantity)'
    field_map:
      description: 1
      unit_price: 2
      quantity: 3
    types:
      quantity: integer
      unit_price: currency
    skip_patterns:
      - '^patterns_to_skip'
    generated_fields:
      sku: "PREFIX-{index}"

sections:
  items_start:
    - "Items Ordered"
  items_end:
    - "Total"

validation:
  min_items: 1
  max_item_price: 10000
```

IMPORTANT:
1. Analyze the invoice text carefully to understand the structure
2. Identify detection patterns (unique text that identifies this format)
3. Create regex patterns for invoice number, date, total
4. Create a line pattern that captures item description and price
5. Add skip_patterns for lines that look like items but are actually metadata (subtotal, tax, shipping)

QUANTITY COLUMN RULES:
- Many B2B invoices have columns: ORD (ordered), SHP (shipped), BCK (backordered)
- ALWAYS use the SHIPPED quantity (SHP) as the quantity, NOT the ordered quantity (ORD)
- If your regex captures multiple quantity columns, use the 'group' key to select the SHP column
- Example for "ORD SHP BCK" columns with a block strategy:
    quantity:
      pattern: '(?:EA|PR|BG|CS)\\s+(\\d+)\\s+(\\d+)'
      group: 2
      type: integer
- Items with SHP=0 and $0.00 total are backordered items — they should still be extracted (not skipped)
- The 'group' key works in both line field_map and block field specs to select a specific capture group

Respond with ONLY the YAML content, no markdown code blocks or explanations."""


def generate_format_spec(invoice_text: str, detected_supplier: str = None) -> Optional[Dict]:
    """
    Generate a format spec from invoice text using the LLM.

    Args:
        invoice_text: Extracted invoice text
        detected_supplier: Pre-detected supplier name (or None to auto-detect)

    Returns:
        dict with 'success', 'format_name', 'spec_path', 'spec_data'
        or dict with 'success': False, 'error': str
    """
    from core.config import get_config
    from core.llm_client import get_llm_client

    cfg = get_config()
    llm = get_llm_client()

    if not invoice_text.strip():
        return {'success': False, 'error': 'No text to generate format spec from'}

    # Auto-detect supplier if not provided
    if not detected_supplier:
        detected_supplier = _detect_supplier(invoice_text)

    # Truncate text
    if len(invoice_text) > cfg.max_invoice_text_length:
        invoice_text = invoice_text[:cfg.max_invoice_text_length] + "\n... [truncated]"

    user_message = f"""Analyze this invoice text and generate a YAML format spec.

Detected supplier: {detected_supplier}

Invoice text:
{invoice_text}

Generate the YAML format spec for this invoice format."""

    response = llm.call(
        user_message=user_message,
        system_prompt=SYSTEM_PROMPT,
        cache_key_extra=f"supplier:{detected_supplier}",
    )

    if not response:
        return {'success': False, 'error': 'LLM returned no response'}

    # Parse and validate YAML
    yaml_content = _clean_yaml_response(response)
    spec_data = _validate_yaml(yaml_content)
    if not spec_data:
        return {'success': False, 'error': 'LLM generated invalid YAML'}

    # Save to _auto/ directory
    format_name = _safe_name(spec_data.get('name', detected_supplier))
    spec_path = _save_auto_spec(format_name, yaml_content, detected_supplier, cfg)

    return {
        'success': True,
        'format_name': format_name,
        'spec_path': spec_path,
        'spec_data': spec_data,
    }


def promote_spec(spec_path: str) -> Optional[str]:
    """
    Promote an auto-generated spec to the main formats directory.

    Called when processing succeeds (XLSX generated + variance resolved).

    Returns new path, or None if failed.
    """
    from core.config import get_config
    cfg = get_config()

    if not os.path.exists(spec_path):
        return None

    filename = os.path.basename(spec_path)
    dest_path = os.path.join(cfg.formats_dir, filename)

    # Don't overwrite existing manual specs
    if os.path.exists(dest_path):
        logger.info(f"Format spec already exists at {dest_path}, keeping auto version")
        return spec_path

    try:
        shutil.move(spec_path, dest_path)
        logger.info(f"Promoted auto-generated spec: {filename}")
        return dest_path
    except Exception as e:
        logger.error(f"Failed to promote spec: {e}")
        return None


def discard_spec(spec_path: str):
    """
    Delete a failed auto-generated spec.

    Called when processing fails (no XLSX or variance unresolved).
    """
    if spec_path and os.path.exists(spec_path):
        try:
            os.remove(spec_path)
            logger.info(f"Discarded failed auto-generated spec: {os.path.basename(spec_path)}")
        except Exception as e:
            logger.warning(f"Failed to delete spec {spec_path}: {e}")


def cleanup_unused_auto_specs(max_age_days: int = 7):
    """
    Clean up auto-generated specs older than max_age_days.

    Called periodically to prevent _auto/ directory from growing.
    """
    from core.config import get_config
    cfg = get_config()
    auto_dir = cfg.auto_formats_dir

    if not os.path.exists(auto_dir):
        return

    import time
    cutoff = time.time() - (max_age_days * 86400)
    removed = 0

    for f in os.listdir(auto_dir):
        if f.endswith('.yaml') or f.endswith('.yml'):
            fpath = os.path.join(auto_dir, f)
            if os.path.getmtime(fpath) < cutoff:
                os.remove(fpath)
                removed += 1

    if removed:
        logger.info(f"Cleaned up {removed} unused auto-generated specs")


def _detect_supplier(text: str) -> str:
    """Detect supplier from invoice text."""
    patterns = [
        (r'SHEIN', 'shein'),
        (r'TEMU', 'temu'),
        (r'Amazon\.com|AMAZON', 'amazon'),
        (r'eBay', 'ebay'),
        (r'AliExpress|ALIEXPRESS', 'aliexpress'),
        (r'Walmart', 'walmart'),
        (r'Target', 'target'),
        (r'Fashion\s*Nova', 'fashionnova'),
    ]

    for pattern, name in patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return name

    match = re.search(r'(?:Sold by|Supplier|From|Ship from)[:\s]+([A-Za-z0-9\s]+)', text)
    if match:
        return _safe_name(match.group(1).strip())[:20]

    words = text.split()
    return 'unknown_' + _safe_name(words[0] if words else 'format')


def _safe_name(name: str) -> str:
    """Convert name to filesystem-safe format."""
    return re.sub(r'[^a-z0-9_]', '_', name.lower()).strip('_')


def _clean_yaml_response(response: str) -> str:
    """Clean LLM response to extract YAML content."""
    yaml_content = response.strip()
    if yaml_content.startswith('```'):
        lines = yaml_content.split('\n')
        yaml_content = '\n'.join(lines[1:-1] if lines[-1].strip() == '```' else lines[1:])

    # Fix unquoted regex patterns
    fixed_lines = []
    for line in yaml_content.split('\n'):
        if 'pattern:' in line and "'" not in line and '"' not in line:
            match = re.match(r'^(\s*-?\s*pattern:\s*)(.+)$', line)
            if match:
                prefix, pattern = match.groups()
                pattern = pattern.strip()
                if pattern and not (pattern.startswith("'") or pattern.startswith('"')):
                    pattern = "'" + pattern.replace("'", "''") + "'"
                    line = prefix + pattern
        fixed_lines.append(line)

    return '\n'.join(fixed_lines)


def _validate_yaml(yaml_content: str) -> Optional[Dict]:
    """Validate and parse YAML content. Returns parsed dict or None."""
    try:
        import yaml
        spec_data = yaml.safe_load(yaml_content)
        if spec_data and isinstance(spec_data, dict) and 'name' in spec_data:
            return spec_data
    except Exception:
        pass
    return None


def _save_auto_spec(format_name: str, yaml_content: str, supplier: str, cfg) -> str:
    """Save auto-generated spec to _auto/ directory."""
    os.makedirs(cfg.auto_formats_dir, exist_ok=True)

    spec_path = os.path.join(cfg.auto_formats_dir, f'{format_name}.yaml')

    # Don't create duplicate versioned names - overwrite in _auto/
    with open(spec_path, 'w', encoding='utf-8') as f:
        f.write(f"# Auto-generated format spec for {supplier}\n")
        f.write(f"# Generated on {datetime.now().isoformat()}\n")
        f.write(f"# Will be promoted to formats/ on successful processing\n\n")
        f.write(yaml_content)

    logger.info(f"Saved auto-generated spec: {spec_path}")
    return spec_path
