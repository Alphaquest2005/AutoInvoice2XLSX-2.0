#!/usr/bin/env python3
"""
Stage: Supplier Lookup
Looks up supplier information from the database and fills in missing metadata.
If supplier not found, adds a placeholder entry for manual completion.

This stage runs after extraction/parsing to enrich invoice metadata with:
- Supplier full name
- Supplier address
- Country code
- Currency
"""

import json
import os
import re
import logging
from datetime import datetime
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# Default database path
DEFAULT_DB_PATH = 'data/suppliers.json'


class SupplierDatabase:
    """Manages supplier information lookup and storage."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.data = self._load_database()

    def _load_database(self) -> Dict:
        """Load supplier database from JSON file."""
        if os.path.exists(self.db_path):
            try:
                with open(self.db_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load supplier database: {e}")
                return self._create_empty_database()
        else:
            logger.info(f"Creating new supplier database at {self.db_path}")
            return self._create_empty_database()

    def _create_empty_database(self) -> Dict:
        """Create empty database structure."""
        return {
            "version": "1.0.0",
            "description": "Supplier database for invoice processing",
            "suppliers": {},
            "last_updated": datetime.now().strftime("%Y-%m-%d")
        }

    def save(self):
        """Save database to file."""
        self.data["last_updated"] = datetime.now().strftime("%Y-%m-%d")
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        with open(self.db_path, 'w', encoding='utf-8') as f:
            json.dump(self.data, f, indent=2)
        logger.info(f"Supplier database saved to {self.db_path}")

    def lookup(self, supplier_name: str) -> Optional[Dict]:
        """
        Look up supplier by name or code.

        Args:
            supplier_name: Supplier name or code to look up

        Returns:
            Supplier info dict or None if not found
        """
        if not supplier_name:
            return None

        name_upper = supplier_name.upper().strip()
        suppliers = self.data.get("suppliers", {})

        # Direct lookup by code
        if name_upper in suppliers:
            return suppliers[name_upper]

        # Search by name variations
        for code, info in suppliers.items():
            if name_upper == info.get("name", "").upper():
                return info
            if name_upper == info.get("full_name", "").upper():
                return info
            # Partial match
            if name_upper in info.get("name", "").upper():
                return info

        return None

    def add_supplier(self, code: str, info: Dict) -> bool:
        """
        Add or update supplier in database.

        Args:
            code: Supplier code (e.g., "TEMU")
            info: Supplier information dict

        Returns:
            True if added/updated successfully
        """
        code_upper = code.upper().strip()

        # Ensure required fields
        info.setdefault("code", code_upper)
        info.setdefault("name", code_upper)

        self.data["suppliers"][code_upper] = info
        self.save()
        logger.info(f"Added/updated supplier: {code_upper}")
        return True

    def add_unknown_supplier(self, name: str, invoice_number: str = None) -> Dict:
        """
        Add placeholder entry for unknown supplier.

        Args:
            name: Detected supplier name
            invoice_number: Invoice number for reference

        Returns:
            Placeholder supplier info
        """
        code = name.upper().strip() if name else "UNKNOWN"

        placeholder = {
            "code": code,
            "name": name or "Unknown Supplier",
            "full_name": f"{name or 'Unknown'} (NEEDS VERIFICATION)",
            "address": "ADDRESS REQUIRED",
            "country_code": "US",  # Default assumption
            "country": "United States",
            "currency": "USD",
            "needs_verification": True,
            "first_seen": datetime.now().isoformat(),
            "first_invoice": invoice_number,
            "notes": "Auto-added - requires manual verification"
        }

        # Only add if not already in database
        if code not in self.data.get("suppliers", {}):
            self.add_supplier(code, placeholder)
            logger.warning(f"Added unknown supplier for verification: {code}")

        return placeholder


def enrich_metadata(metadata: Dict, supplier_info: Dict) -> Dict:
    """
    Enrich invoice metadata with supplier information.

    Only fills in fields that are missing or empty.

    Args:
        metadata: Current invoice metadata
        supplier_info: Supplier information from database

    Returns:
        Enriched metadata dict
    """
    if not supplier_info:
        return metadata

    # Map supplier fields to metadata fields
    field_mapping = {
        "supplier": "name",
        "supplier_code": "code",
        "supplier_name": "full_name",
        "supplier_address": "address",
        "country_code": "country_code",
        "currency": "currency",
    }

    for meta_field, supplier_field in field_mapping.items():
        # Only fill if metadata field is missing or empty
        current_value = metadata.get(meta_field)
        if not current_value or current_value in ['None', '', 'UNKNOWN']:
            supplier_value = supplier_info.get(supplier_field)
            if supplier_value:
                metadata[meta_field] = supplier_value

    return metadata


def run(input_path: str, output_path: str, config: Dict = None, context: Dict = None) -> Dict:
    """
    Run supplier lookup stage.

    Args:
        input_path: Path to parsed/classified JSON
        output_path: Path to write enriched JSON
        config: Stage configuration
        context: Pipeline context

    Returns:
        Stage result dict
    """
    config = config or {}
    context = context or {}

    if not input_path or not os.path.exists(input_path):
        return {'status': 'error', 'error': f'Input not found: {input_path}'}

    # Determine database path
    base_dir = context.get('base_dir', os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    db_path = config.get('database', os.path.join(base_dir, DEFAULT_DB_PATH))

    # Initialize database
    db = SupplierDatabase(db_path)

    # Load input data
    with open(input_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    metadata = data.get('invoice_metadata', {})
    supplier_name = metadata.get('supplier')
    invoice_number = metadata.get('invoice_number')

    result = {
        'status': 'success',
        'supplier_found': False,
        'supplier_added': False,
        'fields_enriched': []
    }

    # Look up supplier
    supplier_info = db.lookup(supplier_name)

    if supplier_info:
        result['supplier_found'] = True
        result['supplier_code'] = supplier_info.get('code')
        logger.info(f"Found supplier in database: {supplier_info.get('code')}")
    elif supplier_name:
        # Add unknown supplier for tracking
        supplier_info = db.add_unknown_supplier(supplier_name, invoice_number)
        result['supplier_added'] = True
        result['supplier_code'] = supplier_info.get('code')
        result['needs_verification'] = True
        logger.warning(f"Unknown supplier added for verification: {supplier_name}")

    # Enrich metadata
    if supplier_info:
        original_metadata = metadata.copy()
        metadata = enrich_metadata(metadata, supplier_info)

        # Track which fields were enriched
        for key in metadata:
            if metadata.get(key) != original_metadata.get(key):
                result['fields_enriched'].append(key)

        data['invoice_metadata'] = metadata

    # Save enriched data
    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)

    result['total_suppliers'] = len(db.data.get('suppliers', {}))
    result['input'] = input_path
    result['output'] = output_path

    return result


def list_suppliers(db_path: str = None) -> Dict:
    """
    List all suppliers in database.

    Args:
        db_path: Path to database file

    Returns:
        Dict with supplier list
    """
    if not db_path:
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        db_path = os.path.join(base_dir, DEFAULT_DB_PATH)

    db = SupplierDatabase(db_path)
    suppliers = db.data.get('suppliers', {})

    return {
        'total': len(suppliers),
        'suppliers': [
            {
                'code': code,
                'name': info.get('name'),
                'country': info.get('country_code'),
                'needs_verification': info.get('needs_verification', False)
            }
            for code, info in suppliers.items()
        ]
    }


if __name__ == '__main__':
    import sys
    import argparse

    parser = argparse.ArgumentParser(description='Supplier Lookup Stage')
    parser.add_argument('--input', '-i', help='Input JSON file')
    parser.add_argument('--output', '-o', help='Output JSON file')
    parser.add_argument('--database', '-d', help='Supplier database path')
    parser.add_argument('--list', '-l', action='store_true', help='List all suppliers')
    parser.add_argument('--add', help='Add supplier: CODE:NAME:ADDRESS:COUNTRY')

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    if args.list:
        result = list_suppliers(args.database)
        print(json.dumps(result, indent=2))
        sys.exit(0)

    if args.add:
        parts = args.add.split(':')
        if len(parts) >= 2:
            code = parts[0]
            info = {
                'name': parts[1],
                'full_name': parts[1],
                'address': parts[2] if len(parts) > 2 else '',
                'country_code': parts[3] if len(parts) > 3 else 'US',
            }
            db_path = args.database or os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                DEFAULT_DB_PATH
            )
            db = SupplierDatabase(db_path)
            db.add_supplier(code, info)
            print(f"Added supplier: {code}")
            sys.exit(0)
        else:
            print("Error: --add requires CODE:NAME[:ADDRESS:COUNTRY]")
            sys.exit(1)

    if args.input:
        result = run(args.input, args.output, config={'database': args.database})
        print(json.dumps(result, indent=2))
        sys.exit(0 if result.get('status') == 'success' else 1)

    parser.print_help()
