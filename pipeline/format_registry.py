#!/usr/bin/env python3
"""
Format Registry

Loads format specifications from config/formats/*.yaml and provides
format detection and parser routing.

Usage:
    registry = FormatRegistry('/path/to/project')
    parser = registry.get_parser(invoice_text)
    if parser:
        result = parser.parse(invoice_text)
"""

import os
import logging
from typing import Any, Dict, List, Optional

try:
    import yaml
except ImportError:
    yaml = None

from format_parser import FormatParser, create_parser

logger = logging.getLogger(__name__)


class FormatRegistry:
    """
    Registry of invoice format specifications.

    Loads all format specs from config/formats/*.yaml and provides
    methods to detect formats and get appropriate parsers.
    """

    def __init__(self, base_dir: str = None):
        """
        Initialize registry and load all format specs.

        Args:
            base_dir: Project root directory (contains config/formats/)
        """
        self.base_dir = base_dir or self._find_base_dir()
        self.formats: List[Dict] = []
        self.formats_by_name: Dict[str, Dict] = {}
        self._load_formats()

    def _find_base_dir(self) -> str:
        """Find project base directory relative to this script."""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        return os.path.dirname(script_dir)

    def _load_formats(self) -> None:
        """Load all format specifications from config/formats/*.yaml and _auto/*.yaml."""
        if not yaml:
            logger.error("PyYAML not installed. Run: pip install pyyaml")
            return

        formats_dir = os.path.join(self.base_dir, 'config', 'formats')
        auto_dir = os.path.join(formats_dir, '_auto')

        # Load from main dir first (higher priority), then _auto/
        dirs_to_scan = []
        if os.path.isdir(formats_dir):
            dirs_to_scan.append(('main', formats_dir))
        if os.path.isdir(auto_dir):
            dirs_to_scan.append(('auto', auto_dir))

        if not dirs_to_scan:
            logger.warning(f"Formats directory not found: {formats_dir}")
            return

        for source_label, scan_dir in dirs_to_scan:
            for filename in os.listdir(scan_dir):
                if not filename.endswith('.yaml') and not filename.endswith('.yml'):
                    continue
                # Skip deprecated specs
                if filename.startswith('_deprecated'):
                    continue
                # Skip subdirectories (like _auto itself when scanning main dir)
                filepath = os.path.join(scan_dir, filename)
                if not os.path.isfile(filepath):
                    continue

                try:
                    with open(filepath, 'r', encoding='utf-8') as f:
                        spec = yaml.safe_load(f)

                    if spec and isinstance(spec, dict):
                        name = spec.get('name', filename.replace('.yaml', ''))
                        spec['_source_file'] = filename
                        spec['_source_dir'] = source_label
                        # Default priority: 0, higher values = checked first
                        # Auto-generated specs get lower priority than manual ones
                        if 'priority' not in spec:
                            spec['priority'] = -10 if source_label == 'auto' else 0
                        # Don't overwrite manual spec with auto-generated one
                        if name not in self.formats_by_name:
                            self.formats.append(spec)
                            self.formats_by_name[name] = spec
                            logger.debug(f"Loaded format spec: {name} ({source_label})")
                        else:
                            logger.debug(f"Skipped duplicate format spec: {name} ({source_label})")

                except Exception as e:
                    logger.error(f"Failed to load format spec {filename}: {e}")

        # Sort formats by priority (higher first), then by name for consistency
        self.formats.sort(key=lambda x: (-x.get('priority', 0), x.get('name', '')))

        logger.info(f"Loaded {len(self.formats)} format specifications")

    def detect_format(self, text: str) -> Optional[Dict]:
        """
        Detect which format specification matches the given text.

        Args:
            text: Invoice text to analyze

        Returns:
            Matching format spec dict, or None if no match
        """
        for spec in self.formats:
            if self._matches(text, spec):
                logger.debug(f"Detected format: {spec.get('name')}")
                return spec

        logger.debug("No format matched")
        return None

    def _matches(self, text: str, spec: Dict) -> bool:
        """
        Check if text matches a format's detection rules.

        Detection rules:
          - all_of: ALL patterns must be present
          - any_of: At least ONE pattern must be present
          - none_of: NONE of these patterns may be present (exclusion)
        """
        detect = spec.get('detect', {})

        # Handle old-style list format (for backwards compatibility)
        if isinstance(detect, list):
            return all(pattern in text for pattern in detect)

        # New-style dict format
        all_of = detect.get('all_of', [])
        any_of = detect.get('any_of', [])
        none_of = detect.get('none_of', [])

        # Check none_of patterns FIRST (NONE may be present - exclusion)
        if none_of:
            if any(self._pattern_in_text(pattern, text) for pattern in none_of):
                return False

        # Check all_of patterns (ALL must match)
        if all_of:
            if not all(self._pattern_in_text(pattern, text) for pattern in all_of):
                return False

        # Check any_of patterns (at least ONE must match)
        if any_of:
            if not any(self._pattern_in_text(pattern, text) for pattern in any_of):
                return False

        # If no detection rules, don't match
        if not all_of and not any_of:
            return False

        return True

    def _pattern_in_text(self, pattern: str, text: str) -> bool:
        """Check if pattern exists in text (case-insensitive)."""
        return pattern.lower() in text.lower()

    def get_parser(self, text: str) -> Optional[FormatParser]:
        """
        Get a parser for the given text.

        Detects the format and returns an appropriate parser.

        Args:
            text: Invoice text to parse

        Returns:
            FormatParser instance, or None if no format matched
        """
        spec = self.detect_format(text)
        if spec:
            return create_parser(spec)
        return None

    def get_parser_by_name(self, name: str) -> Optional[FormatParser]:
        """
        Get a parser for a specific format by name.

        Args:
            name: Format name (e.g., 'amazon', 'absolute')

        Returns:
            FormatParser instance, or None if format not found
        """
        spec = self.formats_by_name.get(name)
        if spec:
            return create_parser(spec)
        return None

    def list_formats(self) -> List[str]:
        """Return list of available format names."""
        return list(self.formats_by_name.keys())

    def get_format_info(self, name: str) -> Optional[Dict]:
        """
        Get information about a specific format.

        Args:
            name: Format name

        Returns:
            Dict with format info (name, description, detection rules)
        """
        spec = self.formats_by_name.get(name)
        if not spec:
            return None

        return {
            'name': spec.get('name'),
            'description': spec.get('description'),
            'version': spec.get('version'),
            'detect': spec.get('detect'),
            'source_file': spec.get('_source_file'),
        }

    def register_spec(self, spec: Dict, source_label: str = 'auto') -> bool:
        """
        Hot-load a format spec into the running registry.

        Used by auto_format to inject LLM-generated specs without
        re-scanning the filesystem.

        Args:
            spec: Format specification dict (same structure as YAML)
            source_label: 'auto' or 'main'

        Returns:
            True if registered, False if duplicate name exists
        """
        name = spec.get('name', 'unknown')
        if name in self.formats_by_name:
            logger.debug(f"Spec '{name}' already registered, skipping")
            return False

        spec['_source_dir'] = source_label
        if 'priority' not in spec:
            spec['priority'] = -10 if source_label == 'auto' else 0

        self.formats.append(spec)
        self.formats_by_name[name] = spec
        self.formats.sort(key=lambda x: (-x.get('priority', 0), x.get('name', '')))

        logger.info(f"Hot-loaded format spec: {name} ({source_label})")
        return True


# Module-level registry instance (lazy initialization)
_registry: Optional[FormatRegistry] = None


def get_registry(base_dir: str = None) -> FormatRegistry:
    """
    Get the global format registry instance.

    Creates the registry on first call, reuses it on subsequent calls.

    Args:
        base_dir: Optional project base directory

    Returns:
        FormatRegistry instance
    """
    global _registry
    if _registry is None:
        _registry = FormatRegistry(base_dir)
    return _registry


def parse_with_format(text: str, base_dir: str = None) -> Optional[Dict]:
    """
    Convenience function to parse text using auto-detected format.

    Args:
        text: Invoice text to parse
        base_dir: Optional project base directory

    Returns:
        Parsed result dict, or None if no format matched
    """
    registry = get_registry(base_dir)
    parser = registry.get_parser(text)

    if parser:
        return parser.parse(text)

    return None


if __name__ == '__main__':
    # Quick test
    import sys

    logging.basicConfig(level=logging.DEBUG)

    registry = FormatRegistry()
    print(f"Available formats: {registry.list_formats()}")

    for name in registry.list_formats():
        info = registry.get_format_info(name)
        print(f"\n{name}:")
        print(f"  Description: {info.get('description')}")
        print(f"  Version: {info.get('version')}")
