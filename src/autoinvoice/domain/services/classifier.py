"""Classifier domain service - pure functions for tariff code classification.

Ports the v1 rule-engine logic into pure, side-effect-free functions that
operate on frozen domain models and depend on ports (protocols) for external
data access.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from autoinvoice.domain.models.invoice import InvoiceItem
    from autoinvoice.domain.ports.code_repository import CodeRepositoryPort

from autoinvoice.domain.models.classification import (
    Classification,
    ClassificationResult,
    TariffCode,
)

# Threshold below which a classification is considered low-confidence.
LOW_CONFIDENCE_THRESHOLD = 0.7


def classify_item(
    item: InvoiceItem,
    rules: list[dict[str, Any]],
    code_repo: CodeRepositoryPort,
    assessed: dict[str, Any] | None = None,
) -> Classification | None:
    """Classify a single invoice item.

    Classification priority:
      1. Assessed customs entries (highest confidence - customs-verified)
      2. Rule-based pattern matching (sorted by priority descending)

    All codes are validated via *code_repo*; invalid codes are corrected
    when a known correction exists.

    Args:
        item: The invoice line item to classify.
        rules: Classification rule dicts (must contain 'patterns', 'code').
        code_repo: Port for code validation and correction.
        assessed: Optional dict mapping SKU -> assessment info with 'code',
            'category', and 'confidence' keys.

    Returns:
        A frozen Classification, or None if no rule matched.
    """
    # ── Layer 0: Assessed classifications (customs-verified) ──
    if assessed and item.sku and item.sku in assessed:
        entry = assessed[item.sku]
        raw_code = entry["code"]
        code = _validate_code(raw_code, code_repo)
        if code is not None:
            return Classification(
                item=item,
                tariff_code=TariffCode(code),
                confidence=entry.get("confidence", 0.97),
                source="assessed",
                category=entry.get("category", "ASSESSED"),
            )

    # ── Layer 1: Rule-based classification ──
    matched_rule = _match_rules(item.description, rules)
    if matched_rule is None:
        return None

    raw_code = matched_rule["code"]
    code = _validate_code(raw_code, code_repo)
    if code is None:
        return None

    return Classification(
        item=item,
        tariff_code=TariffCode(code),
        confidence=matched_rule.get("confidence", 0.80),
        source="rules",
        category=matched_rule.get("category", "PRODUCTS"),
    )


def classify_items(
    items: tuple[InvoiceItem, ...],
    rules: list[dict[str, Any]],
    code_repo: CodeRepositoryPort,
    assessed: dict[str, Any] | None = None,
) -> ClassificationResult:
    """Classify a batch of invoice items.

    Args:
        items: Tuple of invoice line items.
        rules: Classification rule dicts.
        code_repo: Port for code validation and correction.
        assessed: Optional SKU -> assessment mapping.

    Returns:
        A frozen ClassificationResult with counts for unclassified and
        low-confidence items.
    """
    classifications: list[Classification] = []
    unclassified = 0
    low_confidence = 0

    for item in items:
        result = classify_item(item, rules, code_repo, assessed=assessed)
        if result is None:
            unclassified += 1
        else:
            classifications.append(result)
            if result.confidence < LOW_CONFIDENCE_THRESHOLD:
                low_confidence += 1

    return ClassificationResult(
        classifications=tuple(classifications),
        unclassified_count=unclassified,
        low_confidence_count=low_confidence,
    )


# ── Internal helpers ─────────────────────────────────────────────────────────


def _match_rules(description: str, rules: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Find the best matching rule for a description.

    Rules are sorted by priority (descending). For each rule, ALL patterns
    are checked (ANY pattern match counts). Exclusion patterns block the
    match entirely.

    Returns the first matching rule dict, or None.
    """
    norm_desc = _normalize_description(description)
    sorted_rules = sorted(rules, key=lambda r: r.get("priority", 0), reverse=True)

    for rule in sorted_rules:
        patterns: list[str] = rule.get("patterns", [])
        exclusions: list[str] = rule.get("exclude", [])

        # Check exclusion patterns first
        excluded = False
        for excl in exclusions:
            if excl.lower() in norm_desc:
                excluded = True
                break
        if excluded:
            continue

        # Check if ANY pattern matches
        for pattern in patterns:
            if pattern.lower() in norm_desc:
                return rule

    return None


def _normalize_description(description: str) -> str:
    """Lowercase and strip whitespace noise from a description."""
    if not description:
        return ""
    text = description.lower().strip()
    # Collapse multiple spaces
    text = re.sub(r"\s+", " ", text)
    return text


def _validate_code(raw_code: str, code_repo: CodeRepositoryPort) -> str | None:
    """Validate a tariff code, returning corrected code or None.

    1. If the code is already valid, return it.
    2. If a correction exists, return the corrected code.
    3. Otherwise return None (code cannot be used).
    """
    if code_repo.is_valid_code(raw_code):
        return raw_code

    correction = code_repo.get_correction(raw_code)
    if correction is not None and code_repo.is_valid_code(correction):
        return correction

    return None
