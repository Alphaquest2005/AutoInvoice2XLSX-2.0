"""Code validator domain service - pure functions for CET tariff code validation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from autoinvoice.domain.models.classification import TariffCode

if TYPE_CHECKING:
    from autoinvoice.domain.ports.code_repository import CodeRepositoryPort


def is_end_node_code(code: str) -> bool:
    """Check if code is an 8-digit end-node (not a heading/chapter).

    Args:
        code: Tariff code string to check.

    Returns:
        True if code is exactly 8 digits.
    """
    return len(code) == 8 and code.isdigit()


def validate_cet_code(code: str, code_repo: CodeRepositoryPort) -> bool:
    """Check if code is a valid 8-digit CET end-node in the repository.

    Args:
        code: Tariff code string to validate.
        code_repo: Repository port for code lookups.

    Returns:
        True if the code is a valid 8-digit end-node present in the repo.
    """
    if not is_end_node_code(code):
        return False
    return code_repo.is_valid_code(code)


def fix_invalid_code(
    code: str,
    code_repo: CodeRepositoryPort,
) -> TariffCode | None:
    """Validate a code, correct it if needed, and return a TariffCode or None.

    If the code is already valid, returns it as a TariffCode.
    If invalid, attempts correction via the repository.
    If no correction is found, returns None.

    Args:
        code: Tariff code string to validate/fix.
        code_repo: Repository port for code lookups and corrections.

    Returns:
        A valid TariffCode, or None if the code cannot be resolved.
    """
    if validate_cet_code(code, code_repo):
        return TariffCode(code)

    corrected = code_repo.get_correction(code)
    if corrected is not None and validate_cet_code(corrected, code_repo):
        return TariffCode(corrected)

    return None
