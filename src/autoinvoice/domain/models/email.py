"""Email domain models."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EmailAttachment:
    """File attached to an email."""

    filename: str
    content_type: str
    data: bytes
    size: int = 0


@dataclass(frozen=True)
class EmailMessage:
    """Email message for sending or receiving."""

    subject: str
    body: str
    to_addresses: tuple[str, ...]
    from_address: str = ""
    message_id: str = ""
    attachments: tuple[EmailAttachment, ...] = ()
    is_html: bool = False
