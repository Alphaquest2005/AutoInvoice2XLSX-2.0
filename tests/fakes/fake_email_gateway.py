"""In-memory fake for EmailGatewayPort."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from autoinvoice.domain.models.email import EmailMessage


class FakeEmailGateway:
    """Fake email gateway that records sent messages and serves a configurable inbox."""

    def __init__(self, inbox: list[EmailMessage] | None = None) -> None:
        self.sent: list[EmailMessage] = []
        self._inbox: list[EmailMessage] = list(inbox) if inbox else []
        self.marked_read: list[str] = []

    def send(self, message: EmailMessage) -> bool:
        """Record the sent message and return True."""
        self.sent.append(message)
        return True

    def fetch_unread(self, folder: str = "INBOX") -> list[EmailMessage]:
        """Return all messages currently in the fake inbox."""
        return list(self._inbox)

    def mark_read(self, message_id: str) -> None:
        """Track which message IDs were marked as read."""
        self.marked_read.append(message_id)
