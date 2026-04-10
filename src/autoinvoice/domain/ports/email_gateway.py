"""Port for email sending and retrieval."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from autoinvoice.domain.models.email import EmailMessage


class EmailGatewayPort(Protocol):
    """Interface for sending and fetching email messages."""

    def send(self, message: EmailMessage) -> bool:
        """Send an email message.

        Args:
            message: The email message to send.

        Returns:
            True if the message was sent successfully.
        """
        ...

    def fetch_unread(self, folder: str = "INBOX") -> list[EmailMessage]:
        """Fetch unread messages from a mailbox folder.

        Args:
            folder: Mailbox folder name.

        Returns:
            List of unread email messages.
        """
        ...

    def mark_read(self, message_id: str) -> None:
        """Mark a message as read.

        Args:
            message_id: Unique identifier of the message.
        """
        ...
