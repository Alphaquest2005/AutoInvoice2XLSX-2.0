"""Composite email gateway adapter combining IMAP reader and SMTP sender."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from autoinvoice.adapters.email.imap_reader import ImapEmailReader
    from autoinvoice.adapters.email.smtp_sender import SmtpEmailSender
    from autoinvoice.domain.models.email import EmailMessage


class CompositeEmailGateway:
    """Combines ImapEmailReader (receiving) and SmtpEmailSender (sending).

    Implements the full EmailGatewayPort protocol.
    """

    def __init__(
        self,
        imap_reader: ImapEmailReader,
        smtp_sender: SmtpEmailSender,
    ) -> None:
        self._reader = imap_reader
        self._sender = smtp_sender

    def send(self, message: EmailMessage) -> bool:
        """Delegate sending to the SMTP sender."""
        return self._sender.send(message)

    def fetch_unread(self, folder: str = "INBOX") -> list[EmailMessage]:
        """Delegate fetching to the IMAP reader."""
        return self._reader.fetch_unread(folder)

    def mark_read(self, message_id: str) -> None:
        """Delegate mark-read to the IMAP reader."""
        self._reader.mark_read(message_id)
