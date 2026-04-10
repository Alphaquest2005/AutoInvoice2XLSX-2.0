"""SMTP email sender adapter."""

from __future__ import annotations

import logging
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from autoinvoice.domain.models.email import EmailMessage

logger = logging.getLogger(__name__)


class SmtpEmailSender:
    """Sends emails via SMTP."""

    def __init__(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        use_ssl: bool = True,
    ) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._use_ssl = use_ssl

    def send(self, message: EmailMessage) -> bool:
        """Connect to SMTP and send the email with attachments.

        Returns True on success, False on failure.
        """
        try:
            msg = self._build_mime(message)
            if self._use_ssl:
                with smtplib.SMTP_SSL(self._host, self._port) as server:
                    server.login(self._username, self._password)
                    server.send_message(msg)
            else:
                with smtplib.SMTP(self._host, self._port) as server:
                    server.starttls()
                    server.login(self._username, self._password)
                    server.send_message(msg)
            return True
        except Exception:
            logger.exception("Failed to send email")
            return False

    def fetch_unread(self, folder: str = "INBOX") -> list[EmailMessage]:
        """Not supported by sender-only adapter."""
        raise NotImplementedError("SmtpEmailSender does not support fetching.")

    def mark_read(self, message_id: str) -> None:
        """Not supported by sender-only adapter."""
        raise NotImplementedError("SmtpEmailSender does not support mark_read.")

    @staticmethod
    def _build_mime(message: EmailMessage) -> MIMEMultipart:
        """Build a MIME message from our domain EmailMessage."""
        mime = MIMEMultipart()
        mime["Subject"] = message.subject
        mime["From"] = message.from_address
        mime["To"] = ", ".join(message.to_addresses)

        if message.is_html:
            mime.attach(MIMEText(message.body, "html"))
        else:
            mime.attach(MIMEText(message.body, "plain"))

        for att in message.attachments:
            part = MIMEApplication(att.data, Name=att.filename)
            part["Content-Disposition"] = f'attachment; filename="{att.filename}"'
            mime.attach(part)

        return mime
