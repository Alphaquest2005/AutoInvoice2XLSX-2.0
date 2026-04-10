"""IMAP email reader adapter."""

from __future__ import annotations

import email
import imaplib
from email import policy
from typing import cast

from autoinvoice.domain.models.email import EmailAttachment, EmailMessage


def _bytes_payload(part: email.message.Message) -> bytes:
    """Extract decoded payload as bytes, defaulting to empty bytes."""
    raw = part.get_payload(decode=True)
    if isinstance(raw, bytes):
        return raw
    return b""


class ImapEmailReader:
    """Reads unread emails from an IMAP mailbox."""

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

    def fetch_unread(self, folder: str = "INBOX") -> list[EmailMessage]:
        """Connect to IMAP, fetch UNSEEN messages, parse to EmailMessage."""
        conn = self._connect()
        try:
            conn.select(folder)
            _status, data = conn.search(None, "UNSEEN")
            message_ids = data[0].split() if data[0] else []

            messages: list[EmailMessage] = []
            for mid in message_ids:
                _status, msg_data = conn.fetch(mid, "(RFC822)")
                if msg_data[0] is None:
                    continue
                raw = cast("bytes", msg_data[0][1])
                parsed = email.message_from_bytes(raw, policy=policy.default)
                messages.append(self._parse_message(parsed))
            return messages
        finally:
            conn.logout()

    def mark_read(self, message_id: str) -> None:
        """Mark a message as seen by message ID."""
        conn = self._connect()
        try:
            conn.select("INBOX")
            _status, data = conn.search(None, f'HEADER Message-ID "{message_id}"')
            msg_ids = data[0].split() if data[0] else []
            for mid in msg_ids:
                conn.store(mid, "+FLAGS", "\\Seen")
        finally:
            conn.logout()

    def _connect(self) -> imaplib.IMAP4:
        """Create and authenticate an IMAP connection."""
        conn: imaplib.IMAP4
        if self._use_ssl:
            conn = imaplib.IMAP4_SSL(self._host, self._port)
        else:
            conn = imaplib.IMAP4(self._host, self._port)
        conn.login(self._username, self._password)
        return conn

    @staticmethod
    def _parse_message(msg: email.message.Message) -> EmailMessage:
        """Parse a stdlib email.message.Message into our domain EmailMessage."""
        subject = str(msg.get("Subject", ""))
        from_addr = str(msg.get("From", ""))
        to_raw = str(msg.get("To", ""))
        to_addresses = tuple(addr.strip() for addr in to_raw.split(",") if addr.strip())
        message_id = str(msg.get("Message-ID", ""))

        body = ""
        is_html = False
        attachments: list[EmailAttachment] = []

        if msg.is_multipart():
            for part in msg.walk():
                content_type = part.get_content_type()
                disposition = str(part.get("Content-Disposition", ""))

                if "attachment" in disposition:
                    payload_bytes = _bytes_payload(part)
                    attachments.append(
                        EmailAttachment(
                            filename=part.get_filename() or "",
                            content_type=content_type,
                            data=payload_bytes,
                            size=len(payload_bytes),
                        )
                    )
                elif content_type == "text/plain" and not body:
                    body = _bytes_payload(part).decode("utf-8", errors="replace")
                elif content_type == "text/html" and not body:
                    body = _bytes_payload(part).decode("utf-8", errors="replace")
                    is_html = True
        else:
            payload_bytes = _bytes_payload(msg)
            body = payload_bytes.decode("utf-8", errors="replace") if payload_bytes else ""
            is_html = msg.get_content_type() == "text/html"

        return EmailMessage(
            subject=subject,
            body=body,
            to_addresses=to_addresses,
            from_address=from_addr,
            message_id=message_id,
            attachments=tuple(attachments),
            is_html=is_html,
        )
