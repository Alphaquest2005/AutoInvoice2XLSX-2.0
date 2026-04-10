"""Tests for email adapter classes."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from autoinvoice.adapters.email.email_gateway import CompositeEmailGateway
from autoinvoice.adapters.email.imap_reader import ImapEmailReader
from autoinvoice.adapters.email.smtp_sender import SmtpEmailSender
from autoinvoice.domain.models.email import EmailAttachment, EmailMessage


def _make_message(**overrides: object) -> EmailMessage:
    """Create an EmailMessage with sensible defaults."""
    defaults = {
        "subject": "Test Subject",
        "body": "Test body",
        "to_addresses": ("recipient@example.com",),
        "from_address": "sender@example.com",
    }
    defaults.update(overrides)
    return EmailMessage(**defaults)  # type: ignore[arg-type]


class TestSmtpEmailSender:
    """Tests for the SmtpEmailSender adapter."""

    @patch("autoinvoice.adapters.email.smtp_sender.smtplib")
    def test_send_calls_smtplib(self, mock_smtplib: MagicMock) -> None:
        mock_server = MagicMock()
        mock_smtplib.SMTP_SSL.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtplib.SMTP_SSL.return_value.__exit__ = MagicMock(return_value=False)

        sender = SmtpEmailSender(
            host="smtp.example.com",
            port=465,
            username="user",
            password="pass",
            use_ssl=True,
        )
        message = _make_message()
        result = sender.send(message)

        assert result is True
        mock_smtplib.SMTP_SSL.assert_called_once_with("smtp.example.com", 465)
        mock_server.login.assert_called_once_with("user", "pass")
        mock_server.send_message.assert_called_once()

    @patch("autoinvoice.adapters.email.smtp_sender.smtplib")
    def test_send_with_attachments(self, mock_smtplib: MagicMock) -> None:
        mock_server = MagicMock()
        mock_smtplib.SMTP_SSL.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtplib.SMTP_SSL.return_value.__exit__ = MagicMock(return_value=False)

        sender = SmtpEmailSender(
            host="smtp.example.com",
            port=465,
            username="user",
            password="pass",
        )
        message = _make_message(
            attachments=(
                EmailAttachment(
                    filename="test.pdf",
                    content_type="application/pdf",
                    data=b"fake-pdf",
                    size=8,
                ),
            )
        )
        result = sender.send(message)
        assert result is True

    @patch("autoinvoice.adapters.email.smtp_sender.smtplib")
    def test_handles_connection_error(self, mock_smtplib: MagicMock) -> None:
        mock_smtplib.SMTP_SSL.side_effect = ConnectionError("refused")

        sender = SmtpEmailSender(
            host="smtp.example.com",
            port=465,
            username="user",
            password="pass",
        )
        result = sender.send(_make_message())

        assert result is False

    def test_fetch_unread_raises_not_implemented(self) -> None:
        sender = SmtpEmailSender(host="smtp.example.com", port=465, username="u", password="p")
        with pytest.raises(NotImplementedError):
            sender.fetch_unread()

    def test_mark_read_raises_not_implemented(self) -> None:
        sender = SmtpEmailSender(host="smtp.example.com", port=465, username="u", password="p")
        with pytest.raises(NotImplementedError):
            sender.mark_read("msg-123")


class TestCompositeEmailGateway:
    """Tests for the CompositeEmailGateway adapter."""

    def test_delegates_send_to_smtp(self) -> None:
        reader = MagicMock(spec=ImapEmailReader)
        sender = MagicMock(spec=SmtpEmailSender)
        sender.send.return_value = True

        gateway = CompositeEmailGateway(imap_reader=reader, smtp_sender=sender)
        message = _make_message()
        result = gateway.send(message)

        assert result is True
        sender.send.assert_called_once_with(message)
        reader.send.assert_not_called() if hasattr(reader, "send") else None

    def test_delegates_fetch_to_imap(self) -> None:
        reader = MagicMock(spec=ImapEmailReader)
        sender = MagicMock(spec=SmtpEmailSender)
        expected = [_make_message()]
        reader.fetch_unread.return_value = expected

        gateway = CompositeEmailGateway(imap_reader=reader, smtp_sender=sender)
        result = gateway.fetch_unread("INBOX")

        assert result == expected
        reader.fetch_unread.assert_called_once_with("INBOX")

    def test_delegates_mark_read_to_imap(self) -> None:
        reader = MagicMock(spec=ImapEmailReader)
        sender = MagicMock(spec=SmtpEmailSender)

        gateway = CompositeEmailGateway(imap_reader=reader, smtp_sender=sender)
        gateway.mark_read("msg-456")

        reader.mark_read.assert_called_once_with("msg-456")
