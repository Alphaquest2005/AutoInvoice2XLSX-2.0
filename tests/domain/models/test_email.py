"""TDD tests for email domain models."""

from __future__ import annotations

import pytest

from autoinvoice.domain.models.email import EmailAttachment, EmailMessage


class TestEmailAttachment:
    def test_create_with_required_fields(self) -> None:
        att = EmailAttachment(
            filename="invoice.pdf",
            content_type="application/pdf",
            data=b"fake-pdf-bytes",
        )
        assert att.filename == "invoice.pdf"
        assert att.content_type == "application/pdf"
        assert att.data == b"fake-pdf-bytes"

    def test_default_size_is_zero(self) -> None:
        att = EmailAttachment(filename="x", content_type="text/plain", data=b"")
        assert att.size == 0

    def test_frozen_immutability(self) -> None:
        att = EmailAttachment(filename="x", content_type="text/plain", data=b"")
        with pytest.raises(AttributeError):
            att.filename = "y"  # type: ignore[misc]

    def test_equality_by_value(self) -> None:
        args = {"filename": "a.pdf", "content_type": "application/pdf", "data": b"abc", "size": 3}
        assert EmailAttachment(**args) == EmailAttachment(**args)


class TestEmailMessage:
    def test_create_with_required_fields(self) -> None:
        msg = EmailMessage(
            subject="Shipment docs",
            body="Please find attached.",
            to_addresses=("broker@example.com",),
        )
        assert msg.subject == "Shipment docs"
        assert msg.body == "Please find attached."
        assert msg.to_addresses == ("broker@example.com",)

    def test_defaults(self) -> None:
        msg = EmailMessage(subject="x", body="y", to_addresses=())
        assert msg.from_address == ""
        assert msg.message_id == ""
        assert msg.attachments == ()
        assert msg.is_html is False

    def test_frozen_immutability(self) -> None:
        msg = EmailMessage(subject="x", body="y", to_addresses=())
        with pytest.raises(AttributeError):
            msg.subject = "z"  # type: ignore[misc]

    def test_multiple_recipients(self) -> None:
        msg = EmailMessage(
            subject="x",
            body="y",
            to_addresses=("a@b.com", "c@d.com"),
        )
        assert len(msg.to_addresses) == 2

    def test_with_attachments(self) -> None:
        att = EmailAttachment(filename="doc.pdf", content_type="application/pdf", data=b"data")
        msg = EmailMessage(
            subject="x",
            body="y",
            to_addresses=("a@b.com",),
            attachments=(att,),
        )
        assert len(msg.attachments) == 1
        assert msg.attachments[0].filename == "doc.pdf"
