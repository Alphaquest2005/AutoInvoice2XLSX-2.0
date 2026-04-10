"""
Email composition and sending - deterministic, no LLM involvement.

Email subjects and bodies are composed from frozen metadata only.
"""

import logging
import os
import smtplib
import ssl
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def compose_email(
    waybill: str,
    consignee_name: str = "Consignee Name Not Found",
    consignee_code: str = "Consignee Code Not Found",
    consignee_address: str = "Consignee Address Not Found",
    total_invoices: int = 1,
    packages: str = "1",
    weight: str = "0",
    country_origin: str = "US",
    freight: str = "0",
    man_reg: str = None,
    attachment_paths: List[str] = None,
    location: str = "STG01",
    office: str = "GDSGO",
    expected_entries: int = 0,
) -> Dict:
    """
    Compose a shipment email from frozen metadata.

    This function is pure/deterministic - same inputs always produce same outputs.
    No LLM calls, no file reading, no side effects.

    Returns:
        dict with 'subject', 'body', 'attachments'
    """
    bl_number = waybill or 'UNKNOWN'
    if not man_reg:
        now = datetime.now()
        man_reg = f"{now.strftime('%Y')} {now.timetuple().tm_yday}"

    # Format freight with comma separators if numeric
    try:
        freight_val = float(str(freight).replace(',', ''))
        freight_display = f"{freight_val:,.2f}"
    except (ValueError, TypeError):
        freight_display = freight

    lines = [
        f"Expected Entries: {expected_entries or total_invoices}",
        f"Manifest: {man_reg}",
        "",
        f"Consignee Code: {consignee_code}",
        "",
        f"Consignee Name: {consignee_name}",
        "",
        f"Consignee Address: {consignee_address}",
        "",
        f"BL: {bl_number}",
        "",
        f"Freight: {freight_display}",
        "",
        f"Weight(kg): {weight}",
        "",
        "Currency: USD",
        "",
        f"Country of Origin: {country_origin}",
        "",
        f"Total Invoices: {total_invoices}",
        "",
        f"Packages: {packages}",
        "",
        "Freight Currency: US",
        "",
        f"Location of Goods: {location}",
        "",
        f"Office: {office}",
    ]

    # Filter attachments to only existing files
    attachments = [p for p in (attachment_paths or []) if p and os.path.exists(p)]

    return {
        'subject': f"Shipment: {bl_number}",
        'body': '\n'.join(lines),
        'attachments': attachments,
        'bl_number': bl_number,
    }


def send_email(subject: str, body: str, attachments: List[str]) -> bool:
    """
    Send an email with attachments via SMTP SSL.

    Returns True on success, False on failure.
    """
    from core.config import get_config
    cfg = get_config()

    if not cfg.email_password:
        logger.error("No email password configured")
        return False

    try:
        msg = MIMEMultipart()
        msg['From'] = f"{cfg.email_sender_name} <{cfg.email_sender}>"
        msg['To'] = cfg.email_recipient
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        for file_path in attachments:
            if os.path.exists(file_path):
                with open(file_path, 'rb') as f:
                    part = MIMEBase('application', 'octet-stream')
                    part.set_payload(f.read())
                encoders.encode_base64(part)
                filename = os.path.basename(file_path)
                part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
                msg.attach(part)

        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(cfg.smtp_host, cfg.smtp_port, context=context) as server:
            server.login(cfg.email_sender, cfg.email_password)
            server.send_message(msg)

        return True

    except Exception as e:
        logger.error(f"Email send error: {e}")
        return False
