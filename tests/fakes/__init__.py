"""Test fakes for all domain ports - hand-written, no mocks."""

from __future__ import annotations

from tests.fakes.fake_classification_store import FakeClassificationStore
from tests.fakes.fake_code_repo import InMemoryCodeRepository
from tests.fakes.fake_config_provider import FakeConfigProvider
from tests.fakes.fake_email_gateway import FakeEmailGateway
from tests.fakes.fake_filesystem import FakeFileSystem
from tests.fakes.fake_llm_client import FakeLlmClient
from tests.fakes.fake_pdf_extractor import FakePdfExtractor
from tests.fakes.fake_xlsx_writer import FakeXlsxWriter

__all__ = [
    "FakeClassificationStore",
    "FakeConfigProvider",
    "FakeEmailGateway",
    "FakeFileSystem",
    "FakeLlmClient",
    "FakePdfExtractor",
    "FakeXlsxWriter",
    "InMemoryCodeRepository",
]
