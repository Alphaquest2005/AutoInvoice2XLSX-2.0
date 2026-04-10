"""Filesystem adapter using standard library os/shutil/pathlib."""

from __future__ import annotations

import os
import shutil
from pathlib import Path


class OsFileSystem:
    """Implements ``FileSystemPort`` using standard ``os`` and ``shutil`` operations."""

    # -- text I/O -----------------------------------------------------------

    def read_text(self, path: str) -> str:
        """Read a file as UTF-8 text."""
        return Path(path).read_text(encoding="utf-8")

    def write_text(self, path: str, content: str) -> None:
        """Write UTF-8 text to a file, creating or overwriting it."""
        Path(path).write_text(content, encoding="utf-8")

    # -- binary I/O ---------------------------------------------------------

    def read_bytes(self, path: str) -> bytes:
        """Read a file as raw bytes."""
        return Path(path).read_bytes()

    def write_bytes(self, path: str, data: bytes) -> None:
        """Write raw bytes to a file, creating or overwriting it."""
        Path(path).write_bytes(data)

    # -- queries ------------------------------------------------------------

    def exists(self, path: str) -> bool:
        """Check whether a path exists."""
        return Path(path).exists()

    def list_dir(self, path: str) -> list[str]:
        """List entries in a directory."""
        return os.listdir(path)

    # -- mutations ----------------------------------------------------------

    def mkdir(self, path: str) -> None:
        """Create a directory, including any missing parents."""
        Path(path).mkdir(parents=True, exist_ok=True)

    def copy(self, src: str, dst: str) -> None:
        """Copy a file from *src* to *dst*, preserving metadata."""
        shutil.copy2(src, dst)

    def delete(self, path: str) -> None:
        """Delete a file or directory tree."""
        p = Path(path)
        if p.is_dir():
            shutil.rmtree(path)
        else:
            p.unlink()
