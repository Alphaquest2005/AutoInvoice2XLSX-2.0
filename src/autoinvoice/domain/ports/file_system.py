"""Port for file system operations."""

from __future__ import annotations

from typing import Protocol


class FileSystemPort(Protocol):
    """Interface for file system access, enabling testable I/O."""

    def read_text(self, path: str) -> str:
        """Read a file as UTF-8 text.

        Args:
            path: Absolute file path.

        Returns:
            File contents as a string.
        """
        ...

    def write_text(self, path: str, content: str) -> None:
        """Write UTF-8 text to a file, creating or overwriting it.

        Args:
            path: Absolute file path.
            content: Text content to write.
        """
        ...

    def read_bytes(self, path: str) -> bytes:
        """Read a file as raw bytes.

        Args:
            path: Absolute file path.

        Returns:
            File contents as bytes.
        """
        ...

    def write_bytes(self, path: str, data: bytes) -> None:
        """Write raw bytes to a file, creating or overwriting it.

        Args:
            path: Absolute file path.
            data: Byte data to write.
        """
        ...

    def exists(self, path: str) -> bool:
        """Check whether a path exists.

        Args:
            path: Absolute file path.

        Returns:
            True if the path exists.
        """
        ...

    def list_dir(self, path: str) -> list[str]:
        """List entries in a directory.

        Args:
            path: Absolute directory path.

        Returns:
            List of entry names (not full paths).
        """
        ...

    def mkdir(self, path: str) -> None:
        """Create a directory, including any missing parents.

        Args:
            path: Absolute directory path.
        """
        ...

    def copy(self, src: str, dst: str) -> None:
        """Copy a file from src to dst.

        Args:
            src: Source file path.
            dst: Destination file path.
        """
        ...

    def delete(self, path: str) -> None:
        """Delete a file or empty directory.

        Args:
            path: Absolute path to delete.
        """
        ...
