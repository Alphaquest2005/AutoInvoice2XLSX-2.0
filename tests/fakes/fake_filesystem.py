"""In-memory fake for FileSystemPort."""

from __future__ import annotations


class FakeFileSystem:
    """Fake file system backed by in-memory dicts."""

    def __init__(self) -> None:
        self._files: dict[str, str | bytes] = {}
        self._dirs: set[str] = set()

    def read_text(self, path: str) -> str:
        """Read a file as UTF-8 text from the in-memory store."""
        if path not in self._files:
            raise FileNotFoundError(f"No such file: {path}")
        content = self._files[path]
        if isinstance(content, bytes):
            return content.decode("utf-8")
        return content

    def write_text(self, path: str, content: str) -> None:
        """Write UTF-8 text to the in-memory store."""
        self._files[path] = content

    def read_bytes(self, path: str) -> bytes:
        """Read a file as raw bytes from the in-memory store."""
        if path not in self._files:
            raise FileNotFoundError(f"No such file: {path}")
        content = self._files[path]
        if isinstance(content, str):
            return content.encode("utf-8")
        return content

    def write_bytes(self, path: str, data: bytes) -> None:
        """Write raw bytes to the in-memory store."""
        self._files[path] = data

    def exists(self, path: str) -> bool:
        """Check whether a path exists as a file or directory."""
        return path in self._files or path in self._dirs

    def list_dir(self, path: str) -> list[str]:
        """List entry names in a directory by matching stored file prefixes."""
        prefix = path.rstrip("/") + "/"
        names: set[str] = set()
        for key in self._files:
            if key.startswith(prefix):
                relative = key[len(prefix) :]
                names.add(relative.split("/")[0])
        for d in self._dirs:
            if d.startswith(prefix):
                relative = d[len(prefix) :]
                part = relative.split("/")[0]
                if part:
                    names.add(part)
        return sorted(names)

    def mkdir(self, path: str) -> None:
        """Create a directory and all parent directories."""
        parts = path.rstrip("/").split("/")
        for i in range(1, len(parts) + 1):
            self._dirs.add("/".join(parts[:i]))

    def copy(self, src: str, dst: str) -> None:
        """Copy a file from src to dst in the in-memory store."""
        if src not in self._files:
            raise FileNotFoundError(f"No such file: {src}")
        self._files[dst] = self._files[src]

    def delete(self, path: str) -> None:
        """Delete a file or directory from the in-memory store."""
        self._files.pop(path, None)
        self._dirs.discard(path)
