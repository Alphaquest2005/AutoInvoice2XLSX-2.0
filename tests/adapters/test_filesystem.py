"""Tests for OsFileSystem adapter."""

from __future__ import annotations

import os

import pytest

from autoinvoice.adapters.storage.filesystem import OsFileSystem


@pytest.fixture()
def fs() -> OsFileSystem:
    return OsFileSystem()


# -- text I/O ---------------------------------------------------------------


@pytest.mark.integration
def test_write_and_read_text(fs: OsFileSystem, tmp_path: object) -> None:
    path = str(tmp_path / "hello.txt")  # type: ignore[operator]
    fs.write_text(path, "hello world")
    assert fs.read_text(path) == "hello world"


# -- binary I/O -------------------------------------------------------------


@pytest.mark.integration
def test_write_and_read_bytes(fs: OsFileSystem, tmp_path: object) -> None:
    path = str(tmp_path / "data.bin")  # type: ignore[operator]
    payload = b"\x00\x01\x02\xff"
    fs.write_bytes(path, payload)
    assert fs.read_bytes(path) == payload


# -- exists ------------------------------------------------------------------


@pytest.mark.integration
def test_exists_true_for_file(fs: OsFileSystem, tmp_path: object) -> None:
    path = str(tmp_path / "exists.txt")  # type: ignore[operator]
    fs.write_text(path, "x")
    assert fs.exists(path) is True


@pytest.mark.integration
def test_exists_false_for_missing(fs: OsFileSystem, tmp_path: object) -> None:
    path = str(tmp_path / "nope.txt")  # type: ignore[operator]
    assert fs.exists(path) is False


# -- list_dir ----------------------------------------------------------------


@pytest.mark.integration
def test_list_dir(fs: OsFileSystem, tmp_path: object) -> None:
    for name in ("a.txt", "b.txt", "c.txt"):
        fs.write_text(str(tmp_path / name), "")  # type: ignore[operator]
    listing = sorted(fs.list_dir(str(tmp_path)))
    assert listing == ["a.txt", "b.txt", "c.txt"]


# -- mkdir -------------------------------------------------------------------


@pytest.mark.integration
def test_mkdir_creates_nested(fs: OsFileSystem, tmp_path: object) -> None:
    nested = str(tmp_path / "a" / "b" / "c")  # type: ignore[operator]
    fs.mkdir(nested)
    assert os.path.isdir(nested)


# -- copy --------------------------------------------------------------------


@pytest.mark.integration
def test_copy_file(fs: OsFileSystem, tmp_path: object) -> None:
    src = str(tmp_path / "src.txt")  # type: ignore[operator]
    dst = str(tmp_path / "dst.txt")  # type: ignore[operator]
    fs.write_text(src, "copy me")
    fs.copy(src, dst)
    assert fs.read_text(dst) == "copy me"


# -- delete ------------------------------------------------------------------


@pytest.mark.integration
def test_delete_file(fs: OsFileSystem, tmp_path: object) -> None:
    path = str(tmp_path / "gone.txt")  # type: ignore[operator]
    fs.write_text(path, "bye")
    assert fs.exists(path) is True
    fs.delete(path)
    assert fs.exists(path) is False


@pytest.mark.integration
def test_delete_directory(fs: OsFileSystem, tmp_path: object) -> None:
    dir_path = str(tmp_path / "subdir")  # type: ignore[operator]
    fs.mkdir(dir_path)
    fs.write_text(os.path.join(dir_path, "inner.txt"), "x")
    fs.delete(dir_path)
    assert fs.exists(dir_path) is False
