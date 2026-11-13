"""The database protocols are registered in libfsspec, so any consumer of the shared library can open them."""

from ctypes import CDLL, POINTER, Structure, byref, c_char_p, c_size_t, c_void_p, string_at
from pathlib import Path
from sys import platform

import fsspec_rs
import pytest
from fsspec_rs.fsspec_rs import protocols

import fsspec_db


class String(Structure):
    _fields_ = [("data", c_void_p), ("len", c_size_t)]


@pytest.fixture(scope="module")
def lib():
    if not fsspec_db.linked_to_libfsspec():
        pytest.skip("this build keeps its own registry")
    name = {"win32": "fsspec.dll", "darwin": "libfsspec.dylib"}.get(platform, "libfsspec.so")
    lib = CDLL(str(Path(fsspec_rs.get_library_dirs()[0]) / name))
    lib.fsspec_last_error_message.restype = c_char_p
    lib.fsspec_url_to_fs.argtypes = [c_char_p, c_size_t, c_void_p, POINTER(c_void_p), POINTER(String)]
    lib.fsspec_fs_release.argtypes = [c_void_p]
    lib.fsspec_string_free.argtypes = [String]
    return lib


def test_database_protocols_are_registered_in_the_shared_registry():
    assert fsspec_db.linked_to_libfsspec() == fsspec_rs.fsspec_rs.linked_to_libfsspec()
    assert {"db+sqlite", "db+postgres", "db+postgresql", "db+mysql"} <= set(protocols())


def test_libfsspec_opens_sqlite_urls(lib, tmp_path):
    url = f"db+sqlite://{tmp_path / 'registry.db'}/main".encode()
    fs = c_void_p()
    path = String()
    assert lib.fsspec_url_to_fs(url, len(url), None, byref(fs), byref(path)) == 0, lib.fsspec_last_error_message()
    assert string_at(path.data, path.len) == b"/main"
    lib.fsspec_string_free(path)
    lib.fsspec_fs_release(fs)
    assert (tmp_path / "registry.db").exists()
