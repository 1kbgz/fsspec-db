from __future__ import annotations

import io
from importlib import import_module
from typing import Any

import fsspec

from .spec import (
    DatabaseDdlMixin,
    DeferredDatabaseFile,
    IntrospectionCacheMixin,
    _binary_mode,
    _copy_local_to_rust_file,
    _validate_open_mode,
)

_rust = import_module(".fsspec_db", __package__)


class SQLiteDatabaseFileSystem(DatabaseDdlMixin, IntrospectionCacheMixin, fsspec.AbstractFileSystem):
    """SQLite-backed fsspec filesystem registered as ``db+sqlite``.

    Overwrite writes replace table contents; use append mode to preserve rows.
    """

    protocol = ("db+sqlite",)
    root_marker = "/"

    @classmethod
    def _strip_protocol(cls, path: Any) -> Any:
        stripped = super()._strip_protocol(path)
        if isinstance(stripped, list):
            return [cls._strip_protocol(item) for item in stripped]
        if stripped == "localhost":
            return cls.root_marker
        if stripped.startswith("localhost/"):
            return stripped.removeprefix("localhost")
        return stripped

    @classmethod
    def _get_kwargs_from_urls(cls, path: str) -> dict[str, str]:
        database = cls._strip_protocol(path)
        if database == cls.root_marker:
            return {}
        return {"database": database}

    def __init__(self, database: str | None = None, **storage_options: Any) -> None:
        source = database or storage_options.pop("path", None) or storage_options.pop("url", None)
        if source is None:
            raise ValueError("SQLiteDatabaseFileSystem requires a database path or URL")
        super().__init__(**storage_options)
        self.database = source
        self._rust = _rust.RustSqliteDatabaseFs(source)

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> list[dict[str, Any]] | list[str]:
        return self._cached_ls(path, detail, self._rust.ls, kwargs.get("refresh", False))

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        return self._cached_info(path, self._rust.info, kwargs.get("refresh", False))

    def cat_file(
        self,
        path: str,
        start: int | None = None,
        end: int | None = None,
        **kwargs: Any,
    ) -> bytes:
        return self._rust.cat_file(path, start, end)

    def query(self, sql: str, params: list[Any] | None = None) -> Any:
        from pyarrow import ipc

        with ipc.open_stream(self._rust.query_arrow(sql, params)) as reader:
            return reader.read_all()

    def open_query(self, sql: str, params: list[Any] | None = None) -> io.BytesIO:
        return io.BytesIO(self._rust.query_arrow(sql, params))

    def _write_file(self, path: str, data: bytes, mode: str) -> int:
        written = self._rust.write_file(path, data, mode)
        self.invalidate_cache(path)
        return written

    def pipe_file(self, path: str, value: bytes, mode: str = "overwrite", **kwargs: Any) -> None:
        if mode == "create" and self.exists(path):
            raise FileExistsError(path)
        self._write_file(path, bytes(value), "ab" if mode == "append" else "wb")

    def put_file(self, lpath: str, rpath: str, callback: Any = None, mode: str = "overwrite", **kwargs: Any) -> None:
        if mode == "create" and self.exists(rpath):
            raise FileExistsError(rpath)
        _copy_local_to_rust_file(self._rust, lpath, rpath, "ab" if mode == "append" else "wb", callback)
        self.invalidate_cache(rpath)

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        autocommit: bool = True,
        cache_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Any:
        _validate_open_mode(mode, autocommit)
        if mode in {"wb", "w", "ab", "a"}:
            self.invalidate_cache(path)
            if not autocommit:
                return DeferredDatabaseFile(lambda data: self._write_file(path, data, _binary_mode(mode)))
        return self._rust.open_file(path, _binary_mode(mode))


fsspec.register_implementation("db+sqlite", SQLiteDatabaseFileSystem, clobber=True)
