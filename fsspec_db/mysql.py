from __future__ import annotations

import io
from importlib import import_module
from typing import Any
from urllib.parse import quote, urlencode

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


class MySQLDatabaseFileSystem(DatabaseDdlMixin, IntrospectionCacheMixin, fsspec.AbstractFileSystem):
    """MySQL-backed fsspec filesystem registered as ``db+mysql``."""

    protocol = ("db+mysql",)
    root_marker = "/"

    @classmethod
    def _strip_protocol(cls, path: Any) -> Any:
        stripped = super()._strip_protocol(path)
        if isinstance(stripped, list):
            return [cls._strip_protocol(item) for item in stripped]
        return stripped

    @classmethod
    def _get_kwargs_from_urls(cls, path: str) -> dict[str, str]:
        dsn = _dsn_from_url(path)
        if dsn in {"", "mysql://"}:
            return {}
        return {"dsn": dsn}

    def __init__(self, dsn: str | None = None, **storage_options: Any) -> None:
        url = storage_options.pop("url", None)
        option_source = _dsn_from_options(storage_options)
        pool_options = _pool_options_from_options(storage_options)
        source = dsn or url or option_source
        if source is None:
            raise ValueError("MySQLDatabaseFileSystem requires a DSN or host config")
        super().__init__(**storage_options)
        self.dsn = source
        self.pool_options = pool_options
        self._rust = _rust.RustMySqlDatabaseFs(source, **pool_options)

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
        import pyarrow.ipc as ipc

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

    def put_file(
        self,
        lpath: str,
        rpath: str,
        callback: Any = None,
        mode: str = "overwrite",
        **kwargs: Any,
    ) -> None:
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


def _dsn_from_url(path: str) -> str:
    prefix = "db+mysql://"
    if path.startswith(prefix):
        return "mysql://" + path.removeprefix(prefix)
    return path


def _dsn_from_options(options: dict[str, Any]) -> str | None:
    host = options.pop("host", None)
    database = options.pop("database", None) or options.pop("dbname", None)
    user = options.pop("user", None) or options.pop("username", None)
    password = options.pop("password", None)
    port = options.pop("port", None)
    ssl_mode = options.pop("ssl_mode", None) or options.pop("sslmode", None)
    charset = options.pop("charset", None)
    if host is None:
        return None

    auth = ""
    if user is not None:
        auth = quote(str(user), safe="")
        if password is not None:
            auth += ":" + quote(str(password), safe="")
        auth += "@"
    netloc = auth + str(host)
    if port is not None:
        netloc += f":{port}"
    path = f"/{quote(str(database), safe='')}" if database else ""
    query_items = {}
    if ssl_mode:
        query_items["ssl-mode"] = ssl_mode
    if charset:
        query_items["charset"] = charset
    query = f"?{urlencode(query_items)}" if query_items else ""
    return f"mysql://{netloc}{path}{query}"


def _pool_options_from_options(options: dict[str, Any]) -> dict[str, int]:
    values: dict[str, int] = {}
    min_connections = _pop_first(options, "min_connections", "min_pool_size")
    max_connections = _pop_first(options, "max_connections", "max_pool_size")
    if min_connections is not None:
        values["min_connections"] = _non_negative_int("min_connections", min_connections)
    if max_connections is not None:
        values["max_connections"] = _positive_int("max_connections", max_connections)
    min_value = values.get("min_connections")
    max_value = values.get("max_connections")
    if min_value is not None and max_value is not None and min_value > max_value:
        raise ValueError("min_connections cannot exceed max_connections")
    return values


def _pop_first(options: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in options:
            return options.pop(key)
    return None


def _non_negative_int(name: str, value: Any) -> int:
    parsed = int(value)
    if parsed < 0:
        raise ValueError(f"{name} must be greater than or equal to 0")
    return parsed


def _positive_int(name: str, value: Any) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise ValueError(f"{name} must be greater than 0")
    return parsed


fsspec.register_implementation("db+mysql", MySQLDatabaseFileSystem, clobber=True)
