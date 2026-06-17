from __future__ import annotations

import io
from importlib import import_module
from typing import Any
from urllib.parse import quote, urlencode

import fsspec

from .spec import DBFile, _binary_mode

_rust = import_module(".fsspec_db", __package__)


class PostgresDatabaseFileSystem(fsspec.AbstractFileSystem):
    """PostgreSQL-backed fsspec filesystem registered as ``db+postgresql``."""

    protocol = ("db+postgresql", "db+postgres")
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
        if dsn in {"", "postgres://", "postgresql://"}:
            return {}
        return {"dsn": dsn}

    def __init__(self, dsn: str | None = None, **storage_options: Any) -> None:
        source = dsn or storage_options.pop("url", None) or _dsn_from_options(storage_options)
        if source is None:
            raise ValueError("PostgresDatabaseFileSystem requires a DSN or host config")
        super().__init__(**storage_options)
        self.dsn = source
        self._rust = _rust.RustPostgresDatabaseFs(source)

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> list[dict[str, Any]] | list[str]:
        return self._rust.ls(path, detail)

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        return self._rust.info(path)

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
        return self._rust.write_file(path, data, mode)

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
        with open(lpath, "rb") as file:
            self._write_file(rpath, file.read(), "ab" if mode == "append" else "wb")

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        autocommit: bool = True,
        cache_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> io.BytesIO | DBFile:
        if mode in {"xb", "x"}:
            raise NotImplementedError("exclusive create is not supported for database relation writes")
        if mode in {"wb", "w", "ab", "a"}:
            return DBFile(
                self,
                path,
                mode=_binary_mode(mode),
                block_size=block_size,
                autocommit=autocommit,
                cache_options=cache_options,
                **kwargs,
            )
        if mode not in {"rb", "r"}:
            raise NotImplementedError(f"database file mode is not supported: {mode}")
        return io.BytesIO(self._rust.cat_file(path, None, None))


def _dsn_from_url(path: str) -> str:
    prefixes = {
        "db+postgresql://": "postgresql://",
        "db+postgres://": "postgres://",
    }
    for source, target in prefixes.items():
        if path.startswith(source):
            return target + path.removeprefix(source)
    return path


def _dsn_from_options(options: dict[str, Any]) -> str | None:
    host = options.pop("host", None)
    if host is None:
        return None
    database = options.pop("database", None) or options.pop("dbname", None)
    user = options.pop("user", None) or options.pop("username", None)
    password = options.pop("password", None)
    port = options.pop("port", None)
    sslmode = options.pop("sslmode", None)

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
    query = f"?{urlencode({'sslmode': sslmode})}" if sslmode else ""
    return f"postgresql://{netloc}{path}{query}"


fsspec.register_implementation("db+postgresql", PostgresDatabaseFileSystem, clobber=True)
fsspec.register_implementation("db+postgres", PostgresDatabaseFileSystem, clobber=True)
