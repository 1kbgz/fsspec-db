from __future__ import annotations

import abc
import io
from importlib import import_module
from typing import Any

import fsspec

_rust = import_module(".fsspec_db", __package__)

SchemaInfo = _rust.SchemaInfo
RelationInfo = _rust.RelationInfo
ColumnInfo = _rust.ColumnInfo
IndexInfo = _rust.IndexInfo
ConstraintInfo = _rust.ConstraintInfo
DBFile = _rust.RustDbFile

_DEFAULT_COPY_BLOCK_SIZE = 1024 * 1024
_SUPPORTED_OPEN_MODES = {"rb", "r", "wb", "w", "ab", "a"}
_WRITE_OPEN_MODES = {"wb", "w", "ab", "a"}


class AbstractDatabase(abc.ABC):
    """Database primitive contract for Python-defined backends.

    The method set mirrors the Rust ``Database`` trait, except for Rust-only default methods.
    Metadata methods return the exported ``*Info`` classes. ``query`` returns a
    ``pyarrow.Table``. ``insert`` receives a ``pyarrow.Table`` and returns the number of rows
    inserted.
    """

    @abc.abstractmethod
    def dialect(self) -> str:
        """Return ``generic``, ``sqlite``, ``postgres``, ``postgresql``, or ``mysql``."""
        raise NotImplementedError

    @abc.abstractmethod
    def list_schemas(self) -> list[SchemaInfo]:
        """Return all schemas visible to the backend."""
        raise NotImplementedError

    @abc.abstractmethod
    def list_relations(self, schema: str) -> list[RelationInfo]:
        """Return tables and views in ``schema``."""
        raise NotImplementedError

    @abc.abstractmethod
    def list_columns(self, schema: str, relation: str) -> list[ColumnInfo]:
        """Return columns for ``schema.relation``."""
        raise NotImplementedError

    @abc.abstractmethod
    def list_indexes(self, schema: str, relation: str) -> list[IndexInfo]:
        """Return indexes for ``schema.relation``."""
        raise NotImplementedError

    @abc.abstractmethod
    def list_constraints(self, schema: str, relation: str) -> list[ConstraintInfo]:
        """Return constraints for ``schema.relation``."""
        raise NotImplementedError

    @abc.abstractmethod
    def relation_info(self, schema: str, relation: str) -> RelationInfo:
        """Return metadata for one table or view."""
        raise NotImplementedError

    @abc.abstractmethod
    def view_definition(self, schema: str, view: str) -> str:
        """Return SQL text for one view."""
        raise NotImplementedError

    @abc.abstractmethod
    def query(self, sql: str, params: list[Any] | None = None) -> Any:
        """Return a ``pyarrow.Table`` for ``sql`` and optional scalar ``params``."""
        raise NotImplementedError

    @abc.abstractmethod
    def insert(self, schema: str, relation: str, table: Any, mode: str = "append") -> int:
        """Insert ``table`` into ``schema.relation`` with ``append`` or ``truncate`` mode."""
        raise NotImplementedError


class AbstractDatabaseFileSystem(fsspec.AbstractFileSystem):
    """fsspec filesystem adapter for an :class:`AbstractDatabase` implementation."""

    protocol = "db"
    root_marker = "/"

    def __init__(self, db: AbstractDatabase, **storage_options: Any) -> None:
        super().__init__(**storage_options)
        self.db = db
        self._rust = _rust.RustDatabaseFs(db)

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

    def put_file(self, lpath: str, rpath: str, callback: Any = None, mode: str = "overwrite", **kwargs: Any) -> None:
        if mode == "create" and self.exists(rpath):
            raise FileExistsError(rpath)
        _copy_local_to_rust_file(self._rust, lpath, rpath, "ab" if mode == "append" else "wb")

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
        return self._rust.open_file(path, _binary_mode(mode))


def _copy_local_to_rust_file(rust_fs: Any, lpath: str, rpath: str, mode: str) -> None:
    with open(lpath, "rb") as source, rust_fs.open_file(rpath, mode) as target:
        while True:
            chunk = source.read(_DEFAULT_COPY_BLOCK_SIZE)
            if not chunk:
                break
            target.write(chunk)


def _validate_open_mode(mode: str, autocommit: bool) -> None:
    if mode in {"xb", "x"}:
        raise NotImplementedError("exclusive create is not supported for database relation writes")
    if mode not in _SUPPORTED_OPEN_MODES:
        raise NotImplementedError(f"database file mode is not supported: {mode}")
    if not autocommit and mode in _WRITE_OPEN_MODES:
        raise NotImplementedError("autocommit=False is not supported for database relation writes")


def _binary_mode(mode: str) -> str:
    return {
        "r": "rb",
        "rb": "rb",
        "w": "wb",
        "wb": "wb",
        "a": "ab",
        "ab": "ab",
        "x": "xb",
        "xb": "xb",
    }[mode]
