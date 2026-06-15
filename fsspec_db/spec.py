from __future__ import annotations

import abc
import io
from importlib import import_module
from typing import Any

import fsspec
from fsspec.spec import AbstractBufferedFile

_rust = import_module(".fsspec_db", __package__)

SchemaInfo = _rust.SchemaInfo
RelationInfo = _rust.RelationInfo
ColumnInfo = _rust.ColumnInfo
IndexInfo = _rust.IndexInfo
ConstraintInfo = _rust.ConstraintInfo


class DBFile(AbstractBufferedFile):
    """Buffered fsspec file used for database relation writes."""

    def _initiate_upload(self) -> None:
        self._chunks: list[bytes] = []

    def _upload_chunk(self, final: bool = False) -> bool:
        self.buffer.seek(0)
        data = self.buffer.read()
        if data:
            self._chunks.append(data)
        if final:
            self.fs._write_file(self.path, b"".join(self._chunks), self.mode)
        return True


class AbstractDatabase(abc.ABC):
    """Minimal database contract used by :class:`AbstractDatabaseFileSystem`."""

    @abc.abstractmethod
    def dialect(self) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def list_schemas(self) -> list[SchemaInfo]:
        raise NotImplementedError

    @abc.abstractmethod
    def list_relations(self, schema: str) -> list[RelationInfo]:
        raise NotImplementedError

    @abc.abstractmethod
    def list_columns(self, schema: str, relation: str) -> list[ColumnInfo]:
        raise NotImplementedError

    @abc.abstractmethod
    def list_indexes(self, schema: str, relation: str) -> list[IndexInfo]:
        raise NotImplementedError

    @abc.abstractmethod
    def list_constraints(self, schema: str, relation: str) -> list[ConstraintInfo]:
        raise NotImplementedError

    @abc.abstractmethod
    def relation_info(self, schema: str, relation: str) -> RelationInfo:
        raise NotImplementedError

    @abc.abstractmethod
    def view_definition(self, schema: str, view: str) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def query(self, sql: str, params: list[Any] | None = None) -> Any:
        raise NotImplementedError

    @abc.abstractmethod
    def insert(self, schema: str, relation: str, table: Any, mode: str = "append") -> int:
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


def _binary_mode(mode: str) -> str:
    return {
        "w": "wb",
        "wb": "wb",
        "a": "ab",
        "ab": "ab",
        "x": "xb",
        "xb": "xb",
    }[mode]
