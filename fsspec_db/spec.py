from __future__ import annotations

import abc
import io
import os
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
_INFO_CACHE_PREFIX = "__fsspec_db_info__:"


class IntrospectionCacheMixin:
    """Cache database listings and metadata using fsspec's bounded TTL cache."""

    def _cached_ls(self, path: str, detail: bool, loader: Any, refresh: bool = False) -> list[Any]:
        if refresh:
            self.invalidate_cache(path)
        key = path.rstrip("/") or self.root_marker
        entries = None
        if not refresh:
            try:
                entries = self.dircache[key]
            except KeyError:
                pass
        if entries is None:
            entries = loader(path, True)
            self.dircache[key] = entries
        return entries if detail else [entry["name"] for entry in entries]

    def _cached_info(self, path: str, loader: Any, refresh: bool = False) -> dict[str, Any]:
        if refresh:
            self.invalidate_cache(path)
        key = _INFO_CACHE_PREFIX + (path.rstrip("/") or self.root_marker)
        entries = None
        if not refresh:
            try:
                entries = self.dircache[key]
            except KeyError:
                pass
        if entries is None:
            entries = [loader(path)]
            self.dircache[key] = entries
        return entries[0]

    def invalidate_cache(self, path: str | None = None) -> None:
        self.dircache.clear()
        rust = getattr(self, "_rust", None)
        if rust is not None and hasattr(rust, "invalidate_cache"):
            rust.invalidate_cache()
        super().invalidate_cache(path)


class DatabaseDdlMixin:
    """Guarded database DDL mapped onto fsspec mutation methods."""

    def mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        self._require_ddl()
        schema_name, relation, extension = _ddl_path(path)
        if relation is None or extension is not None:
            raise ValueError("mkdir requires a /schema/relation path")
        sql = kwargs.get("sql")
        if sql is None:
            arrow_schema = kwargs.get("schema")
            if arrow_schema is None:
                raise ValueError("mkdir requires schema=pyarrow.Schema or sql=CREATE TABLE ...")
            columns = ", ".join(
                f"{_quote_ddl_identifier(self, field.name)} {_arrow_type_to_sql(field.type)}" + ("" if field.nullable else " NOT NULL")
                for field in arrow_schema
            )
            sql = f"CREATE TABLE {_quote_ddl_identifier(self, schema_name)}.{_quote_ddl_identifier(self, relation)} ({columns})"
        self.query(sql)
        self.invalidate_cache(path)

    def rm_file(self, path: str) -> None:
        self._require_ddl()
        schema, relation, extension = _ddl_path(path)
        if relation is None:
            raise ValueError("database removal requires a relation path")
        target = f"{_quote_ddl_identifier(self, schema)}.{_quote_ddl_identifier(self, relation)}"
        if extension is not None:
            self.query(f"DELETE FROM {target}")
        else:
            kind = self.info(path).get("kind", "table").upper()
            self.query(f"DROP {kind} {target}")
        self.invalidate_cache(path)

    def mv(self, path1: str, path2: str, recursive: bool = False, maxdepth: int | None = None, **kwargs: Any) -> None:
        self._require_ddl()
        source_schema, source, source_extension = _ddl_path(path1)
        target_schema, target, target_extension = _ddl_path(path2)
        if None in {source, target} or source_extension is not None or target_extension is not None:
            raise ValueError("database rename requires relation paths")
        if source_schema != target_schema:
            raise NotImplementedError("cross-schema relation moves are not supported")
        old = f"{_quote_ddl_identifier(self, source_schema)}.{_quote_ddl_identifier(self, source)}"
        if _ddl_dialect(self) == "mysql":
            new = f"{_quote_ddl_identifier(self, target_schema)}.{_quote_ddl_identifier(self, target)}"
            sql = f"RENAME TABLE {old} TO {new}"
        else:
            sql = f"ALTER TABLE {old} RENAME TO {_quote_ddl_identifier(self, target)}"
        self.query(sql)
        self.invalidate_cache(path1)

    def _require_ddl(self) -> None:
        if not self.storage_options.get("allow_ddl", False):
            raise PermissionError("database DDL is disabled; construct the filesystem with allow_ddl=True")


def _ddl_path(path: str) -> tuple[str, str | None, str | None]:
    clean = path.split("?", 1)[0].strip("/")
    parts = clean.split("/") if clean else []
    if len(parts) not in {1, 2}:
        raise ValueError(f"unsupported database DDL path: {path}")
    if len(parts) == 1:
        return parts[0], None, None
    relation, dot, extension = parts[1].rpartition(".")
    return parts[0], relation if dot else parts[1], extension if dot else None


def _ddl_dialect(fs: Any) -> str:
    db = getattr(fs, "db", None)
    if db is not None:
        return db.dialect()
    protocol = fs.protocol[0] if isinstance(fs.protocol, tuple) else fs.protocol
    return {"db+postgres": "postgres", "db+postgresql": "postgres", "db+mysql": "mysql"}.get(protocol, "generic")


def _quote_ddl_identifier(fs: Any, name: str) -> str:
    quote = "`" if _ddl_dialect(fs) == "mysql" else '"'
    return quote + name.replace(quote, quote * 2) + quote


def _arrow_type_to_sql(data_type: Any) -> str:
    import pyarrow as pa

    if pa.types.is_boolean(data_type):
        return "BOOLEAN"
    if pa.types.is_integer(data_type):
        return "BIGINT"
    if pa.types.is_floating(data_type):
        return "DOUBLE"
    if pa.types.is_decimal(data_type):
        return f"DECIMAL({data_type.precision}, {data_type.scale})"
    if pa.types.is_binary(data_type) or pa.types.is_large_binary(data_type):
        return "BLOB"
    if pa.types.is_date(data_type):
        return "DATE"
    if pa.types.is_timestamp(data_type):
        return "TIMESTAMP"
    if pa.types.is_string(data_type) or pa.types.is_large_string(data_type):
        return "VARCHAR"
    raise TypeError(f"unsupported Arrow type for database DDL: {data_type}")


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


class AbstractDatabaseFileSystem(DatabaseDdlMixin, IntrospectionCacheMixin, fsspec.AbstractFileSystem):
    """fsspec filesystem adapter for an :class:`AbstractDatabase` implementation."""

    protocol = "db"
    root_marker = "/"

    def __init__(self, db: AbstractDatabase, **storage_options: Any) -> None:
        super().__init__(**storage_options)
        self.db = db
        self._rust = _rust.RustDatabaseFs(db)

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
        if mode in _WRITE_OPEN_MODES:
            self.invalidate_cache(path)
            if not autocommit:
                return DeferredDatabaseFile(lambda data: self._write_file(path, data, _binary_mode(mode)))
        return self._rust.open_file(path, _binary_mode(mode))


class DeferredDatabaseFile(io.BytesIO):
    """Write buffer committed or discarded by ``fsspec.Transaction``."""

    def __init__(self, commit: Any) -> None:
        super().__init__()
        self._commit_callback = commit
        self._completed = False

    def close(self) -> None:
        if self._completed:
            super().close()

    def commit(self) -> None:
        if not self._completed:
            self._commit_callback(self.getvalue())
            self._completed = True
            super().close()

    def discard(self) -> None:
        if not self._completed:
            self._completed = True
            super().close()


def _copy_local_to_rust_file(rust_fs: Any, lpath: str, rpath: str, mode: str, callback: Any = None) -> None:
    if callback is not None:
        callback.set_size(os.path.getsize(lpath))
    with open(lpath, "rb") as source, rust_fs.open_file(rpath, mode) as target:
        while True:
            chunk = source.read(_DEFAULT_COPY_BLOCK_SIZE)
            if not chunk:
                break
            target.write(chunk)
            if callback is not None:
                callback.relative_update(len(chunk))


def _validate_open_mode(mode: str, autocommit: bool) -> None:
    if mode in {"xb", "x"}:
        raise NotImplementedError("exclusive create is not supported for database relation writes")
    if mode not in _SUPPORTED_OPEN_MODES:
        raise NotImplementedError(f"database file mode is not supported: {mode}")


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
