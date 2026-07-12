from __future__ import annotations

import io
import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, urlsplit

import fsspec
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.ipc as ipc
import pyarrow.json as pajson
import pyarrow.parquet as pq

from .spec import AbstractDatabase, _validate_open_mode

_FORMATS = {"arrow", "parquet", "csv", "jsonl", "sql"}
_FACETS = {"columns", "indexes", "constraints", "depends_on"}


@dataclass(frozen=True)
class _Path:
    schema: str | None
    relation: str | None
    kind: str
    item: str | None = None
    format: str | None = None
    query: tuple[tuple[str, str], ...] = ()


class _WriteBuffer(io.BytesIO):
    def __init__(self, commit: Any) -> None:
        super().__init__()
        self._commit = commit
        self._commit_enabled = True

    def close(self) -> None:
        if not self.closed and self._commit_enabled:
            self._commit(self.getvalue())
        super().close()

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        if exc_type is not None:
            self._commit_enabled = False
        self.close()

    def __del__(self) -> None:
        self._commit_enabled = False
        super().close()


class PyDatabaseFileSystem(fsspec.AbstractFileSystem):
    """Direct Python filesystem adapter for an :class:`AbstractDatabase`.

    Unlike ``AbstractDatabaseFileSystem``, this class does not call the Rust bridge. It is
    primarily useful for Python-only backend implementations and conformance testing.
    """

    protocol = "db+python"
    root_marker = "/"

    def __init__(self, db: AbstractDatabase, **storage_options: Any) -> None:
        super().__init__(**storage_options)
        self.db = db

    def ls(self, path: str, detail: bool = True, **kwargs: Any) -> list[dict[str, Any]] | list[str]:
        parsed = _parse_path(path)
        if parsed.kind == "root":
            entries = [_schema_info(info) for info in self.db.list_schemas()]
        elif parsed.kind == "schema":
            self._ensure_schema(parsed.schema)
            entries = [_relation_info(parsed.schema, info) for info in self.db.list_relations(parsed.schema)]
        elif parsed.kind == "relation":
            relation = self.db.relation_info(parsed.schema, parsed.relation)
            entries = [_directory(f"/{parsed.schema}/{parsed.relation}/{facet}") for facet in ("columns", "indexes", "constraints")]
            if relation.kind == "view":
                entries.extend(
                    [
                        _directory(f"/{parsed.schema}/{parsed.relation}/depends_on"),
                        _file(f"/{parsed.schema}/{parsed.relation}/definition.sql", 0),
                    ]
                )
        elif parsed.kind == "facet":
            entries = self._facet_entries(parsed)
        else:
            raise NotADirectoryError(path)
        return entries if detail else [entry["name"] for entry in entries]

    def info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        parsed = _parse_path(path)
        if parsed.kind == "root":
            return _directory("/")
        if parsed.kind == "schema":
            return _schema_info(self._ensure_schema(parsed.schema))
        if parsed.kind == "relation":
            return _relation_info(parsed.schema, self.db.relation_info(parsed.schema, parsed.relation))
        if parsed.kind == "facet" and parsed.item is None:
            self.db.relation_info(parsed.schema, parsed.relation)
            return _directory(f"/{parsed.schema}/{parsed.relation}/{parsed.format}")
        if parsed.kind == "facet":
            entries = {entry["name"].rsplit("/", 1)[-1]: entry for entry in self._facet_entries(parsed)}
            try:
                return entries[parsed.item]
            except KeyError as exc:
                raise FileNotFoundError(path) from exc
        if parsed.kind == "definition":
            definition = self.db.view_definition(parsed.schema, parsed.relation)
            return _file(f"/{parsed.schema}/{parsed.relation}/definition.sql", len(definition.encode()))
        if parsed.kind == "data":
            relation = self.db.relation_info(parsed.schema, parsed.relation)
            return _file(
                f"/{parsed.schema}/{parsed.relation}.{parsed.format}",
                relation.size_bytes or 0,
                kind=relation.kind,
                row_count=relation.row_count,
                size_known=relation.size_bytes is not None,
            )
        raise FileNotFoundError(path)

    def cat_file(self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any) -> bytes:
        data = self._read(path)
        return data[slice(start, end)]

    def query(self, sql: str, params: list[Any] | None = None) -> pa.Table:
        return self.db.query(sql, params)

    def open_query(self, sql: str, params: list[Any] | None = None) -> io.BytesIO:
        return io.BytesIO(_encode(self.query(sql, params), "arrow"))

    def pipe_file(self, path: str, value: bytes, mode: str = "overwrite", **kwargs: Any) -> None:
        if mode == "create" and self.exists(path):
            raise FileExistsError(path)
        self._write(path, bytes(value), "append" if mode == "append" else "truncate")

    def put_file(self, lpath: str, rpath: str, callback: Any = None, mode: str = "overwrite", **kwargs: Any) -> None:
        with open(lpath, "rb") as source:
            self.pipe_file(rpath, source.read(), mode=mode)

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        autocommit: bool = True,
        cache_options: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> io.BytesIO:
        _validate_open_mode(mode, autocommit)
        if mode.startswith("r"):
            return io.BytesIO(self._read(path))
        insert_mode = "append" if mode.startswith("a") else "truncate"
        return _WriteBuffer(lambda data: self._write(path, data, insert_mode))

    def _read(self, path: str) -> bytes:
        parsed = _parse_path(path)
        if parsed.kind == "definition":
            return self.db.view_definition(parsed.schema, parsed.relation).encode()
        if parsed.kind != "data":
            raise IsADirectoryError(path)
        if parsed.format == "sql":
            raise NotImplementedError("DDL rendering is not supported by the direct Python adapter")
        sql = _select_sql(self.db.dialect(), parsed)
        return _encode(self.db.query(sql), parsed.format)

    def _write(self, path: str, data: bytes, mode: str) -> int:
        parsed = _parse_path(path)
        if parsed.kind != "data" or parsed.format == "sql":
            raise ValueError(f"database writes require a relation data path: {path}")
        relation = self.db.relation_info(parsed.schema, parsed.relation)
        if relation.kind != "table":
            raise NotImplementedError("database writes require a table path")
        return self.db.insert(parsed.schema, parsed.relation, _decode(data, parsed.format), mode)

    def _ensure_schema(self, schema: str | None) -> Any:
        for info in self.db.list_schemas():
            if info.name == schema:
                return info
        raise FileNotFoundError(schema)

    def _facet_entries(self, parsed: _Path) -> list[dict[str, Any]]:
        base = f"/{parsed.schema}/{parsed.relation}/{parsed.format}"
        if parsed.format == "columns":
            return [
                _file(
                    f"{base}/{info.name}",
                    0,
                    data_type=info.data_type,
                    nullable=info.nullable,
                    default=info.default,
                    ordinal=info.ordinal,
                    primary_key=info.primary_key,
                    comment=info.comment,
                )
                for info in self.db.list_columns(parsed.schema, parsed.relation)
            ]
        if parsed.format == "indexes":
            return [
                _file(f"{base}/{info.name}", 0, columns=info.columns, unique=info.unique, method=info.method)
                for info in self.db.list_indexes(parsed.schema, parsed.relation)
            ]
        if parsed.format == "constraints":
            return [
                _file(
                    f"{base}/{info.name}",
                    0,
                    kind=info.kind,
                    columns=info.columns,
                    references=info.references,
                    expr=info.expr,
                )
                for info in self.db.list_constraints(parsed.schema, parsed.relation)
            ]
        definition = self.db.view_definition(parsed.schema, parsed.relation)
        names = _view_dependencies(definition)
        return [_file(f"{base}/{name}", 0, target=f"/{parsed.schema}/{name}") for name in names]


def _parse_path(path: str) -> _Path:
    parsed = urlsplit(path)
    clean = "/" + parsed.path.split("://", 1)[-1].strip("/")
    query = tuple(parse_qsl(parsed.query, keep_blank_values=True))
    if clean == "/":
        return _Path(None, None, "root", query=query)
    parts = clean.strip("/").split("/")
    if len(parts) == 1:
        return _Path(parts[0], None, "schema", query=query)
    schema, relation = parts[:2]
    if len(parts) == 2:
        name, dot, extension = relation.rpartition(".")
        if dot and extension in _FORMATS:
            return _Path(schema, name, "data", format=extension, query=query)
        return _Path(schema, relation, "relation", query=query)
    if len(parts) == 3 and parts[2] == "definition.sql":
        return _Path(schema, relation, "definition", query=query)
    if len(parts) in {3, 4} and parts[2] in _FACETS:
        return _Path(schema, relation, "facet", parts[3] if len(parts) == 4 else None, parts[2], query)
    raise ValueError(f"unsupported database path: {path}")


def _quote(dialect: str, name: str) -> str:
    quote = "`" if dialect == "mysql" else '"'
    return quote + name.replace(quote, quote * 2) + quote


def _select_sql(dialect: str, path: _Path) -> str:
    options = dict(path.query)
    columns = options.get("columns")
    projection = ", ".join(_quote(dialect, item) for item in columns.split(",")) if columns else "*"
    sql = f"SELECT {projection} FROM {_quote(dialect, path.schema)}.{_quote(dialect, path.relation)}"
    if where := options.get("where"):
        sql += f" WHERE {where}"
    if limit := options.get("limit"):
        if not limit.isdigit():
            raise ValueError("limit must be a non-negative integer")
        sql += f" LIMIT {limit}"
    return sql


def _encode(table: pa.Table, format: str) -> bytes:
    sink = pa.BufferOutputStream()
    if format == "arrow":
        with ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
    elif format == "parquet":
        pq.write_table(table, sink)
    elif format == "csv":
        pacsv.write_csv(table, sink)
    elif format == "jsonl":
        return b"".join(json.dumps(row, default=str).encode() + b"\n" for row in table.to_pylist())
    else:
        raise ValueError(f"unsupported data format: {format}")
    return sink.getvalue().to_pybytes()


def _decode(data: bytes, format: str) -> pa.Table:
    source = pa.BufferReader(data)
    if format == "arrow":
        return ipc.open_stream(source).read_all()
    if format == "parquet":
        return pq.read_table(source)
    if format == "csv":
        return pacsv.read_csv(source)
    if format == "jsonl":
        return pajson.read_json(source)
    raise ValueError(f"unsupported data format: {format}")


def _directory(name: str, **extra: Any) -> dict[str, Any]:
    return {"name": name, "size": 0, "type": "directory", **extra}


def _file(name: str, size: int, **extra: Any) -> dict[str, Any]:
    return {"name": name, "size": size, "type": "file", **extra}


def _schema_info(info: Any) -> dict[str, Any]:
    return _directory(f"/{info.name}", catalog=info.catalog, comment=info.comment)


def _relation_info(schema: str, info: Any) -> dict[str, Any]:
    return _directory(
        f"/{schema}/{info.name}",
        kind=info.kind,
        row_count=info.row_count,
        size_bytes=info.size_bytes,
        comment=info.comment,
    )


def _view_dependencies(definition: str) -> list[str]:
    import re

    return sorted(set(re.findall(r"(?i)\b(?:from|join)\s+(?:[\w\"`]+\.)?[\"`]?([\w]+)[\"`]?", definition)))


fsspec.register_implementation("db+python", PyDatabaseFileSystem, clobber=True)
