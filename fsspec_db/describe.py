"""Describes database filesystems to fsspec-data, so a data view can render their relations
and metadata as files."""

from __future__ import annotations

from typing import Any, BinaryIO

import pyarrow as pa
from fsspec_data import DataSource, Facet, Node
from pyarrow import ipc

# Kinds whose node holds the rows of a relation.
_RELATION_KINDS = {"table", "view"}
# Containers whose children carry the metadata, so their index is the useful rendering.
_CONTAINER_KINDS = {"columns", "indexes", "constraints", "depends_on"}
# Files whose bytes are the content: a view's SQL and a records-view row.
_RAW_KINDS = {"view_definition", "record"}
_INFO_FIELDS = {"name", "type", "size", "size_known"}


class DatabaseDataSource(DataSource):
    """``DataSource`` for the fsspec-db filesystems, built from the ``kind`` each ``info``
    entry reports. Relations and their ``records/`` directories are tables read through
    the ``.arrow`` encoding; columns, indexes, constraints, and dependencies are metadata;
    view definitions and records are raw bytes. Encoded aliases such as ``orders.parquet``
    are not listed, so a view does not show the same rows twice.

    Column metadata adds the column's ``arrow_type``. With the storage option
    ``column_statistics="exact"`` it also adds ``distinct_count``, ``null_count``, ``min``, and
    ``max``, computed with one aggregate query per relation that reads the whole table."""

    def describe(self, path: str) -> Node:
        return _node(self.info(path))

    def children(self, path: str) -> list[Node]:
        return [_node(entry) for entry in self.ls(path, detail=True)]

    def schema(self, path: str) -> pa.Schema:
        relation = _relation(self, path)
        cache = _cache(self, "schemas")
        if relation not in cache:
            with self.open(f"{relation}.arrow?limit=0", "rb") as file:
                schema = ipc.open_stream(file).schema
            if any(pa.types.is_null(field.type) for field in schema):
                # Backends that type results from their rows, such as SQLAlchemy, return
                # untyped columns for an empty read; the rows give the real types.
                with self.open(f"{relation}.arrow", "rb") as file:
                    schema = ipc.open_stream(file).schema
            cache[relation] = schema
        return cache[relation]

    def scan(
        self,
        path: str,
        *,
        batch_size: int = 1024,
        row_limit: int | None = None,
        byte_limit: int | None = None,
    ) -> pa.RecordBatchReader:
        del batch_size, row_limit, byte_limit
        return ipc.open_stream(self.open(f"{_relation(self, path)}.arrow", "rb"))

    def open_raw(self, path: str) -> BinaryIO:
        return self.open(path, "rb")

    def metadata(self, path: str) -> dict[str, Any]:
        info = self.info(path)
        metadata = {key: _json(value) for key, value in info.items() if key not in _INFO_FIELDS}
        if info.get("kind") == "column":
            relation, _, column = self._strip_protocol(path).rstrip("/").rpartition("/columns/")
            metadata["arrow_type"] = str(self.schema(relation).field(column).type)
            if self.storage_options.get("column_statistics") == "exact":
                metadata.update(self._column_statistics(relation)[column])
        return metadata

    def invalidate_cache(self, path: str | None = None) -> None:
        _cache(self, "schemas").clear()
        _cache(self, "statistics").clear()
        super().invalidate_cache(path)

    def _column_statistics(self, relation: str) -> dict[str, dict[str, Any]]:
        cache = _cache(self, "statistics")
        if relation not in cache:
            from .spec import _quote_ddl_identifier

            def quote(name: str) -> str:
                return _quote_ddl_identifier(self, name)

            columns = self.schema(relation).names
            keys = ("distinct_count", "null_count", "min", "max")
            aggregates = ", ".join(
                f"{expression} AS {quote(f'c{index}_{key}')}"
                for index, name in enumerate(columns)
                for key, expression in zip(
                    keys,
                    (
                        f"COUNT(DISTINCT {quote(name)})",
                        f"SUM(CASE WHEN {quote(name)} IS NULL THEN 1 ELSE 0 END)",
                        f"MIN({quote(name)})",
                        f"MAX({quote(name)})",
                    ),
                    strict=True,
                )
            )
            schema_name, _, table = relation.strip("/").partition("/")
            result = self.query(f"SELECT {aggregates} FROM {quote(schema_name)}.{quote(table)}")
            values = [column[0].as_py() for column in result.columns]
            cache[relation] = {
                name: {key: _json(value) for key, value in zip(keys, values[index * 4 : index * 4 + 4], strict=True)}
                for index, name in enumerate(columns)
            }
        return cache[relation]


def _node(info: dict[str, Any]) -> Node:
    path = info["name"]
    kind = info.get("kind")
    if kind in _RELATION_KINDS:
        return Node(path, [Facet.CHILDREN, Facet.TABLE, Facet.METADATA])
    if kind == "records":
        return Node(path, [Facet.CHILDREN, Facet.TABLE])
    if kind in _CONTAINER_KINDS:
        return Node(path, [Facet.CHILDREN])
    if kind in _RAW_KINDS:
        return Node(path, [Facet.RAW])
    if info["type"] == "directory":
        return Node(path, [Facet.CHILDREN, Facet.METADATA])
    return Node(path, [Facet.METADATA])


def _cache(fs: Any, name: str) -> dict[str, Any]:
    """A per-filesystem cache that ``invalidate_cache`` clears."""
    return fs.__dict__.setdefault(f"_data_source_{name}", {})


def _relation(fs: Any, path: str) -> str:
    """The relation whose rows ``path`` holds: the relation itself or its ``records/``."""
    path = fs._strip_protocol(path).rstrip("/")
    return path.removesuffix("/records") if fs.info(path).get("kind") == "records" else path


def _json(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    if isinstance(value, (list, tuple)):
        return [_json(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json(item) for key, item in value.items()}
    return str(value)
