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
    are not listed, so a view does not show the same rows twice."""

    def describe(self, path: str) -> Node:
        return _node(self.info(path))

    def children(self, path: str) -> list[Node]:
        return [_node(entry) for entry in self.ls(path, detail=True)]

    def schema(self, path: str) -> pa.Schema:
        with self.open(f"{_relation(self, path)}.arrow", "rb") as file:
            return ipc.open_stream(file).schema

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
        return {key: _json(value) for key, value in self.info(path).items() if key not in _INFO_FIELDS}


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
