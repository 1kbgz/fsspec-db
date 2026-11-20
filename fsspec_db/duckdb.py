from __future__ import annotations

from typing import Any

import fsspec
import pyarrow as pa

from . import _information_schema
from .python import PyDatabaseFileSystem
from .spec import AbstractDatabase, IndexInfo


class DuckDBDatabase(AbstractDatabase):
    """Arrow-native Python backend for an embedded DuckDB database."""

    def __init__(self, database: str = ":memory:", *, connection: Any = None, read_only: bool = False) -> None:
        if connection is None:
            import duckdb

            connection = duckdb.connect(database, read_only=read_only)
        self.connection = connection

    def dialect(self) -> str:
        return "generic"

    def list_schemas(self):
        return _information_schema.list_schemas(self.query)

    def list_relations(self, schema: str):
        return _information_schema.list_relations(self.query, schema)

    def list_columns(self, schema: str, relation: str):
        return _information_schema.list_columns(self.query, schema, relation)

    def list_indexes(self, schema: str, relation: str):
        table = self.query(
            "SELECT index_name, expressions, is_unique FROM duckdb_indexes() WHERE schema_name = ? AND table_name = ? ORDER BY index_name",
            [schema, relation],
        )
        return [IndexInfo(row["index_name"], row["expressions"] or [], row["is_unique"], None) for row in table.to_pylist()]

    def list_constraints(self, schema: str, relation: str):
        return _information_schema.list_constraints(self.query, schema, relation)

    def relation_info(self, schema: str, relation: str):
        return _information_schema.relation_info(self.query, schema, relation)

    def view_definition(self, schema: str, view: str) -> str:
        table = self.query("SELECT sql FROM duckdb_views() WHERE schema_name = ? AND view_name = ?", [schema, view])
        if table.num_rows == 0:
            raise FileNotFoundError(f"{schema}.{view}")
        return table.column("sql")[0].as_py()

    def query(self, sql: str, params: list[Any] | None = None) -> pa.Table:
        result = self.connection.execute(sql, params or [])
        reader = result.to_arrow_reader()
        return reader.read_all()

    def insert(self, schema: str, relation: str, table: pa.Table, mode: str = "append") -> int:
        temporary = "__fsspec_db_insert"
        self.connection.register(temporary, table)
        try:
            if mode == "truncate":
                self.connection.execute(f'DELETE FROM "{schema}"."{relation}"')
            self.connection.execute(f'INSERT INTO "{schema}"."{relation}" SELECT * FROM {temporary}')
        finally:
            self.connection.unregister(temporary)
        return table.num_rows


class DuckDBDatabaseFileSystem(PyDatabaseFileSystem):
    protocol = "db+duckdb"

    @classmethod
    def _strip_protocol(cls, path: Any) -> Any:
        if isinstance(path, str) and path.startswith("db+duckdb://"):
            return _split_url(path)[1]
        return super()._strip_protocol(path)

    @classmethod
    def _get_kwargs_from_urls(cls, path: str) -> dict[str, str]:
        database = _split_url(path)[0]
        return {} if database is None else {"database": database}

    def __init__(self, database: str = ":memory:", *, connection: Any = None, **storage_options: Any) -> None:
        super().__init__(DuckDBDatabase(database, connection=connection), **storage_options)
        self.database = database

    def unstrip_protocol(self, name: str) -> str:
        """Return a URL containing the database filename and the path within it."""
        inner = name.strip("/")
        return f"db+duckdb://{self.database}/{inner}" if inner else f"db+duckdb://{self.database}"


def _split_url(path: str) -> tuple[str | None, str]:
    """Split after the first .duckdb or .db segment; other URLs name only a database."""
    rest = path.removeprefix("db+duckdb://").rstrip("/")
    parts = rest.split("/")
    for index, part in enumerate(parts):
        if part.lower().endswith((".duckdb", ".db")):
            return "/".join(parts[: index + 1]), "/" + "/".join(parts[index + 1 :]).strip("/")
    return rest or None, "/"


fsspec.register_implementation("db+duckdb", DuckDBDatabaseFileSystem, clobber=True)
