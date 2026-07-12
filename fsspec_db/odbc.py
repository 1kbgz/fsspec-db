from __future__ import annotations

from typing import Any

import fsspec
import pyarrow as pa

from . import _information_schema
from .python import PyDatabaseFileSystem
from .spec import AbstractDatabase


class OdbcDatabase(AbstractDatabase):
    """Arrow-native ODBC backend using the arrow-odbc connection API."""

    def __init__(self, connection_string: str, *, user: str = "", password: str = "", batch_size: int = 65535) -> None:
        from arrow_odbc import connect

        self.connection = connect(connection_string=connection_string, user=user, password=password)
        self.batch_size = batch_size

    def dialect(self) -> str:
        return "generic"

    def list_schemas(self):
        return _information_schema.list_schemas(self.query)

    def list_relations(self, schema: str):
        return _information_schema.list_relations(self.query, schema)

    def list_columns(self, schema: str, relation: str):
        return _information_schema.list_columns(self.query, schema, relation)

    def list_indexes(self, schema: str, relation: str):
        self.relation_info(schema, relation)
        return []

    def list_constraints(self, schema: str, relation: str):
        return _information_schema.list_constraints(self.query, schema, relation)

    def relation_info(self, schema: str, relation: str):
        return _information_schema.relation_info(self.query, schema, relation)

    def view_definition(self, schema: str, view: str) -> str:
        raise NotImplementedError("ODBC does not expose a portable view-definition query")

    def query(self, sql: str, params: list[Any] | None = None) -> pa.Table:
        reader = self.connection.read_arrow_batches(query=sql, parameters=params or [], batch_size=self.batch_size)
        batches = list(reader)
        return pa.Table.from_batches(batches, schema=reader.schema)

    def insert(self, schema: str, relation: str, table: pa.Table, mode: str = "append") -> int:
        target = f"{schema}.{relation}"
        if mode == "truncate":
            list(self.connection.read_arrow_batches(query=f"DELETE FROM {target}"))
        reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
        self.connection.insert_into_table(chunk_size=self.batch_size, table=target, reader=reader)
        return table.num_rows


class OdbcDatabaseFileSystem(PyDatabaseFileSystem):
    protocol = "db+odbc"

    def __init__(self, connection_string: str, **storage_options: Any) -> None:
        options = {key: storage_options.pop(key) for key in ("user", "password", "batch_size") if key in storage_options}
        super().__init__(OdbcDatabase(connection_string, **options), **storage_options)


fsspec.register_implementation("db+odbc", OdbcDatabaseFileSystem, clobber=True)
