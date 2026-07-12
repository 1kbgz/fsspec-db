from __future__ import annotations

from typing import Any

import fsspec
import pyarrow as pa

from .python import PyDatabaseFileSystem
from .spec import AbstractDatabase, ColumnInfo, ConstraintInfo, IndexInfo, RelationInfo, SchemaInfo


class SQLAlchemyDatabase(AbstractDatabase):
    """SQLAlchemy engine adapter implementing the Python database contract."""

    def __init__(self, url: str | None = None, *, engine: Any = None, **engine_options: Any) -> None:
        if engine is None:
            if url is None:
                raise ValueError("SQLAlchemyDatabase requires url or engine")
            from sqlalchemy import create_engine

            engine = create_engine(url, **engine_options)
        self.engine = engine

    def dialect(self) -> str:
        name = self.engine.dialect.name
        return "postgres" if name == "postgresql" else name

    def list_schemas(self) -> list[SchemaInfo]:
        return [SchemaInfo(name) for name in self._inspector().get_schema_names()]

    def list_relations(self, schema: str) -> list[RelationInfo]:
        inspector = self._inspector()
        tables = [RelationInfo(name, "table") for name in inspector.get_table_names(schema=schema)]
        views = [RelationInfo(name, "view") for name in inspector.get_view_names(schema=schema)]
        return tables + views

    def list_columns(self, schema: str, relation: str) -> list[ColumnInfo]:
        inspector = self._inspector()
        primary_key = set((inspector.get_pk_constraint(relation, schema=schema) or {}).get("constrained_columns") or [])
        return [
            ColumnInfo(
                info["name"],
                str(info["type"]),
                bool(info.get("nullable", True)),
                None if info.get("default") is None else str(info["default"]),
                ordinal,
                info["name"] in primary_key,
                info.get("comment"),
            )
            for ordinal, info in enumerate(inspector.get_columns(relation, schema=schema), 1)
        ]

    def list_indexes(self, schema: str, relation: str) -> list[IndexInfo]:
        return [
            IndexInfo(info["name"], info.get("column_names") or [], bool(info.get("unique")), info.get("dialect_options", {}).get("method"))
            for info in self._inspector().get_indexes(relation, schema=schema)
            if info.get("name")
        ]

    def list_constraints(self, schema: str, relation: str) -> list[ConstraintInfo]:
        inspector = self._inspector()
        result = []
        primary = inspector.get_pk_constraint(relation, schema=schema) or {}
        if primary.get("constrained_columns"):
            result.append(ConstraintInfo(primary.get("name") or f"pk_{relation}", "pk", primary["constrained_columns"]))
        for info in inspector.get_foreign_keys(relation, schema=schema):
            target = ".".join(filter(None, (info.get("referred_schema"), info.get("referred_table"))))
            result.append(ConstraintInfo(info.get("name") or f"fk_{relation}_{len(result)}", "fk", info.get("constrained_columns") or [], target))
        for info in inspector.get_unique_constraints(relation, schema=schema):
            result.append(ConstraintInfo(info.get("name") or f"uq_{relation}_{len(result)}", "unique", info.get("column_names") or []))
        for info in inspector.get_check_constraints(relation, schema=schema):
            result.append(ConstraintInfo(info.get("name") or f"ck_{relation}_{len(result)}", "check", [], None, info.get("sqltext")))
        return result

    def relation_info(self, schema: str, relation: str) -> RelationInfo:
        for info in self.list_relations(schema):
            if info.name == relation:
                return info
        raise FileNotFoundError(f"{schema}.{relation}")

    def view_definition(self, schema: str, view: str) -> str:
        definition = self._inspector().get_view_definition(view, schema=schema)
        if definition is None:
            raise FileNotFoundError(f"{schema}.{view}")
        return definition

    def query(self, sql: str, params: list[Any] | None = None) -> pa.Table:
        from sqlalchemy import text

        with self.engine.connect() as connection:
            result = connection.exec_driver_sql(sql, tuple(params or ())) if params else connection.execute(text(sql))
            names = list(result.keys())
            rows = [dict(row._mapping) for row in result]
        if rows:
            return pa.Table.from_pylist(rows)
        return pa.table({name: pa.array([], type=pa.null()) for name in names})

    def insert(self, schema: str, relation: str, table: pa.Table, mode: str = "append") -> int:
        from sqlalchemy import MetaData, Table, delete

        target = Table(relation, MetaData(), schema=schema, autoload_with=self.engine)
        with self.engine.begin() as connection:
            if mode == "truncate":
                connection.execute(delete(target))
            rows = table.to_pylist()
            if rows:
                connection.execute(target.insert(), rows)
        return table.num_rows

    def _inspector(self) -> Any:
        from sqlalchemy import inspect

        return inspect(self.engine)


class SQLAlchemyDatabaseFileSystem(PyDatabaseFileSystem):
    protocol = "db+sqlalchemy"

    def __init__(self, url: str | None = None, *, engine: Any = None, **storage_options: Any) -> None:
        super().__init__(SQLAlchemyDatabase(url, engine=engine, **storage_options))


fsspec.register_implementation("db+sqlalchemy", SQLAlchemyDatabaseFileSystem, clobber=True)
