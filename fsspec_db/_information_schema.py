from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pyarrow as pa

from .spec import ColumnInfo, ConstraintInfo, RelationInfo, SchemaInfo

Query = Callable[[str, list[Any] | None], pa.Table]


def list_schemas(query: Query) -> list[SchemaInfo]:
    table = query("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name", None)
    return [SchemaInfo(row["schema_name"]) for row in table.to_pylist()]


def list_relations(query: Query, schema: str) -> list[RelationInfo]:
    table = query(
        "SELECT table_name, table_type FROM information_schema.tables "
        "WHERE table_schema = ? ORDER BY table_name",
        [schema],
    )
    return [
        RelationInfo(row["table_name"], "view" if "VIEW" in row["table_type"].upper() else "table")
        for row in table.to_pylist()
    ]


def relation_info(query: Query, schema: str, relation: str) -> RelationInfo:
    for info in list_relations(query, schema):
        if info.name == relation:
            return info
    raise FileNotFoundError(f"{schema}.{relation}")


def list_columns(query: Query, schema: str, relation: str) -> list[ColumnInfo]:
    table = query(
        "SELECT column_name, data_type, is_nullable, column_default, ordinal_position "
        "FROM information_schema.columns WHERE table_schema = ? AND table_name = ? "
        "ORDER BY ordinal_position",
        [schema, relation],
    )
    primary = _primary_key_columns(query, schema, relation)
    return [
        ColumnInfo(
            row["column_name"],
            row["data_type"],
            row["is_nullable"].upper() == "YES",
            None if row["column_default"] is None else str(row["column_default"]),
            row["ordinal_position"],
            row["column_name"] in primary,
        )
        for row in table.to_pylist()
    ]


def list_constraints(query: Query, schema: str, relation: str) -> list[ConstraintInfo]:
    table = query(
        "SELECT tc.constraint_name, tc.constraint_type, kcu.column_name "
        "FROM information_schema.table_constraints tc "
        "LEFT JOIN information_schema.key_column_usage kcu "
        "ON tc.constraint_catalog = kcu.constraint_catalog "
        "AND tc.constraint_schema = kcu.constraint_schema "
        "AND tc.constraint_name = kcu.constraint_name "
        "WHERE tc.table_schema = ? AND tc.table_name = ? "
        "ORDER BY tc.constraint_name, kcu.ordinal_position",
        [schema, relation],
    )
    grouped: dict[tuple[str, str], list[str]] = {}
    for row in table.to_pylist():
        key = (row["constraint_name"], row["constraint_type"])
        if row["column_name"] is not None:
            grouped.setdefault(key, []).append(row["column_name"])
        else:
            grouped.setdefault(key, [])
    kinds = {"PRIMARY KEY": "pk", "FOREIGN KEY": "fk", "UNIQUE": "unique", "CHECK": "check"}
    return [ConstraintInfo(name, kinds[kind.upper()], columns) for (name, kind), columns in grouped.items() if kind.upper() in kinds]


def _primary_key_columns(query: Query, schema: str, relation: str) -> set[str]:
    return {
        column
        for constraint in list_constraints(query, schema, relation)
        if constraint.kind == "pk"
        for column in constraint.columns
    }
