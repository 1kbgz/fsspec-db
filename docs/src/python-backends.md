# Python Backends

Python-defined database backends implement `fsspec_db.AbstractDatabase`. An
`AbstractDatabaseFileSystem` wraps the object and delegates filesystem behavior through the
Rust `DatabaseFs` bridge.

## Contract

`AbstractDatabase` mirrors the Rust `Database` trait method set, except for Rust-only default
methods such as `arrow_extraction`. The object must be safe to call from the bridge while the
Python GIL is held.

| Method                                           | Return value           | Contract                                                                |
| ------------------------------------------------ | ---------------------- | ----------------------------------------------------------------------- |
| `dialect()`                                      | `str`                  | One of `generic`, `sqlite`, `postgres`, `postgresql`, or `mysql`.       |
| `list_schemas()`                                 | `list[SchemaInfo]`     | All schemas visible to the backend.                                     |
| `list_relations(schema)`                         | `list[RelationInfo]`   | Tables and views in `schema`.                                           |
| `list_columns(schema, relation)`                 | `list[ColumnInfo]`     | Columns for `schema.relation`, ordered by ordinal position.             |
| `list_indexes(schema, relation)`                 | `list[IndexInfo]`      | Indexes for `schema.relation`.                                          |
| `list_constraints(schema, relation)`             | `list[ConstraintInfo]` | Constraints for `schema.relation`.                                      |
| `relation_info(schema, relation)`                | `RelationInfo`         | Metadata for one table or view.                                         |
| `view_definition(schema, view)`                  | `str`                  | SQL text for one view.                                                  |
| `query(sql, params=None)`                        | `pyarrow.Table`        | Query result for SQL plus optional scalar bind parameters.              |
| `insert(schema, relation, table, mode="append")` | `int`                  | Number of rows inserted from `table`; `mode` is `append` or `truncate`. |

## Metadata Objects

Backends return the metadata classes exported by `fsspec_db`.

| Class            | Constructor                                                                                |
| ---------------- | ------------------------------------------------------------------------------------------ |
| `SchemaInfo`     | `SchemaInfo(name, catalog=None, comment=None)`                                             |
| `RelationInfo`   | `RelationInfo(name, kind, row_count=None, size_bytes=None, comment=None)`                  |
| `ColumnInfo`     | `ColumnInfo(name, data_type, nullable, default, ordinal, primary_key=False, comment=None)` |
| `IndexInfo`      | `IndexInfo(name, columns, unique, method=None)`                                            |
| `ConstraintInfo` | `ConstraintInfo(name, kind, columns, references=None, expr=None)`                          |

`RelationInfo.kind` is `table` or `view`. `ConstraintInfo.kind` is `pk`, `fk`, `unique`, or
`check`.

## Query Boundary

`query(sql, params=None)` returns a `pyarrow.Table`. `params` contains values representable by
the bridge scalar type set: `None`, `bool`, `int`, `float`, `str`, and `bytes`.

`AbstractDatabaseFileSystem.query()` and `open_query()` call the backend's `query()` through the
Rust bridge. Path reads such as `/main/users.arrow?columns=id&limit=10` generate SQL in Rust and
then call the same backend method.

## Insert Boundary

`insert(schema, relation, table, mode="append")` receives a `pyarrow.Table` decoded from a
relation data file. `mode` values are:

| Mode       | Semantics                                     |
| ---------- | --------------------------------------------- |
| `append`   | Add rows to the existing relation.            |
| `truncate` | Replace relation rows before inserting table. |

The method returns the number of inserted rows.

## Exceptions

The bridge maps common Python exceptions to fsspec/Rust error categories.

| Python exception     | Meaning                                |
| -------------------- | -------------------------------------- |
| `FileNotFoundError`  | Missing schema, relation, or item.     |
| `PermissionError`    | Permission-denied operation.           |
| `FileExistsError`    | Existing item for create semantics.    |
| `NotADirectoryError` | Directory operation on a file.         |
| `IsADirectoryError`  | File operation on a directory.         |
| `ValueError`         | Invalid argument or unsupported value. |
| Other exceptions     | Generic backend error.                 |

## Minimal Backend

```python
import pyarrow as pa

from fsspec_db import (
    AbstractDatabase,
    AbstractDatabaseFileSystem,
    ColumnInfo,
    ConstraintInfo,
    IndexInfo,
    RelationInfo,
    SchemaInfo,
)


class MemoryDatabase(AbstractDatabase):
    def __init__(self):
        self.table = pa.table({"id": [1], "name": ["ada"]})

    def dialect(self):
        return "sqlite"

    def list_schemas(self):
        return [SchemaInfo("main")]

    def list_relations(self, schema):
        return [RelationInfo("users", "table", row_count=self.table.num_rows)]

    def list_columns(self, schema, relation):
        return [
            ColumnInfo("id", "INTEGER", False, None, 1, True),
            ColumnInfo("name", "TEXT", True, None, 2),
        ]

    def list_indexes(self, schema, relation):
        return []

    def list_constraints(self, schema, relation):
        return [ConstraintInfo("pk_users", "pk", ["id"])]

    def relation_info(self, schema, relation):
        return self.list_relations(schema)[0]

    def view_definition(self, schema, view):
        raise FileNotFoundError(view)

    def query(self, sql, params=None):
        return self.table

    def insert(self, schema, relation, table, mode="append"):
        self.table = table if mode == "truncate" else pa.concat_tables([self.table, table])
        return table.num_rows


fs = AbstractDatabaseFileSystem(MemoryDatabase())
```
