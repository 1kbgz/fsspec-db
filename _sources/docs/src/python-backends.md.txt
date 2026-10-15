# Python backend contract

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

## Filesystem wrappers

`AbstractDatabaseFileSystem(db, **storage_options)` and
`PyDatabaseFileSystem(db, **storage_options)` adapt a concrete `AbstractDatabase`.
`AbstractDatabaseFileSystem` routes paths through Rust `DatabaseFs`.
`PyDatabaseFileSystem` implements the path mapping directly in Python and does
not call the Rust bridge.

The backend must execute SQL generated for path projection, predicates, limits,
and offsets, as well as SQL supplied directly to `query`. Returning an unchanged
in-memory table for every query does not satisfy that contract.

Metadata must describe actual relation state. Insert behavior, rollback, and
connection lifetime remain backend responsibilities. Existing implementations in
`fsspec_db.duckdb`, `fsspec_db.sqlalchemy`, and `fsspec_db.odbc` are concrete adapters.
