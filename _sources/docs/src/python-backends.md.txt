# Python Backends

Python-defined databases implement `AbstractDatabase`. `AbstractDatabaseFileSystem` wraps
that object and delegates filesystem behavior through the Rust `DatabaseFs` bridge, so Python
backend authors do not reimplement path parsing.

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

## Required Methods

| Method                                           | Purpose                                                                       |
| ------------------------------------------------ | ----------------------------------------------------------------------------- |
| `dialect()`                                      | Returns a dialect name such as `sqlite`, `postgresql`, `mysql`, or `generic`. |
| `list_schemas()`                                 | Returns `SchemaInfo` objects.                                                 |
| `list_relations(schema)`                         | Returns tables and views as `RelationInfo`.                                   |
| `list_columns(schema, relation)`                 | Returns `ColumnInfo` metadata.                                                |
| `list_indexes(schema, relation)`                 | Returns `IndexInfo` metadata.                                                 |
| `list_constraints(schema, relation)`             | Returns `ConstraintInfo` metadata.                                            |
| `relation_info(schema, relation)`                | Returns one relation or raises `FileNotFoundError`.                           |
| `view_definition(schema, view)`                  | Returns SQL text for a view.                                                  |
| `query(sql, params=None)`                        | Returns a `pyarrow.Table`.                                                    |
| `insert(schema, relation, table, mode="append")` | Inserts a `pyarrow.Table` and returns row count.                              |

Raise normal Python filesystem-style exceptions where possible. The bridge maps
`FileNotFoundError`, `PermissionError`, `FileExistsError`, `NotADirectoryError`,
`IsADirectoryError`, and `ValueError` back into fsspec exceptions.

## Query And Insert Boundaries

`AbstractDatabaseFileSystem.query()` and `open_query()` both cross the Rust bridge. Query
parameters are marshaled to Rust values, then back into Python for the backend's `query`
method. `insert()` receives a `pyarrow.Table` decoded from the incoming relation data file.

This keeps Python-defined backends behaviorally aligned with native Rust backends:

```python
fs.cat_file("/main/users.arrow")
fs.pipe_file("/main/users.arrow", arrow_ipc_bytes)
fs.query("SELECT * FROM users")
```

## When To Use This

Use Python-defined backends when a database driver already exists in Python, when you want to
adapt SQLAlchemy/DBAPI metadata, or when the "database" is a virtual table over another
source. Native Rust backends are better for production driver integrations where avoiding the
GIL and reducing Python boundary crossings matter.
