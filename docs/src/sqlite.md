# SQLite

SQLite is the first native backend. It is implemented in Rust with sqlx and exposed to
Python as the `db+sqlite` fsspec protocol.

## Construction

```python
import fsspec

fs = fsspec.filesystem("db+sqlite", database="app.db")
```

`database` can be a filesystem path, `:memory:`, or a SQLite URL such as
`sqlite:///tmp/app.db`.

You can also construct the class directly:

```python
from fsspec_db import SQLiteDatabaseFileSystem

fs = SQLiteDatabaseFileSystem(database="app.db")
```

## Reading Metadata

```python
fs.ls("/", detail=False)
fs.ls("/main", detail=False)
fs.info("/main/users")
fs.info("/main/users/columns/id")
```

The backend introspects:

- schemas from `PRAGMA database_list`;
- tables and views from `sqlite_master`;
- columns from `PRAGMA table_info`;
- indexes from `PRAGMA index_list` and `PRAGMA index_info`;
- primary keys and foreign keys from `PRAGMA table_info` and `PRAGMA foreign_key_list`.

Relation lookups check only the requested relation. Listing a whole schema may still count
rows for every listed table so `row_count` is available in `info()`.

## Reading Data

```python
import pyarrow.ipc as ipc

with ipc.open_stream(fs.cat_file("/main/users.arrow")) as reader:
    table = reader.read_all()

table = fs.query("SELECT id, name FROM users WHERE id > ?", [0])
```

The native path releases the Python GIL while SQLite I/O is running.

## Writing Arrow IPC

```{warning}
Overwrite writes replace the table contents. `open(path, "wb")`, default `pipe_file`, and
default `put_file` delete existing rows inside the write transaction before inserting the
incoming rows. Use `"ab"` or `mode="append"` to preserve existing rows.
```

```python
import pyarrow as pa
import pyarrow.ipc as ipc

table = pa.table({"name": ["ada"], "score": [1.0]})
sink = pa.BufferOutputStream()
with ipc.new_stream(sink, table.schema) as writer:
    writer.write_table(table)

with fs.open("/main/users.arrow", "ab") as file:
    file.write(sink.getvalue().to_pybytes())
```

`"ab"` appends rows. `"wb"` truncates the table with `DELETE FROM` inside the same
transaction, then inserts rows.

## Writing Parquet

```python
import pyarrow as pa
import pyarrow.parquet as pq

pq.write_table(pa.table({"name": ["grace"], "score": [2.0]}), "rows.parquet")
fs.put_file("rows.parquet", "/main/users.parquet")
```

`put_file` truncates by default. Pass `mode="append"` to append:

```python
fs.put_file("rows.parquet", "/main/users.parquet", mode="append")
```

## Type Handling

SQLite values are converted into Arrow arrays with a small affinity mapper:

| SQLite value/type | Arrow type |
| ----------------- | ---------- |
| boolean-like      | `bool`     |
| integer-like      | `int64`    |
| real/float/double | `float64`  |
| blob/binary       | `binary`   |
| everything else   | `utf8`     |

For writes, Arrow `Null` columns bind SQLite `NULL`. Temporal arrays are bound as integer
epoch values. When a query produces mixed SQLite runtime types in one expression column,
cast explicitly in SQL for predictable Arrow output.
