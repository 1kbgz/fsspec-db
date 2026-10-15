# How to read and write SQLite data

Install `fsspec-db` and connect to an existing database:

```console
python -m pip install fsspec-db
```

```python
import fsspec

fs = fsspec.filesystem("db+sqlite", database="app.db")
```

The examples below require a `users` table with `name TEXT` and `score REAL`
columns. For a complete database setup, start with the [tutorial](tutorial.md).

## Inspect and read

```python
fs.ls("/", detail=False)
fs.ls("/main", detail=False)
fs.info("/main/users/columns/name")
table = fs.query("SELECT name, score FROM users WHERE score > ?", [1.0])
```

Read a format-selecting path when the consumer needs a file:

```python
import pyarrow.ipc as ipc

with fs.open("/main/users.arrow?limit=100", "rb") as file:
    table = ipc.open_stream(file).read_all()
```

## Append rows

Encode rows as Arrow IPC and close the handle to commit:

```python
import pyarrow as pa
import pyarrow.ipc as ipc

rows = pa.table({"name": ["Ada"], "score": [1.0]})
sink = pa.BufferOutputStream()
with ipc.new_stream(sink, rows.schema) as writer:
    writer.write_table(rows)
with fs.open("/main/users.arrow", "ab") as file:
    file.write(sink.getvalue().to_pybytes())
```

Use `"wb"` only when replacing all existing rows. An exception inside the write
context discards a native write instead of committing it.

## Import a Parquet file

```python
import pyarrow.parquet as pq

pq.write_table(rows, "rows.parquet")
fs.put_file("rows.parquet", "/main/users.parquet", mode="append")
```

Omit `mode="append"` to replace existing rows. The destination table must already
exist. See [Backend configuration and types](backends.md) for type mappings and
[How to manage tables and metadata caches](how-to-manage.md) for table creation.
