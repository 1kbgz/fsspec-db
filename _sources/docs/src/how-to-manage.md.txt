# How to manage tables and metadata caches

Enable DDL when an application needs filesystem mutation methods. These examples
create and remove a table in `managed.db`.

## Create and rename a table

```python
import fsspec
import pyarrow as pa

fs = fsspec.filesystem("db+sqlite", database="managed.db", allow_ddl=True)
fs.mkdir("/main/events", schema=pa.schema([pa.field("id", pa.int64())]))
fs.mv("/main/events", "/main/archived_events")
print(fs.ls("/main", detail=False))
```

The listing contains `/main/archived_events`. Use `sql="CREATE TABLE ..."` instead
of `schema=` when database-specific types or constraints are needed. The flag
guards filesystem DDL helpers; raw `query()` retains normal SQL permissions.

## Refresh external changes

Bypass stale introspection after another connection changes database objects:

```python
fs.ls("/main", detail=False, refresh=True)
fs.info("/main/archived_events", refresh=True)
```

Set `listings_expiry_time` and `max_paths` at construction to bound cache lifetime
and entries. Call `fs.invalidate_cache()` to clear introspection explicitly.

## Defer writes until a transaction exits

Encode rows and group deferred writes with fsspec's transaction context:

```python
import pyarrow.ipc as ipc

rows = pa.table({"id": [1, 2]})
sink = pa.BufferOutputStream()
with ipc.new_stream(sink, rows.schema) as writer:
    writer.write_table(rows)

with fs.transaction:
    with fs.open("/main/archived_events.arrow", "ab") as file:
        file.write(sink.getvalue().to_pybytes())

print(fs.query("SELECT id FROM archived_events ORDER BY id").to_pylist())
```

The result is `[{'id': 1}, {'id': 2}]`. Exceptions before transaction completion
discard deferred buffers. Each file commits separately; use a backend's database
transaction API when several writes must be atomic together.

## Remove rows or a relation

With `allow_ddl=True`, use an extension to delete rows while keeping the table:

```python
fs.rm_file("/main/archived_events.arrow")
```

Use the bare relation path to drop the table itself:

```python
fs.rm_file("/main/archived_events")
```

See [Paths and metadata](paths.md) for exact DDL and write-mode behavior.
