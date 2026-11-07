# How to store keyed records

Tables with a primary key expose a records view at `/{schema}/{table}/records`. Each
record is one row, read and written with ordinary fsspec calls, so code written for
`fsspec.get_mapper` can keep state in a database table.

## Create a table

Make the primary key match the path segments the application writes. A store addressed
as `{namespace}/{key}` needs two key columns and one binary value column:

```python
import sqlite3

with sqlite3.connect("state.db") as conn:
    conn.execute(
        "CREATE TABLE kv (ns TEXT, key TEXT, value BLOB NOT NULL, PRIMARY KEY (ns, key))"
    )
```

On MySQL, declare text key columns with a binary collation such as `utf8mb4_bin`, or
keys that differ only in case collide in the primary key.

## Use a mapper

Put the path inside the database after the database part of the URL:

```python
import fsspec

store = fsspec.get_mapper("db+sqlite://state.db/main/kv/records")
store["orders/latest"] = b"\x00\x01"
print(store["orders/latest"])
print(list(store))
del store["orders/latest"]
```

PostgreSQL and MySQL URLs follow the same pattern, with the database name before the
path: `db+postgresql://user:password@localhost:5432/app/public/kv/records`.

## Read and write rows as JSON

When a table has other columns, each record is the row as a JSON object at `{key}.json`:

```python
fs = fsspec.filesystem("db+sqlite", database="app.db")
fs.pipe_file("/main/users/records/1.json", b'{"name": "ada", "score": 1.5}')
fs.cat_file("/main/users/records/1.json")
# b'{"id":1,"name":"ada","score":1.5}'
```

A write updates the columns named in the object and leaves the others unchanged. Key
columns may appear in the object only when they match the path.

See [Paths and metadata](paths.md#records) for the full rules.
