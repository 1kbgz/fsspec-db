# Paths and metadata

fsspec-db paths are absolute, protocol-free paths once a filesystem is constructed.

```python
import fsspec

fs = fsspec.filesystem("db+sqlite", database="app.db")
fs.ls("/main")
```

## Grammar

| Path                                            | Meaning                                       |
| ----------------------------------------------- | --------------------------------------------- |
| `/`                                             | Root, listing schemas/catalogs.               |
| `/{schema}`                                     | Schema directory, listing tables and views.   |
| `/{schema}/{relation}`                          | Relation directory, listing metadata facets.  |
| `/{schema}/{relation}/columns`                  | Column facet directory.                       |
| `/{schema}/{relation}/columns/{column}`         | Column metadata item.                         |
| `/{schema}/{relation}/indexes`                  | Index facet directory.                        |
| `/{schema}/{relation}/indexes/{index}`          | Index metadata item.                          |
| `/{schema}/{relation}/constraints`              | Constraint facet directory.                   |
| `/{schema}/{relation}/constraints/{constraint}` | Constraint metadata item.                     |
| `/{schema}/{view}/depends_on`                   | View dependency facet directory.              |
| `/{schema}/{view}/depends_on/{relation}`        | A relation the view reads, with `target`.     |
| `/{schema}/{view}/definition.sql`               | View definition SQL.                          |
| `/{schema}/{relation}.arrow`                    | Materialized relation as Arrow IPC stream.    |
| `/{schema}/{relation}.parquet`                  | Materialized relation as Parquet.             |
| `/{schema}/{relation}.csv`                      | Materialized relation as CSV.                 |
| `/{schema}/{relation}.jsonl`                    | Materialized relation as line-delimited JSON. |
| `/{schema}/{relation}.sql`                      | Relation DDL text.                            |
| `/{schema}/{table}/records`                     | Records of a table with a primary key.        |
| `/{schema}/{table}/records/{key...}`            | One record, or a directory of keys.           |

SQLite normally exposes the default database as `/main`. Native backends render
relation `.sql` data paths. The direct `PyDatabaseFileSystem` adapter raises
`NotImplementedError` for relation DDL rendering; view `definition.sql` remains
available when its backend implements `view_definition`.

## Listing

```python
fs.ls("/", detail=False)
# ["/main"]

fs.ls("/main", detail=False)
# ["/main/users", "/main/active_users"]

fs.ls("/main/users", detail=False)
# ["/main/users/columns", "/main/users/indexes", "/main/users/constraints", "/main/users/records"]
```

Tables without a primary key have no `records` entry.

`detail=True` returns fsspec info dictionaries:

```python
fs.ls("/main/users/columns", detail=True)
```

## Metadata

```python
fs.info("/main/users")
fs.info("/main/users/columns/id")
fs.info("/main/users/indexes/idx_users_name")
```

Relation info includes `kind` and may include `row_count`. Column info includes database
type, nullability, ordinal position, and primary-key status.

## Read Shaping

Four query parameters are available on materialized relation paths:

| Parameter | Example                                | Status                                |
| --------- | -------------------------------------- | ------------------------------------- |
| `columns` | `/main/users.arrow?columns=id,name`    | Selects a column list.                |
| `limit`   | `/main/users.parquet?limit=100`        | Adds `LIMIT`.                         |
| `offset`  | `/main/users.arrow?limit=20&offset=20` | Skips rows before applying the limit. |
| `where`   | `/main/users.arrow?where=score > 10`   | Adds a `WHERE` predicate (see below). |

Query values are URL-decoded, so percent-encoded predicates such as
`?where=score%20%3E%2010` are equivalent to the unencoded form.

```python
data = fs.cat_file("/main/users.arrow?columns=id,name&limit=10&where=id > 0")
```

### `where` predicate pushdown

The `where` value is parsed as a single boolean SQL expression with a dialect-aware parser,
re-serialized from its AST, and appended as a `WHERE` clause. Because only a parsed expression
is ever emitted, multi-statement or non-expression payloads are rejected with a `ValueError`
rather than interpolated:

```python
fs.cat_file("/main/users.arrow?where=score > 10")          # ok
fs.cat_file("/main/users.arrow?where=1); DROP TABLE x;--")  # rejected
```

`fs.query()` accepts arbitrary SQL beyond a single predicate.

## Write Paths

Data writes use the same materialized relation paths as reads:

```python
with fs.open("/main/users.arrow", "ab") as f:
    f.write(arrow_ipc_bytes)
```

The incoming file extension controls decoding. `.arrow` and `.parquet` carry their own
schema. `.csv` and `.jsonl` are decoded using the database table schema.

## Metadata dictionaries

`info()` and `ls(detail=True)` return ordinary fsspec dictionaries with these common keys:

| Key    | Meaning                                                                                              |
| ------ | ---------------------------------------------------------------------------------------------------- |
| `name` | Absolute fsspec-db path, without protocol.                                                           |
| `type` | `"directory"` for schemas, relations, and facets; `"file"` for metadata items and materialized data. |
| `size` | Byte size when known. Materialized table reads usually learn size only after encoding.               |
| `kind` | fsspec-db object kind, such as `schema`, `table`, `view`, `column`, `index`, or `constraint`.        |

Extra metadata is stored directly in the same dictionary:

| Path               | Extra fields                                                           |
| ------------------ | ---------------------------------------------------------------------- |
| Relation directory | `kind`, optional `row_count`, optional `size_bytes`.                   |
| Column item        | `data_type`, `nullable`, `ordinal`, `primary_key`, optional `default`. |
| Index item         | `columns`, `unique`, optional `method`.                                |
| Constraint item    | `kind`, `columns`, optional `references`, optional `expr`.             |
| Data file          | `format`, `dialect`, `size_known`.                                     |

## Column values

`/{schema}/{relation}/columns/{column}` is both a metadata item for `info()` and a
JSON array of distinct, sorted values for reads. `limit` and `offset` bound
column-value reads; the default limit is 100.

## Write modes

| Operation                                                        | Behavior                                                                  |
| ---------------------------------------------------------------- | ------------------------------------------------------------------------- |
| `open(path, "wb")`                                               | Replaces relation rows when the write commits.                            |
| `open(path, "ab")`                                               | Appends rows when the write commits.                                      |
| `pipe_file(path, bytes)` / `put_file(local, path)`               | Overwrites by default.                                                    |
| `pipe_file(..., mode="append")` / `put_file(..., mode="append")` | Appends.                                                                  |
| `pipe_file(..., mode="create")` / `put_file(..., mode="create")` | Raises `FileExistsError` for an existing target; does not create a table. |

Supported open modes are `r`, `rb`, `w`, `wb`, `a`, and `ab`. Exclusive-create and
update modes raise `NotImplementedError`. Writes decode the completed encoded
stream at commit time. Unclosed native write handles are discarded.

## Records

Path segments after `records/` map to primary-key columns in key order, so a composite
key is a nested directory. The last key column takes the rest of the path and may
contain `/`. Keys are stored exactly as the path segments are written; callers that
need `/` or other reserved characters inside a segment should encode them.

| Table shape                                | Record path             | Contents                     |
| ------------------------------------------ | ----------------------- | ---------------------------- |
| Exactly one non-key column, of binary type | `records/{key...}`      | The column value, unchanged. |
| Any other table with a primary key         | `records/{key...}.json` | The row as one JSON object.  |

| Operation                           | Behavior                                                    |
| ----------------------------------- | ----------------------------------------------------------- |
| `cat_file`, `open(path, "rb")`      | Reads one record; a missing key raises `FileNotFoundError`. |
| `pipe_file`, `open(path, "wb")`     | Inserts or replaces one row with a single upsert statement. |
| `rm_file`, `rm`                     | Deletes one row. Deleting a missing key succeeds.           |
| `ls`, `find`                        | Select key columns only and are never cached.               |
| `open(path, "ab")`, `mode="append"` | Raise `OSError`; records are replaced, not appended.        |

Upserts use `ON CONFLICT` on SQLite and PostgreSQL and `ON DUPLICATE KEY UPDATE` on
MySQL. Listing under a partial last key compares bytes (`COLLATE BINARY` on SQLite,
`COLLATE "C"` on PostgreSQL, `CAST(... AS BINARY)` on MySQL), so prefixes do not depend
on the column collation. Record writes and deletes do not need `allow_ddl`. Records
are available on the native SQLite, PostgreSQL, and MySQL backends.

## URLs

A URL names the database, optionally followed by a path inside it:

| URL                                            | Database        | Path               |
| ---------------------------------------------- | --------------- | ------------------ |
| `db+sqlite://app.db`                           | `app.db`        | `/`                |
| `db+sqlite:///data/app.db/main/kv/records`     | `/data/app.db`  | `/main/kv/records` |
| `db+postgresql://user@host:5432/app/public/kv` | `app` on `host` | `/public/kv`       |
| `db+mysql://user@host:3306/app/app/kv`         | `app` on `host` | `/app/kv`          |

For SQLite, the database ends at the first segment ending in `.db`, `.sqlite`, or
`.sqlite3`. For PostgreSQL and MySQL, it is the first path segment. A query string
belongs to the connection when the URL has no path inside the database, and to the
path otherwise.

## DDL and cache controls

`allow_ddl=False` is the default. Guarded `mkdir`, `rm_file`, and `mv` raise
`PermissionError` unless enabled. Raw `query()` is not restricted by that flag.

With DDL enabled, `mkdir("/schema/relation", schema=arrow_schema)` creates a table;
`sql=` accepts explicit creation SQL. `rm_file("/schema/relation")` drops the
relation, while a relation path with an extension deletes its rows. `mv` renames
relations within one schema; cross-schema moves raise `NotImplementedError`.

`ls(..., refresh=True)` and `info(..., refresh=True)` bypass cached introspection.
`invalidate_cache(path=None)` clears all Python and available Rust introspection
cache entries, even when a path is supplied. Listing cache options include
`use_listings_cache`, `listings_expiry_time`, and `max_paths`.
