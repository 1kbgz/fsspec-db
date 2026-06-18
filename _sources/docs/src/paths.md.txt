# Paths

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

SQLite normally exposes the default database as `/main`.

## Listing

```python
fs.ls("/", detail=False)
# ["/main"]

fs.ls("/main", detail=False)
# ["/main/users", "/main/active_users"]

fs.ls("/main/users", detail=False)
# ["/main/users/columns", "/main/users/indexes", "/main/users/constraints"]
```

Use `detail=True` to get fsspec info dictionaries:

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

Three query parameters are available on materialized relation paths:

| Parameter | Example                              | Status                                |
| --------- | ------------------------------------ | ------------------------------------- |
| `columns` | `/main/users.arrow?columns=id,name`  | Selects a column list.                |
| `limit`   | `/main/users.parquet?limit=100`      | Adds `LIMIT`.                         |
| `where`   | `/main/users.arrow?where=score > 10` | Adds a `WHERE` predicate (see below). |

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

For arbitrary SQL beyond a single predicate, use `fs.query()`.

## Write Paths

Write to the same data paths used for reads:

```python
with fs.open("/main/users.arrow", "ab") as f:
    f.write(arrow_ipc_bytes)
```

The incoming file extension controls decoding. `.arrow` and `.parquet` carry their own
schema. `.csv` and `.jsonl` are decoded using the database table schema.
