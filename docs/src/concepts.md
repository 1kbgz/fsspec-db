# Concepts

`fsspec-db` maps database structure onto the fsspec filesystem model. Schemas, tables,
views, columns, indexes, and constraints become paths. Table data becomes files whose
extension selects the transfer format.

## Layers

The implementation has three layers:

1. A Rust `Database` trait describes database primitives: list schemas, list relations,
   inspect metadata, run queries, and insert Arrow batches.
1. Rust `DatabaseFs<D>` turns a `Database` implementation into an `fsspec_rs::FileSystem`.
   It owns path parsing, metadata shaping, SQL generation for table reads, and format
   encoding/decoding.
1. Python filesystem classes subclass `fsspec.AbstractFileSystem` and delegate primitives
   to the PyO3 bridge. Python gets normal fsspec behavior while Rust remains the source of
   truth for database path semantics.

The native SQLite, PostgreSQL, and MySQL backends use sqlx. Python-defined databases
implement `AbstractDatabase`; the reverse bridge lets Rust call that Python object through
the same `DatabaseFs` path.

## Data Model

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

## Reads

Reading a data path runs a generated `SELECT` against the relation, converts rows to Arrow,
then encodes the result based on the path extension:

| Extension  | Bytes returned                    |
| ---------- | --------------------------------- |
| `.arrow`   | Arrow IPC stream                  |
| `.parquet` | Parquet                           |
| `.csv`     | CSV with a header                 |
| `.jsonl`   | Arrow JSON line-delimited records |
| `.sql`     | DDL or view definition text       |

`fs.query(sql, params=None)` is intentionally separate from path reads. It accepts raw SQL,
binds parameters, and returns a `pyarrow.Table`.

`open(path, "rb")` returns a Rust-backed file object with chunked `read(size)`, `seek`,
and `tell`. `cat_file()` still buffers the full encoded result.

## Writes

Writes decode incoming Arrow-compatible bytes and call `Database.insert()`:

| Operation                               | Insert mode                         |
| --------------------------------------- | ----------------------------------- |
| `open(path, "wb")`                      | truncate relation, then insert rows |
| `open(path, "ab")`                      | append rows                         |
| `pipe_file(path, bytes)`                | truncate by default                 |
| `pipe_file(path, bytes, mode="append")` | append                              |
| `put_file(local, path)`                 | truncate by default                 |
| `put_file(local, path, mode="append")`  | append                              |

`open(path, "wb")` and `open(path, "ab")` return Rust-backed write handles. Data is
committed when the file closes, and context-manager exits discard the write if an exception
is raised. Unclosed write handles are discarded rather than committed during garbage
collection. `put_file()` copies local bytes into the same Rust write path in chunks. The
current codecs still decode the completed byte stream at commit time.

DDL writes are deliberately not part of the early surface. Creating or dropping tables will
be a guarded later feature.

## Boundaries

Current native support is SQLite, PostgreSQL, and MySQL. These backends handle common
Arrow scalar types: booleans, integers, floats, UTF-8 strings, binary values, and all-null
columns. SQLite also binds temporal arrays as integer epoch values. Decimal, temporal, and
specialized PostgreSQL/MySQL types should be cast in SQL until richer Arrow mappings land.
