# PostgreSQL and MySQL

PostgreSQL and MySQL are native Rust backends implemented with sqlx. They use the same
filesystem path model as SQLite and are exposed as `db+postgresql`, `db+postgres`, and
`db+mysql`.

## Construction

```python
import fsspec

pg = fsspec.filesystem(
    "db+postgresql",
    dsn="postgresql://user:password@localhost:5432/app",
)

mysql = fsspec.filesystem(
    "db+mysql",
    dsn="mysql://user:password@localhost:3306/app",
)
```

URL-style construction is also supported:

```python
pg, _ = fsspec.core.url_to_fs("db+postgresql://user:password@localhost:5432/app")
mysql, _ = fsspec.core.url_to_fs("db+mysql://user:password@localhost:3306/app")
```

Both backends also accept individual connection options:

```python
pg = fsspec.filesystem(
    "db+postgresql",
    host="localhost",
    port=5432,
    database="app",
    user="user",
    password="password",
    sslmode="require",
)

mysql = fsspec.filesystem(
    "db+mysql",
    host="localhost",
    port=3306,
    database="app",
    user="user",
    password="password",
    ssl_mode="REQUIRED",
    charset="utf8mb4",
)
```

Pool sizing can be set for both networked backends with `min_connections` and
`max_connections`:

```python
pg = fsspec.filesystem(
    "db+postgresql",
    dsn="postgresql://user:password@localhost:5432/app",
    min_connections=1,
    max_connections=8,
)
```

These options also work through fsspec config files. For example, a config section named
`db+postgresql` or `db+mysql` can provide the same `host`, `user`, `password`,
`database`, `min_connections`, and `max_connections` keys; explicit constructor
arguments still win. If `dsn` or `url` is passed together with connection fields such
as `database` or `user`, the explicit source is used and those connection fields are
consumed instead of being passed through as generic fsspec storage options.

## Metadata

PostgreSQL introspection uses `information_schema` plus `pg_catalog` for indexes,
constraints, estimated row counts, and view definitions. MySQL uses `information_schema`
for schemata, tables, columns, constraints, views, and `statistics` index metadata.

```python
fs.ls("/", detail=False)
fs.ls("/public", detail=False)
fs.info("/public/users")
fs.info("/public/users/columns/id")
fs.info("/public/users/indexes/users_pkey")
```

MySQL database names appear as top-level schemas. PostgreSQL excludes `pg_catalog`,
`information_schema`, and toast schemas from root listings. MySQL excludes
`information_schema`, `mysql`, `performance_schema`, and `sys`.

## Reads and Queries

Path reads generate dialect-aware `SELECT` statements:

```python
data = pg.cat_file("/public/users.parquet")
table = mysql.query("SELECT id, name FROM users WHERE id > ?", [0])
```

Raw `query()` SQL uses the target database driver's placeholder syntax:

| Backend    | Placeholder example |
| ---------- | ------------------- |
| PostgreSQL | `WHERE id > $1`     |
| MySQL      | `WHERE id > ?`      |
| SQLite     | `WHERE id > ?`      |

## Writes

Writes decode Arrow IPC, Parquet, CSV, or JSONL bytes and insert Arrow batches into the
target relation.

```python
with pg.open("/public/users.arrow", "ab") as file:
    file.write(arrow_ipc_bytes)

mysql.put_file("rows.parquet", "/app/users.parquet", mode="append")
```

Overwrite writes replace table contents before inserting incoming rows. PostgreSQL uses
`TRUNCATE TABLE` for overwrite mode. MySQL uses `DELETE FROM` so the write remains in the
same transaction as the insert.

## Type Handling

The sqlx backends convert common database scalar types into Arrow:

| Backend    | Database type family                    | Arrow type |
| ---------- | --------------------------------------- | ---------- |
| PostgreSQL | `BOOL`                                  | `bool`     |
| PostgreSQL | `INT2`, `INT4`, `INT8`, `OID`           | `int64`    |
| PostgreSQL | `FLOAT4`, `FLOAT8`                      | `float64`  |
| PostgreSQL | `BYTEA`                                 | `binary`   |
| PostgreSQL | `TEXT`, `VARCHAR`, `CHAR`, `BPCHAR`     | `utf8`     |
| MySQL      | `BOOLEAN`                               | `bool`     |
| MySQL      | signed and unsigned integer families    | `int64`    |
| MySQL      | `FLOAT`, `DOUBLE`                       | `float64`  |
| MySQL      | `BINARY`, `VARBINARY`, `*BLOB`          | `binary`   |
| MySQL      | `CHAR`, `VARCHAR`, `*TEXT`, JSON, enums | `utf8`     |

Unsigned MySQL integers that do not fit in Arrow `int64` return an error. Decimal,
temporal, geometry, and other specialized types should be cast in SQL until richer Arrow
type mappings land.

## Arrow Extraction Boundary

All native backends expose query results through `Database::query() -> RecordBatchStream`.
The current sqlx drivers build Arrow from rows. The Rust trait also records an
`ArrowExtraction` strategy so future native-Arrow readers such as connector-x or
`arrow-odbc` can be added as optional driver implementations without changing fsspec path
semantics.
