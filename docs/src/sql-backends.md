# How to connect PostgreSQL or MySQL

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

## Read and write an existing relation

Use PostgreSQL's schema name or MySQL's database name in relation paths:

```python
import pyarrow as pa
import pyarrow.parquet as pq

orders = pg.query("SELECT id, name FROM users WHERE id > $1", [0])
data = pg.cat_file("/public/users.parquet?limit=100")
pq.write_table(pa.table({"name": ["Ada"]}), "rows.parquet")
mysql.put_file("rows.parquet", "/app/users.parquet", mode="append")
```

The target table must exist and accept the incoming columns. Use overwrite only
to replace its rows. Cast unsupported specialized types in SQL before reading.
Consult [Backend configuration and types](backends.md) for placeholders,
metadata exclusions, and native Arrow mappings.
