# Backend configuration and types

## Protocols and construction

| Backend    | Protocol                       | Source options                                                                                                             |
| ---------- | ------------------------------ | -------------------------------------------------------------------------------------------------------------------------- |
| SQLite     | `db+sqlite`                    | Required `database`, or `path` / `url` alias.                                                                              |
| PostgreSQL | `db+postgresql`, `db+postgres` | `dsn`, `url`, or connection fields.                                                                                        |
| MySQL      | `db+mysql`                     | `dsn`, `url`, or connection fields.                                                                                        |
| DuckDB     | `db+duckdb`                    | `database=":memory:"` or existing `connection`. Requires the `duckdb` extra.                                               |
| SQLAlchemy | `db+sqlalchemy`                | `url` or existing `engine`. Requires SQLAlchemy and a backend driver.                                                      |
| ODBC       | `db+odbc`                      | Required `connection_string`; optional `user`, `password`, `batch_size=65535`. Requires the `odbc` extra and an OS driver. |

SQLite accepts a path, `:memory:`, or SQLite URL. Network backends accept `host`,
`port`, `database`, `user`, and `password`. PostgreSQL accepts `sslmode`; MySQL
accepts `ssl_mode` and `charset`. Both accept `min_connections` and
`max_connections`. An explicit DSN or URL takes precedence over connection fields.
fsspec config sections use the backend protocol name; explicit constructor
arguments take precedence.

## Parameters

Raw `query(sql, params=None)` uses driver-specific placeholders:

| Backend    | Placeholder     |
| ---------- | --------------- |
| SQLite     | `?`             |
| PostgreSQL | `$1`, `$2`, ... |
| MySQL      | `?`             |

SQLAlchemy uses its driver's SQL parameter conventions; DuckDB uses `?`. ODBC
parameters are passed to its driver. The bridge accepts scalar parameters of
`None`, `bool`, `int`, `float`, `str`, and `bytes`.

## SQLite mapping

SQLite values are converted into Arrow arrays with a small affinity mapper. Query results use
declared column metadata plus observed non-null runtime value types, so NULL-first and mixed
numeric expression columns can still produce numeric Arrow arrays.

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

## PostgreSQL and MySQL mapping

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

Additional native output mappings are:

| Backend    | Database type                      | Arrow type                                                |
| ---------- | ---------------------------------- | --------------------------------------------------------- |
| PostgreSQL | `DATE`                             | `date32`                                                  |
| PostgreSQL | `TIME`                             | `time64[us]`                                              |
| PostgreSQL | `TIMESTAMP`, `TIMESTAMPTZ`         | `timestamp[us]` without timezone metadata                 |
| PostgreSQL | `NUMERIC`, `UUID`, `JSON`, `JSONB` | `utf8` containing the decoded value's text representation |
| MySQL      | `DATE`                             | `date32`                                                  |
| MySQL      | `DATETIME`, `TIMESTAMP`            | `timestamp[us]` without timezone metadata                 |
| MySQL      | `DECIMAL`, `JSON`                  | `utf8` containing the decoded value's text representation |

PostgreSQL `NAME` also maps to UTF-8. MySQL `YEAR` and `BIT` follow the integer
mapping; `SET` follows the string mapping. Unsigned MySQL integers that do not fit
in Arrow `int64` return an error. Non-null output types absent from these mappings
raise an unsupported-type error; explicit SQL casts can select a supported type.

## Metadata and writes

SQLite introspection uses `PRAGMA database_list`, `sqlite_master`, `table_info`,
`index_list`, `index_info`, and `foreign_key_list`. Schema listings do not count
every relation; relation lookups inspect the requested relation.

PostgreSQL uses `information_schema` and `pg_catalog`. Root listings exclude
`pg_catalog`, `information_schema`, and toast schemas. MySQL uses
`information_schema` and excludes `information_schema`, `mysql`,
`performance_schema`, and `sys` from root listings. MySQL database names are
schema directories.

Native PostgreSQL overwrite uses `TRUNCATE TABLE`; MySQL and SQLite use
`DELETE FROM` in the insertion transaction. Python adapter atomicity depends on
the adapter. ODBC lists no indexes and does not provide portable view definitions.

Native queries expose `RecordBatchStream` through the Rust `Database` trait. The
current sqlx backends construct Arrow arrays from rows. `ArrowExtraction` records
the extraction strategy without changing path semantics.
