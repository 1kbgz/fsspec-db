# API

The public Python API is intentionally small. Most users construct a registered backend
through `fsspec.filesystem(...)`; backend authors use `AbstractDatabase` and one of the
Python filesystem adapters.

## Package Exports

```{eval-rst}
.. automodule:: fsspec_db
   :members:
```

## Base Classes

```{eval-rst}
.. automodule:: fsspec_db.spec
   :members: AbstractDatabase, AbstractDatabaseFileSystem, DBFile
   :show-inheritance:
```

`AbstractDatabaseFileSystem` and the native SQLite, PostgreSQL, and MySQL filesystem
classes expose `query(sql, params=None)` for `pyarrow.Table` results and
`open_query(sql, params=None)` for Arrow IPC stream bytes.

## Python Filesystem Adapter

```{eval-rst}
.. automodule:: fsspec_db.python
   :members: PyDatabaseFileSystem
   :show-inheritance:
```

## SQLite Backend

```{eval-rst}
.. automodule:: fsspec_db.sqlite
   :members: SQLiteDatabaseFileSystem
   :show-inheritance:
```

## PostgreSQL Backend

```{eval-rst}
.. automodule:: fsspec_db.postgres
   :members: PostgresDatabaseFileSystem
   :show-inheritance:
```

## MySQL Backend

```{eval-rst}
.. automodule:: fsspec_db.mysql
   :members: MySQLDatabaseFileSystem
   :show-inheritance:
```

## DuckDB Backend

```{eval-rst}
.. automodule:: fsspec_db.duckdb
   :members: DuckDBDatabase, DuckDBDatabaseFileSystem
   :show-inheritance:
```

## SQLAlchemy Backend

```{eval-rst}
.. automodule:: fsspec_db.sqlalchemy
   :members: SQLAlchemyDatabase, SQLAlchemyDatabaseFileSystem
   :show-inheritance:
```

## ODBC Backend

```{eval-rst}
.. automodule:: fsspec_db.odbc
   :members: OdbcDatabase, OdbcDatabaseFileSystem
   :show-inheritance:
```
