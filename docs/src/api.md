# API

The public Python API is intentionally small. Most users construct filesystems through
`fsspec.filesystem("db+sqlite", ...)`, `fsspec.filesystem("db+postgresql", ...)`, or
`fsspec.filesystem("db+mysql", ...)`; backend authors use `AbstractDatabase` and
`AbstractDatabaseFileSystem`.

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
