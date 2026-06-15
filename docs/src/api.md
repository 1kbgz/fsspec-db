# API

The public Python API is intentionally small. Most users construct filesystems through
`fsspec.filesystem("db+sqlite", ...)`; backend authors use `AbstractDatabase` and
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

## SQLite Backend

```{eval-rst}
.. automodule:: fsspec_db.sqlite
   :members: SQLiteDatabaseFileSystem
   :show-inheritance:
```
