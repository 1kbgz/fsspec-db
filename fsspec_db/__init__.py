from importlib import import_module

# The extension links libfsspec from the fsspec_rs package, which puts its lib/ on the DLL path.
import fsspec_rs

from .duckdb import DuckDBDatabase, DuckDBDatabaseFileSystem
from .mysql import MySQLDatabaseFileSystem
from .odbc import OdbcDatabase, OdbcDatabaseFileSystem
from .postgres import PostgresDatabaseFileSystem
from .python import PyDatabaseFileSystem
from .spec import (
    AbstractDatabase,
    AbstractDatabaseFileSystem,
    ColumnInfo,
    ConstraintInfo,
    DBFile,
    IndexInfo,
    RelationInfo,
    SchemaInfo,
)
from .sqlalchemy import SQLAlchemyDatabase, SQLAlchemyDatabaseFileSystem
from .sqlite import SQLiteDatabaseFileSystem

linked_to_libfsspec = import_module(".fsspec_db", __package__).linked_to_libfsspec

__version__ = "0.3.2"

__all__ = [
    "AbstractDatabase",
    "AbstractDatabaseFileSystem",
    "ColumnInfo",
    "ConstraintInfo",
    "DBFile",
    "DuckDBDatabase",
    "DuckDBDatabaseFileSystem",
    "IndexInfo",
    "MySQLDatabaseFileSystem",
    "OdbcDatabase",
    "OdbcDatabaseFileSystem",
    "PostgresDatabaseFileSystem",
    "PyDatabaseFileSystem",
    "RelationInfo",
    "SQLAlchemyDatabase",
    "SQLAlchemyDatabaseFileSystem",
    "SQLiteDatabaseFileSystem",
    "SchemaInfo",
    "__version__",
    "linked_to_libfsspec",
]
