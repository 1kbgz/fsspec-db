# The extension links libfsspec from the fsspec_rs package, which puts its lib/ on the DLL path.
import fsspec_rs

from .duckdb import DuckDBDatabase, DuckDBDatabaseFileSystem
from .fsspec_db import linked_to_libfsspec
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
