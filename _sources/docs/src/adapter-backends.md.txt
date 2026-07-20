# How to connect Python database adapters

Use these adapters when a database already exposes Arrow results in Python, is available
through SQLAlchemy, or has an ODBC driver. They share the same paths and fsspec operations
as the native Rust SQL backends.

## Connect DuckDB

Install the DuckDB extra:

```console
python -m pip install 'fsspec-db[duckdb]'
```

Open a database file:

```python
import fsspec

fs = fsspec.filesystem("db+duckdb", database="warehouse.duckdb")
print(fs.ls("/main", detail=False))
orders = fs.query("SELECT * FROM orders LIMIT ?", [100])
```

To reuse an existing connection, construct the class directly:

```python
import duckdb

from fsspec_db import DuckDBDatabaseFileSystem

connection = duckdb.connect("warehouse.duckdb")
fs = DuckDBDatabaseFileSystem(connection=connection)
```

## Connect through SQLAlchemy

Install SQLAlchemy and the driver for the target database:

```console
python -m pip install fsspec-db sqlalchemy
```

Pass a SQLAlchemy URL through the registered protocol:

```python
import fsspec

fs = fsspec.filesystem("db+sqlalchemy", url="sqlite:///warehouse.db")
print(fs.ls("/main", detail=False))
```

Reuse an engine when the application already owns connection pooling:

```python
from sqlalchemy import create_engine

from fsspec_db import SQLAlchemyDatabaseFileSystem

engine = create_engine("sqlite:///warehouse.db")
fs = SQLAlchemyDatabaseFileSystem(engine=engine)
```

## Connect through ODBC

Install the ODBC extra and an operating-system ODBC driver for the target database:

```console
python -m pip install 'fsspec-db[odbc]'
```

Pass the driver's connection string:

```python
import fsspec

fs = fsspec.filesystem(
    "db+odbc",
    connection_string="Driver={PostgreSQL Unicode};Server=localhost;Database=warehouse",
    user="analyst",
    password="secret",
    batch_size=65535,
)
```

`batch_size` controls rows requested from `arrow-odbc` per batch. Keep credentials out of
source files; supply them through the application's normal secret or configuration system.

## Use the common filesystem surface

After construction, all three adapters support the database path model:

```python
fs.ls("/", detail=False)
fs.ls("/main", detail=False)
fs.info("/main/orders/columns/order_id")
fs.cat_file("/main/orders.arrow?limit=100")
```

Backend metadata capabilities vary with the database and driver. See the [path
reference](paths.md) for common paths and [Python backends](python-backends.md) to
implement a custom adapter.
