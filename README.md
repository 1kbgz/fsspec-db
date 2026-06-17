# fsspec-db

Database tables and views as fsspec filesystems.

[![Build Status](https://github.com/1kbgz/fsspec-db/actions/workflows/build.yaml/badge.svg?branch=main&event=push)](https://github.com/1kbgz/fsspec-db/actions/workflows/build.yaml)
[![codecov](https://codecov.io/gh/1kbgz/fsspec-db/branch/main/graph/badge.svg)](https://codecov.io/gh/1kbgz/fsspec-db)
[![License](https://img.shields.io/github/license/1kbgz/fsspec-db)](https://github.com/1kbgz/fsspec-db)
[![PyPI](https://img.shields.io/pypi/v/fsspec-db.svg)](https://pypi.python.org/pypi/fsspec-db)

## Overview

`fsspec-db` exposes SQL databases through familiar fsspec operations:

- `ls("/")` lists schemas.
- `ls("/main")` lists tables and views.
- `info("/main/users/columns/id")` returns column metadata.
- `cat_file("/main/users.parquet")` materializes a table as bytes.
- `query("SELECT ...")` returns a `pyarrow.Table`.
- `open("/main/users.arrow", "wb")` or `put_file(..., "/main/users.parquet")` inserts rows.

Native backends are registered as `db+sqlite`, `db+postgresql` / `db+postgres`, and
`db+mysql`.

> [!WARNING]
> Database overwrite writes replace table contents. `open(path, "wb")`, `pipe_file`, and
> default `put_file` run truncate semantics before inserting incoming rows. Use `"ab"` or
> `mode="append"` to append instead.

## Install

```bash
pip install fsspec-db
```

## Quick Start

```python
import fsspec
import pyarrow as pa
import pyarrow.ipc as ipc

fs = fsspec.filesystem("db+sqlite", database="app.db")

print(fs.ls("/", detail=False))
print(fs.ls("/main", detail=False))
print(fs.info("/main/users"))

table = fs.query("SELECT id, name FROM users WHERE id > ?", [0])

with fs.open("/main/users.arrow", "ab") as file:
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, pa.table({"name": ["ada"]}).schema) as writer:
        writer.write_table(pa.table({"name": ["ada"]}))
    file.write(sink.getvalue().to_pybytes())
```

## Path Shape

```text
/main
/main/users
/main/users/columns/id
/main/users/indexes/idx_users_name
/main/users.arrow
/main/users.parquet
/main/active_users/definition.sql
```

## Documentation

The full yardang/Sphinx docs cover concepts, path semantics, native SQL backends,
Python-defined backends, and the API reference.
