# Browse a SQLite database as a filesystem

This tutorial creates a small SQLite database, browses its structure with fsspec paths,
and reads a table as Arrow data.

## Before you start

Install `fsspec-db`:

```console
python -m pip install fsspec-db
```

## Create a database

Create `create_library.py`:

```python
import sqlite3

with sqlite3.connect("library.db") as connection:
    connection.execute("CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT NOT NULL)")
    connection.executemany(
        "INSERT INTO books (title) VALUES (?)",
        [("The Left Hand of Darkness",), ("Kindred",), ("Piranesi",)],
    )
```

Run it once:

```console
python create_library.py
```

You now have a normal SQLite database named `library.db`.

## Browse its structure

Create `browse_library.py` and construct the registered SQLite filesystem:

```python
import fsspec

fs = fsspec.filesystem("db+sqlite", database="library.db")

print(fs.ls("/", detail=False))
print(fs.ls("/main", detail=False))
print(fs.ls("/main/books/columns", detail=False))
```

Run the script:

```console
python browse_library.py
```

The output shows one schema, its table, and the table's columns:

```text
['/main']
['/main/books']
['/main/books/columns/id', '/main/books/columns/title']
```

## Inspect metadata

Add these lines:

```python
book_info = fs.info("/main/books")
title_info = fs.info("/main/books/columns/title")

print(book_info["kind"])
print(title_info["data_type"], title_info["nullable"])
```

Run the script again. The final lines identify a table and its required text column:

```text
table
TEXT False
```

## Read table data

Add a bounded query:

```python
table = fs.query("SELECT id, title FROM books ORDER BY id LIMIT ?", [2])
print(table.to_pylist())
```

The result is a `pyarrow.Table`:

```text
[{'id': 1, 'title': 'The Left Hand of Darkness'}, {'id': 2, 'title': 'Kindred'}]
```

Read the same relation through a materialized filesystem path:

```python
import pyarrow.parquet as pq

with fs.open("/main/books.parquet?limit=2", "rb") as file:
    materialized = pq.read_table(file)

print(materialized.num_rows)
```

The path extension selected Parquet and the query parameter limited the read:

```text
2
```

## What you built

You browsed database metadata with filesystem operations and read rows through both SQL
and a format-selecting path. See [Paths](paths.md) for the complete namespace and
[SQLite](sqlite.md) for writing data.
