import os
import sqlite3
import uuid

import fsspec
import pytest
from fsspec_rs.testing.records import *

SERVERS = {
    "postgres": (
        "FSSPEC_DB_POSTGRES_URL",
        "public",
        "CREATE TABLE public.{table} (ns TEXT, name TEXT, value BYTEA NOT NULL, PRIMARY KEY (ns, name))",
    ),
    "mysql": (
        "FSSPEC_DB_MYSQL_URL",
        "fsspec",
        (
            "CREATE TABLE fsspec.{table} (ns VARCHAR(191) COLLATE utf8mb4_bin, name VARCHAR(191) COLLATE utf8mb4_bin,"
            " value LONGBLOB NOT NULL, PRIMARY KEY (ns, name))"
        ),
    ),
}


@pytest.fixture(params=["sqlite", "postgres", "mysql"])
def backend(request, tmp_path):
    if request.param == "sqlite":
        database = tmp_path / "state.db"
        with sqlite3.connect(database) as conn:
            conn.execute("CREATE TABLE kv (ns TEXT, name TEXT, value BLOB NOT NULL, PRIMARY KEY (ns, name))")
        yield f"db+sqlite://{database}/main/kv/records", {}
        return

    env, schema, create = SERVERS[request.param]
    url = os.environ.get(env)
    if not url:
        pytest.skip(f"{env} not set")
    table = f"records_{uuid.uuid4().hex[:12]}"
    fs = fsspec.filesystem(f"db+{url.split('://')[0]}", dsn=url, skip_instance_cache=True)
    fs.query(create.format(table=table))
    try:
        yield f"db+{url}/{schema}/{table}/records", {}
    finally:
        fs.query(f"DROP TABLE {schema}.{table}")


@pytest.fixture
def records_url(backend):
    return backend[0]


@pytest.fixture
def records_storage_options(backend):
    return backend[1]


def test_records_listings_are_not_cached(tmp_path):
    database = tmp_path / "state.db"
    with sqlite3.connect(database) as conn:
        conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
    reader = fsspec.filesystem("db+sqlite", database=str(database), skip_instance_cache=True)
    writer = fsspec.filesystem("db+sqlite", database=str(database), skip_instance_cache=True)

    assert reader.ls("/main/users/records", detail=False) == []
    writer.pipe_file("/main/users/records/1.json", b'{"name": "ada"}')

    assert reader.ls("/main/users/records", detail=False) == ["/main/users/records/1.json"]
    assert reader.cat_file("/main/users/records/1.json") == b'{"id":1,"name":"ada"}'
    assert reader.info("/main/users/records/1.json")["size"] == 21
    assert "/main/users/records" in reader.ls("/main/users", detail=False)

    writer.rm("/main/users/records/1.json")
    assert not reader.exists("/main/users/records/1.json")


def test_records_need_a_rust_backed_filesystem():
    from fsspec_db import PyDatabaseFileSystem
    from fsspec_db.tests.test_all import MockDatabase

    with pytest.raises(NotImplementedError, match="records"):
        PyDatabaseFileSystem(MockDatabase()).rm_file("/main/users/records/1.json")
