import gc
import io
import json
import re
import sqlite3
from pathlib import Path

import fsspec
import fsspec.config as fsspec_config
import fsspec.dircache as fsspec_dircache
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.ipc as ipc
import pyarrow.json as pajson
import pyarrow.parquet as pq
import pytest

import fsspec_db.mysql as mysql_mod
import fsspec_db.postgres as postgres_mod
import fsspec_db.sqlite as sqlite_mod
from fsspec_db import (
    AbstractDatabase,
    AbstractDatabaseFileSystem,
    ColumnInfo,
    ConstraintInfo,
    IndexInfo,
    MySQLDatabaseFileSystem,
    PostgresDatabaseFileSystem,
    PyDatabaseFileSystem,
    RelationInfo,
    SchemaInfo,
    SQLAlchemyDatabase,
    SQLAlchemyDatabaseFileSystem,
    SQLiteDatabaseFileSystem,
)
from fsspec_db.mysql import (
    _dsn_from_options as mysql_dsn_from_options,
    _pool_options_from_options as mysql_pool_options_from_options,
)
from fsspec_db.postgres import (
    _dsn_from_options,
    _pool_options_from_options as pg_pool_options_from_options,
)


class MockDatabase(AbstractDatabase):
    def __init__(self):
        self.queries = []
        self.params = []

    def dialect(self):
        return "sqlite"

    def list_schemas(self):
        return [SchemaInfo("main")]

    def list_relations(self, schema):
        if schema != "main":
            raise FileNotFoundError(schema)
        return [RelationInfo("users", "table", row_count=2), RelationInfo("active_users", "view")]

    def list_columns(self, schema, relation):
        self.relation_info(schema, relation)
        return [
            ColumnInfo("id", "INTEGER", False, None, 1, True),
            ColumnInfo("name", "TEXT", True, None, 2),
        ]

    def list_indexes(self, schema, relation):
        self.relation_info(schema, relation)
        return [IndexInfo("idx_users_name", ["name"], False, "btree")]

    def list_constraints(self, schema, relation):
        self.relation_info(schema, relation)
        return [ConstraintInfo("pk_users", "pk", ["id"])]

    def relation_info(self, schema, relation):
        for info in self.list_relations(schema):
            if info.name == relation:
                return info
        raise FileNotFoundError(relation)

    def view_definition(self, schema, view):
        self.relation_info(schema, view)
        return "CREATE VIEW active_users AS SELECT * FROM users"

    def query(self, sql, params=None):
        self.queries.append(sql)
        self.params.append(params)
        return pa.table({"id": [1, 2], "name": ["ada", "grace"]})

    def insert(self, schema, relation, table, mode="append"):
        self.queries.append(f"insert:{schema}.{relation}:{mode}:{table.num_rows}")
        return table.num_rows


def test_abstract_database_contract_matches_rust_trait():
    rust_database = Path(__file__).resolve().parents[2] / "rust" / "src" / "database.rs"
    trait_source = rust_database.read_text().split("pub trait Database", 1)[1]
    rust_methods = set(re.findall(r"^    fn ([a-z_]+)\(", trait_source, flags=re.MULTILINE))
    rust_only_defaults = {"arrow_extraction"}

    assert set(AbstractDatabase.__abstractmethods__) == rust_methods - rust_only_defaults


def arrow_stream_bytes(table):
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def table_format_bytes(table, fmt):
    if fmt == "arrow":
        return arrow_stream_bytes(table)
    if fmt == "parquet":
        sink = io.BytesIO()
        pq.write_table(table, sink)
        return sink.getvalue()
    if fmt == "csv":
        sink = pa.BufferOutputStream()
        pacsv.write_csv(table, sink)
        return sink.getvalue().to_pybytes()
    if fmt == "jsonl":
        return b"".join(json.dumps(row).encode() + b"\n" for row in table.to_pylist())
    raise ValueError(fmt)


def read_format_table(data, fmt):
    if fmt == "arrow":
        with ipc.open_stream(data) as reader:
            return reader.read_all()
    if fmt == "parquet":
        return pq.read_table(io.BytesIO(data))
    if fmt == "csv":
        return pacsv.read_csv(io.BytesIO(data))
    if fmt == "jsonl":
        return pajson.read_json(io.BytesIO(data))
    raise ValueError(fmt)


def test_python_filesystem_lists_and_infos():
    fs = AbstractDatabaseFileSystem(MockDatabase())

    assert fs.ls("/", detail=False) == ["/main"]
    assert fs.info("/main/users")["kind"] == "table"
    column = fs.info("/main/users/columns/id")
    assert column["data_type"] == "INTEGER"
    assert column["primary_key"] is True


def test_python_filesystem_reads_arrow():
    db = MockDatabase()
    fs = AbstractDatabaseFileSystem(db)

    data = fs.cat_file("/main/users.arrow")
    with ipc.open_stream(data) as reader:
        table = reader.read_all()

    assert table.num_rows == 2
    assert db.queries == ['SELECT * FROM "main"."users"']


def test_python_filesystem_open_read_supports_chunks_and_seek():
    db = MockDatabase()
    fs = AbstractDatabaseFileSystem(db)

    with fs.open("/main/users.arrow", "rb") as file:
        assert file.readable()
        assert file.seekable()
        assert not file.writable()
        first = file.read(8)
        assert file.tell() == 8
        rest = file.read()
        assert file.read() == b""
        assert file.size() == len(first) + len(rest)
        file.seek(0)
        data = file.read()
    assert file.closed
    assert data == first + rest

    with ipc.open_stream(data) as reader:
        table = reader.read_all()
    assert table.num_rows == 2
    assert db.queries == ['SELECT * FROM "main"."users"']


def test_python_filesystem_reads_parquet():
    fs = AbstractDatabaseFileSystem(MockDatabase())

    data = fs.cat_file("/main/users.parquet")
    table = pq.read_table(io.BytesIO(data))

    assert table.num_rows == 2


def test_python_filesystem_applies_query_params():
    db = MockDatabase()
    fs = AbstractDatabaseFileSystem(db)

    fs.cat_file("/main/users.arrow?columns=id,name&limit=1")

    assert db.queries == ['SELECT "id", "name" FROM "main"."users" LIMIT 1']


def test_python_filesystem_open_query_passes_params():
    db = MockDatabase()
    fs = AbstractDatabaseFileSystem(db)

    with ipc.open_stream(fs.open_query("SELECT ? AS id", [1]).read()) as reader:
        table = reader.read_all()

    assert table.num_rows == 2
    assert db.queries == ["SELECT ? AS id"]
    assert db.params == [[1]]


def test_python_filesystem_query_uses_bridge():
    db = MockDatabase()
    fs = AbstractDatabaseFileSystem(db)

    table = fs.query("SELECT ? AS id", [1])

    assert table.num_rows == 2
    assert db.queries == ["SELECT ? AS id"]
    assert db.params == [[1]]


@pytest.mark.parametrize("filesystem_type", [AbstractDatabaseFileSystem, PyDatabaseFileSystem])
def test_python_backend_conformance(filesystem_type):
    db = MockDatabase()
    fs = filesystem_type(db, skip_instance_cache=True)

    assert fs.ls("/", detail=False) == ["/main"]
    assert fs.info("/main/users")["kind"] == "table"
    assert fs.info("/main/users/columns/id")["primary_key"] is True
    with ipc.open_stream(fs.cat_file("/main/users.arrow?columns=id&limit=1")) as reader:
        assert reader.read_all().num_rows == 2
    assert db.queries[-1] == 'SELECT "id" FROM "main"."users" LIMIT 1'

    data = arrow_stream_bytes(pa.table({"id": [3], "name": ["lin"]}))
    fs.pipe_file("/main/users.arrow", data, mode="append")
    assert db.queries[-1] == "insert:main.users:append:1"


@pytest.mark.parametrize("filesystem_type", [AbstractDatabaseFileSystem, PyDatabaseFileSystem])
def test_python_backend_introspection_cache(filesystem_type):
    class CountingDatabase(MockDatabase):
        def __init__(self):
            super().__init__()
            self.schema_reads = 0

        def list_schemas(self):
            self.schema_reads += 1
            return super().list_schemas()

    db = CountingDatabase()
    fs = filesystem_type(db, max_paths=2, listings_expiry_time=60, skip_instance_cache=True)

    assert fs.ls("/", detail=False) == ["/main"]
    assert fs.ls("/", detail=False) == ["/main"]
    assert db.schema_reads == 1

    fs.ls("/", detail=False, refresh=True)
    assert db.schema_reads == 2

    fs.invalidate_cache()
    fs.ls("/", detail=False)
    assert db.schema_reads == 3

    data = arrow_stream_bytes(pa.table({"id": [3], "name": ["lin"]}))
    fs.pipe_file("/main/users.arrow", data, mode="append")
    fs.ls("/", detail=False)
    assert db.schema_reads == 4


def test_introspection_cache_expires_and_evicts(monkeypatch):
    class CountingDatabase(MockDatabase):
        def __init__(self):
            super().__init__()
            self.schema_reads = 0

        def list_schemas(self):
            self.schema_reads += 1
            return super().list_schemas()

    now = 100.0
    monkeypatch.setattr(fsspec_dircache.time, "time", lambda: now)
    db = CountingDatabase()
    fs = PyDatabaseFileSystem(db, max_paths=1, listings_expiry_time=10, skip_instance_cache=True)

    fs.ls("/")
    now = 111.0
    fs.ls("/")
    assert db.schema_reads == 2

    fs.ls("/main")
    fs.ls("/")
    assert db.schema_reads == 3


def test_native_filesystem_uses_introspection_cache(monkeypatch):
    class FakeRustFs:
        def __init__(self, *args, **kwargs):
            self.list_calls = 0
            self.info_calls = 0

        def ls(self, path, detail=True):
            self.list_calls += 1
            return [{"name": "/main", "size": 0, "type": "directory"}]

        def info(self, path):
            self.info_calls += 1
            return {"name": path, "size": 0, "type": "directory"}

    monkeypatch.setattr(sqlite_mod._rust, "RustSqliteDatabaseFs", FakeRustFs)
    fs = SQLiteDatabaseFileSystem(database=":memory:", skip_instance_cache=True)

    fs.ls("/")
    fs.ls("/")
    fs.info("/main")
    fs.info("/main")

    assert fs._rust.list_calls == 1
    assert fs._rust.info_calls == 1


def test_direct_python_filesystem_discards_failed_write():
    db = MockDatabase()
    fs = PyDatabaseFileSystem(db)

    with pytest.raises(RuntimeError):
        with fs.open("/main/users.arrow", "wb") as file:
            file.write(arrow_stream_bytes(pa.table({"id": [3], "name": ["lin"]})))
            raise RuntimeError("abort")

    assert not any(str(query).startswith("insert:") for query in db.queries)


def test_sqlalchemy_python_backend_and_registration(tmp_path):
    sqlalchemy = pytest.importorskip("sqlalchemy")
    database = tmp_path / "sqlalchemy.sqlite"
    engine = sqlalchemy.create_engine(f"sqlite:///{database}")
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        connection.exec_driver_sql("INSERT INTO users VALUES (1, 'ada'), (2, 'grace')")

    db = SQLAlchemyDatabase(engine=engine)
    fs = SQLAlchemyDatabaseFileSystem(engine=engine, skip_instance_cache=True)

    assert db.dialect() == "sqlite"
    assert fs.info("/main/users")["kind"] == "table"
    assert fs.info("/main/users/columns/id")["primary_key"] is True
    assert fs.query("SELECT name FROM users ORDER BY id").column("name").to_pylist() == ["ada", "grace"]
    fs.pipe_file("/main/users.arrow", arrow_stream_bytes(pa.table({"id": [3], "name": ["lin"]})), mode="append")
    assert fs.query("SELECT count(*) AS count FROM users").column("count").to_pylist() == [3]
    assert fsspec.get_filesystem_class("db+sqlalchemy") is SQLAlchemyDatabaseFileSystem


def test_native_filesystems_expose_query_helpers(monkeypatch):
    class FakeRustFs:
        def __init__(self, *args, **kwargs):
            self.calls = []

        def query_arrow(self, sql, params=None):
            self.calls.append((sql, params))
            return arrow_stream_bytes(pa.table({"id": [1]}))

    monkeypatch.setattr(sqlite_mod._rust, "RustSqliteDatabaseFs", FakeRustFs)
    monkeypatch.setattr(postgres_mod._rust, "RustPostgresDatabaseFs", FakeRustFs)
    monkeypatch.setattr(mysql_mod._rust, "RustMySqlDatabaseFs", FakeRustFs)

    filesystems = [
        SQLiteDatabaseFileSystem(database=":memory:", skip_instance_cache=True),
        PostgresDatabaseFileSystem(dsn="postgresql://localhost/app", skip_instance_cache=True),
        MySQLDatabaseFileSystem(dsn="mysql://localhost/app", skip_instance_cache=True),
    ]

    for fs in filesystems:
        assert fs.query("SELECT 1").column("id").to_pylist() == [1]
        query_file = fs.open_query("SELECT ?", [1])
        assert isinstance(query_file, io.BytesIO)
        with ipc.open_stream(query_file.read()) as reader:
            assert reader.read_all().column("id").to_pylist() == [1]
        assert fs._rust.calls == [("SELECT 1", None), ("SELECT ?", [1])]


def test_python_database_error_crosses_bridge():
    fs = AbstractDatabaseFileSystem(MockDatabase())

    try:
        fs.info("/main/missing")
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("Python FileNotFoundError should round-trip through Rust")


def test_python_database_rejects_unknown_dialect():
    class BadDialectDatabase(MockDatabase):
        def dialect(self):
            return "surprise"

    try:
        AbstractDatabaseFileSystem(BadDialectDatabase())
    except ValueError:
        pass
    else:
        raise AssertionError("unknown Python database dialect should fail fast")


def test_python_filesystem_write_uses_bridge():
    db = MockDatabase()
    fs = AbstractDatabaseFileSystem(db)
    data = arrow_stream_bytes(pa.table({"id": [3], "name": ["katherine"]}))

    with fs.open("/main/users.arrow", "wb") as file:
        assert file.writable()
        assert not file.readable()
        file.write(data[:8])
        file.write(data[8:])
        assert db.queries == []

    assert db.queries == ["insert:main.users:truncate:1"]

    fs.pipe_file("/main/users.arrow", data, mode="append")

    assert db.queries == ["insert:main.users:truncate:1", "insert:main.users:append:1"]


def test_python_filesystem_write_discards_on_context_error():
    db = MockDatabase()
    fs = AbstractDatabaseFileSystem(db)
    data = arrow_stream_bytes(pa.table({"id": [3], "name": ["katherine"]}))

    with pytest.raises(RuntimeError):
        with fs.open("/main/users.arrow", "wb") as file:
            file.write(data)
            raise RuntimeError("abort")

    assert db.queries == []


def test_python_filesystem_write_does_not_commit_on_gc():
    db = MockDatabase()
    fs = AbstractDatabaseFileSystem(db)
    data = arrow_stream_bytes(pa.table({"id": [3], "name": ["katherine"]}))

    file = fs.open("/main/users.arrow", "wb")
    file.write(data)
    del file
    gc.collect()

    assert db.queries == []


def test_python_filesystem_rejects_write_autocommit_false():
    fs = AbstractDatabaseFileSystem(MockDatabase())

    with pytest.raises(NotImplementedError, match="autocommit=False"):
        fs.open("/main/users.arrow", "wb", autocommit=False)


def test_python_filesystem_rejects_exclusive_create_at_open():
    fs = AbstractDatabaseFileSystem(MockDatabase())

    try:
        fs.open("/main/users.arrow", "xb")
    except NotImplementedError:
        pass
    else:
        raise AssertionError("exclusive create should fail at open time")


def test_python_filesystem_applies_where_query_param():
    db = MockDatabase()
    fs = AbstractDatabaseFileSystem(db)

    fs.cat_file("/main/users.arrow?where=id > 0")

    assert db.queries == ['SELECT * FROM "main"."users" WHERE id > 0']


def test_python_filesystem_decodes_where_query_param():
    db = MockDatabase()
    fs = AbstractDatabaseFileSystem(db)

    fs.cat_file("/main/users.arrow?where=name%20%3D%20%27ada%27")

    assert db.queries == ['SELECT * FROM "main"."users" WHERE name = \'ada\'']


def test_python_filesystem_rejects_injection_where_query_param():
    fs = AbstractDatabaseFileSystem(MockDatabase())

    try:
        fs.cat_file("/main/users.arrow?where=1); DROP TABLE users;--")
    except (ValueError, OSError):
        pass
    else:
        raise AssertionError("injection-style where clause must be rejected")


def test_python_filesystem_rejects_non_directory_ls():
    fs = AbstractDatabaseFileSystem(MockDatabase())

    try:
        fs.ls("/main/users.arrow")
    except NotADirectoryError:
        pass
    else:
        raise AssertionError("data file path should not be listable")


def test_rust_pyclass_to_dict():
    info = ColumnInfo("id", "INTEGER", False, None, 1, True)

    assert info.to_dict() == {
        "name": "id",
        "data_type": "INTEGER",
        "nullable": False,
        "default": None,
        "ordinal": 1,
        "primary_key": True,
        "comment": None,
    }


def test_sqlite_filesystem_registered_and_reads(tmp_path):
    path = tmp_path / "app.db"
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                score REAL
            );
            CREATE INDEX idx_users_name ON users(name);
            INSERT INTO users (name, score) VALUES ('ada', 1.5), ('grace', NULL);
            """
        )

    fs = fsspec.filesystem("db+sqlite", database=str(path))

    assert isinstance(fs, SQLiteDatabaseFileSystem)
    assert fs.ls("/", detail=False) == ["/main"]
    assert fs.info("/main/users")["kind"] == "table"
    column = fs.info("/main/users/columns/id")
    assert column["primary_key"] is True

    with ipc.open_stream(fs.cat_file("/main/users.arrow")) as reader:
        table = reader.read_all()
    assert table.num_rows == 2

    with fs.open("/main/users.arrow", "rb") as file:
        first = file.read(12)
        assert file.tell() == 12
        rest = file.read()
        file.seek(0)
        data = file.read()
    assert data == first + rest
    with ipc.open_stream(data) as reader:
        table = reader.read_all()
    assert table.column("name").to_pylist() == ["ada", "grace"]

    queried = fs.query("SELECT name FROM users WHERE id > ? ORDER BY id", [0])
    assert queried.column("name").to_pylist() == ["ada", "grace"]


def test_sqlite_filesystem_url_path_reads(tmp_path, monkeypatch):
    path = tmp_path / "app.db"
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
            INSERT INTO users (name) VALUES ('ada');
            """
        )

    monkeypatch.chdir(tmp_path)
    fs, token = fsspec.core.url_to_fs("db+sqlite://app.db")

    assert token == "app.db"
    assert isinstance(fs, SQLiteDatabaseFileSystem)
    assert fs.database == "app.db"
    assert fs.query("SELECT name FROM users").column("name").to_pylist() == ["ada"]


def test_postgres_filesystem_url_parsing_and_registration():
    assert fsspec.get_filesystem_class("db+postgresql") is PostgresDatabaseFileSystem
    assert PostgresDatabaseFileSystem._get_kwargs_from_urls("db+postgresql://user:pass@localhost:5432/app?sslmode=require") == {
        "dsn": "postgresql://user:pass@localhost:5432/app?sslmode=require"
    }
    assert PostgresDatabaseFileSystem._get_kwargs_from_urls("db+postgres://localhost/app") == {"dsn": "postgres://localhost/app"}

    options = {
        "host": "localhost",
        "port": 5432,
        "database": "app",
        "user": "ada",
        "password": "secret",
        "sslmode": "require",
    }
    assert _dsn_from_options(options) == ("postgresql://ada:secret@localhost:5432/app?sslmode=require")
    assert options == {}

    options = {"min_connections": "1", "max_pool_size": 4}
    assert pg_pool_options_from_options(options) == {"min_connections": 1, "max_connections": 4}
    assert options == {}
    with pytest.raises(ValueError, match="min_connections"):
        pg_pool_options_from_options({"min_connections": 3, "max_connections": 2})


def test_mysql_filesystem_url_parsing_and_registration():
    assert fsspec.get_filesystem_class("db+mysql") is MySQLDatabaseFileSystem
    assert MySQLDatabaseFileSystem._get_kwargs_from_urls("db+mysql://user:pass@localhost:3306/app?ssl-mode=REQUIRED") == {
        "dsn": "mysql://user:pass@localhost:3306/app?ssl-mode=REQUIRED"
    }

    options = {
        "host": "localhost",
        "port": 3306,
        "database": "app",
        "user": "ada",
        "password": "secret",
        "ssl_mode": "REQUIRED",
        "charset": "utf8mb4",
    }
    assert mysql_dsn_from_options(options) == ("mysql://ada:secret@localhost:3306/app?ssl-mode=REQUIRED&charset=utf8mb4")
    assert options == {}

    options = {"min_pool_size": "0", "max_connections": "5"}
    assert mysql_pool_options_from_options(options) == {"min_connections": 0, "max_connections": 5}
    assert options == {}
    with pytest.raises(ValueError, match="max_connections"):
        mysql_pool_options_from_options({"max_connections": 0})


def test_postgres_fsspec_config_connection_options(monkeypatch):
    class FakeRustPostgresFs:
        def __init__(self, source, **kwargs):
            self.source = source
            self.kwargs = kwargs

    PostgresDatabaseFileSystem.clear_instance_cache()
    monkeypatch.setattr(postgres_mod._rust, "RustPostgresDatabaseFs", FakeRustPostgresFs)
    monkeypatch.setitem(
        fsspec_config.conf,
        "db+postgresql",
        {
            "host": "localhost",
            "database": "app",
            "user": "ada",
            "password": "secret",
            "min_connections": "1",
            "max_connections": "4",
        },
    )

    fs = PostgresDatabaseFileSystem(skip_instance_cache=True)

    assert fs.dsn == "postgresql://ada:secret@localhost/app"
    assert fs.pool_options == {"min_connections": 1, "max_connections": 4}
    assert fs._rust.source == fs.dsn
    assert fs._rust.kwargs == fs.pool_options


def test_postgres_explicit_dsn_wins_over_connection_options(monkeypatch):
    class FakeRustPostgresFs:
        def __init__(self, source, **kwargs):
            self.source = source
            self.kwargs = kwargs

    PostgresDatabaseFileSystem.clear_instance_cache()
    monkeypatch.setattr(postgres_mod._rust, "RustPostgresDatabaseFs", FakeRustPostgresFs)

    fs = PostgresDatabaseFileSystem(
        dsn="postgresql://primary/app",
        database="ignored",
        user="ada",
        password="secret",
        min_connections="2",
        skip_instance_cache=True,
    )

    assert fs.dsn == "postgresql://primary/app"
    assert fs.pool_options == {"min_connections": 2}
    assert fs._rust.source == fs.dsn
    assert fs._rust.kwargs == fs.pool_options

    options = {"database": "ignored", "user": "ada", "password": "secret"}
    assert _dsn_from_options(options) is None
    assert options == {}


def test_mysql_fsspec_config_connection_options(monkeypatch):
    class FakeRustMySqlFs:
        def __init__(self, source, **kwargs):
            self.source = source
            self.kwargs = kwargs

    MySQLDatabaseFileSystem.clear_instance_cache()
    monkeypatch.setattr(mysql_mod._rust, "RustMySqlDatabaseFs", FakeRustMySqlFs)
    monkeypatch.setitem(
        fsspec_config.conf,
        "db+mysql",
        {
            "host": "localhost",
            "database": "app",
            "user": "ada",
            "password": "secret",
            "min_pool_size": "1",
            "max_pool_size": "3",
        },
    )

    fs = MySQLDatabaseFileSystem(skip_instance_cache=True)

    assert fs.dsn == "mysql://ada:secret@localhost/app"
    assert fs.pool_options == {"min_connections": 1, "max_connections": 3}
    assert fs._rust.source == fs.dsn
    assert fs._rust.kwargs == fs.pool_options


def test_mysql_explicit_dsn_wins_over_connection_options(monkeypatch):
    class FakeRustMySqlFs:
        def __init__(self, source, **kwargs):
            self.source = source
            self.kwargs = kwargs

    MySQLDatabaseFileSystem.clear_instance_cache()
    monkeypatch.setattr(mysql_mod._rust, "RustMySqlDatabaseFs", FakeRustMySqlFs)

    fs = MySQLDatabaseFileSystem(
        dsn="mysql://primary/app",
        database="ignored",
        user="ada",
        password="secret",
        min_pool_size="2",
        skip_instance_cache=True,
    )

    assert fs.dsn == "mysql://primary/app"
    assert fs.pool_options == {"min_connections": 2}
    assert fs._rust.source == fs.dsn
    assert fs._rust.kwargs == fs.pool_options

    options = {"database": "ignored", "user": "ada", "password": "secret"}
    assert mysql_dsn_from_options(options) is None
    assert options == {}


def test_sqlite_filesystem_writes_and_puts(tmp_path):
    path = tmp_path / "app.db"
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                score REAL
            );
            INSERT INTO users (name, score) VALUES ('ada', 1.5);
            """
        )

    fs = fsspec.filesystem("db+sqlite", database=str(path))
    with fs.open("/main/users.arrow", "wb") as file:
        assert file.writable()
        assert not file.readable()
        file.write(arrow_stream_bytes(pa.table({"name": ["grace"], "score": [2.5]})))
    assert fs.query("SELECT name FROM users ORDER BY id").column("name").to_pylist() == ["grace"]

    with fs.open("/main/users.arrow", "ab") as file:
        file.write(arrow_stream_bytes(pa.table({"name": ["katherine"], "score": [3.5]})))
    assert fs.query("SELECT name FROM users ORDER BY id").column("name").to_pylist() == ["grace", "katherine"]

    local = tmp_path / "rows.parquet"
    pq.write_table(pa.table({"name": ["dorothy"], "score": [None]}), local)
    fs.put_file(str(local), "/main/users.parquet")
    assert fs.query("SELECT name, score FROM users ORDER BY id").column("name").to_pylist() == ["dorothy"]


def test_sqlite_filesystem_round_trips_read_codecs(tmp_path):
    path = tmp_path / "app.db"
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE users (
                name TEXT NOT NULL,
                score REAL NOT NULL
            );
            INSERT INTO users (name, score) VALUES ('ada', 1.5), ('grace', 2.5);
            """
        )

    fs = fsspec.filesystem("db+sqlite", database=str(path))

    for fmt in ("arrow", "parquet", "csv", "jsonl"):
        data = fs.cat_file(f"/main/users.{fmt}?columns=name,score")
        table = read_format_table(data, fmt)
        assert table.column_names == ["name", "score"]
        assert table.column("name").to_pylist() == ["ada", "grace"]
        assert table.column("score").to_pylist() == [1.5, 2.5]


def test_sqlite_filesystem_round_trips_write_codecs_and_modes(tmp_path):
    path = tmp_path / "app.db"
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE users (
                name TEXT NOT NULL,
                score REAL NOT NULL
            );
            INSERT INTO users (name, score) VALUES ('seed', 0.5);
            """
        )

    fs = fsspec.filesystem("db+sqlite", database=str(path))

    for index, fmt in enumerate(("arrow", "parquet", "csv", "jsonl"), start=1):
        first = pa.table({"name": [f"{fmt}-truncate"], "score": [float(index)]})
        second = pa.table({"name": [f"{fmt}-append"], "score": [float(index) + 0.5]})

        fs.pipe_file(f"/main/users.{fmt}", table_format_bytes(first, fmt))
        assert fs.query("SELECT name FROM users ORDER BY rowid").column("name").to_pylist() == [f"{fmt}-truncate"]

        fs.pipe_file(f"/main/users.{fmt}", table_format_bytes(second, fmt), mode="append")
        assert fs.query("SELECT name FROM users ORDER BY rowid").column("name").to_pylist() == [
            f"{fmt}-truncate",
            f"{fmt}-append",
        ]


def test_sqlite_filesystem_pushes_projection_predicate_and_limit(tmp_path):
    path = tmp_path / "app.db"
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE users (
                name TEXT NOT NULL,
                score REAL NOT NULL
            );
            INSERT INTO users (name, score) VALUES ('ada', 1.5), ('grace', 2.5), ('katherine', 3.5);
            """
        )

    fs = fsspec.filesystem("db+sqlite", database=str(path))

    data = fs.cat_file("/main/users.arrow?columns=name&where=score%20%3E%202&limit=1")
    with ipc.open_stream(data) as reader:
        table = reader.read_all()

    assert table.column_names == ["name"]
    assert table.column("name").to_pylist() == ["grace"]


def test_sqlite_filesystem_lists_view_dependency_targets(tmp_path):
    path = tmp_path / "app.db"
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE users (
                name TEXT NOT NULL
            );
            CREATE TABLE teams (
                name TEXT NOT NULL
            );
            CREATE VIEW user_teams AS
                SELECT users.name AS user_name, teams.name AS team_name
                FROM users JOIN teams ON 1 = 1;
            """
        )

    fs = fsspec.filesystem("db+sqlite", database=str(path))

    assert fs.info("/main/user_teams")["kind"] == "view"
    assert fs.cat_file("/main/user_teams/definition.sql").startswith(b"CREATE VIEW user_teams")
    assert set(fs.ls("/main/user_teams/depends_on", detail=False)) == {
        "/main/user_teams/depends_on/teams",
        "/main/user_teams/depends_on/users",
    }
    assert fs.info("/main/user_teams/depends_on/users")["target"] == "/main/users"
    assert fs.info("/main/user_teams/depends_on/teams")["target"] == "/main/teams"


def test_sqlite_filesystem_rejects_unknown_insert_column(tmp_path):
    path = tmp_path / "app.db"
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
            """
        )

    fs = fsspec.filesystem("db+sqlite", database=str(path))

    try:
        fs.pipe_file("/main/users.arrow", arrow_stream_bytes(pa.table({"missing": [1]})))
    except ValueError as exc:
        assert "column not found" in str(exc)
    else:
        raise AssertionError("unknown insert columns should become ValueError")
