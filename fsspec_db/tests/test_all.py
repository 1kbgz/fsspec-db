import io
import sqlite3

import fsspec
import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from fsspec_db import (
    AbstractDatabase,
    AbstractDatabaseFileSystem,
    ColumnInfo,
    ConstraintInfo,
    IndexInfo,
    RelationInfo,
    SchemaInfo,
    SQLiteDatabaseFileSystem,
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


def arrow_stream_bytes(table):
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


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


def test_python_database_error_crosses_bridge():
    fs = AbstractDatabaseFileSystem(MockDatabase())

    try:
        fs.info("/main/missing")
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("Python FileNotFoundError should round-trip through Rust")


def test_python_filesystem_write_uses_bridge():
    db = MockDatabase()
    fs = AbstractDatabaseFileSystem(db)

    fs.pipe_file("/main/users.arrow", arrow_stream_bytes(pa.table({"id": [3], "name": ["katherine"]})))

    assert db.queries == ["insert:main.users:truncate:1"]


def test_python_filesystem_rejects_exclusive_create_at_open():
    fs = AbstractDatabaseFileSystem(MockDatabase())

    try:
        fs.open("/main/users.arrow", "xb")
    except NotImplementedError:
        pass
    else:
        raise AssertionError("exclusive create should fail at open time")


def test_python_filesystem_rejects_unparsed_where_query_param():
    fs = AbstractDatabaseFileSystem(MockDatabase())

    try:
        fs.cat_file("/main/users.arrow?where=id=1")
    except OSError:
        pass
    else:
        raise AssertionError("where query parameter should be rejected until Phase 3")


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

    queried = fs.query("SELECT name FROM users WHERE id > ? ORDER BY id", [0])
    assert queried.column("name").to_pylist() == ["ada", "grace"]


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
        file.write(arrow_stream_bytes(pa.table({"name": ["grace"], "score": [2.5]})))
    assert fs.query("SELECT name FROM users ORDER BY id").column("name").to_pylist() == ["grace"]

    with fs.open("/main/users.arrow", "ab") as file:
        file.write(arrow_stream_bytes(pa.table({"name": ["katherine"], "score": [3.5]})))
    assert fs.query("SELECT name FROM users ORDER BY id").column("name").to_pylist() == ["grace", "katherine"]

    local = tmp_path / "rows.parquet"
    pq.write_table(pa.table({"name": ["dorothy"], "score": [None]}), local)
    fs.put_file(str(local), "/main/users.parquet")
    assert fs.query("SELECT name, score FROM users ORDER BY id").column("name").to_pylist() == ["dorothy"]


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
