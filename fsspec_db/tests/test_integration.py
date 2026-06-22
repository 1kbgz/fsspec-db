"""Live Postgres/MySQL integration tests.

Gated on ``FSSPEC_DB_POSTGRES_URL`` / ``FSSPEC_DB_MYSQL_URL`` (set by
``make test-integration`` after ``make dbs-up``). Each test seeds a fixture
table with SQLAlchemy, then exercises the registered fsspec backend through the
Rust bridge. Skipped when the env var or the optional drivers are unavailable.

    pip install -e .[integration]
    make dbs-up dbs-wait
    make test-integration-py
"""

import os

import fsspec
import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

sqlalchemy = pytest.importorskip("sqlalchemy")


def _arrow_stream_bytes(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _seed(engine, schema: str) -> None:
    qualified = f"{schema}.fsspec_db_pyusers" if schema != "fsspec" else "fsspec_db_pyusers"
    autoinc = "BIGSERIAL" if engine.dialect.name == "postgresql" else "BIGINT AUTO_INCREMENT"
    view_qualified = f"{schema}.fsspec_db_pyview" if schema != "fsspec" else "fsspec_db_pyview"
    with engine.begin() as conn:
        conn.execute(sqlalchemy.text(f"DROP VIEW IF EXISTS {view_qualified}"))
        conn.execute(sqlalchemy.text(f"DROP TABLE IF EXISTS {qualified}"))
        conn.execute(
            sqlalchemy.text(
                f"CREATE TABLE {qualified} (  id {autoinc} PRIMARY KEY,  name VARCHAR(255) NOT NULL,  score DOUBLE PRECISION)"
                if engine.dialect.name == "postgresql"
                else f"CREATE TABLE {qualified} (  id {autoinc} PRIMARY KEY,  name VARCHAR(255) NOT NULL,  score DOUBLE)"
            )
        )
        conn.execute(sqlalchemy.text(f"INSERT INTO {qualified} (name, score) VALUES ('ada', 1.5), ('grace', 2.5)"))


CASES = {
    "postgres": {
        "env": "FSSPEC_DB_POSTGRES_URL",
        "protocol": "db+postgresql",
        "schema": "public",
        "sa_scheme": "postgresql+psycopg",
        "driver_module": "psycopg",
    },
    "mysql": {
        "env": "FSSPEC_DB_MYSQL_URL",
        "protocol": "db+mysql",
        "schema": "fsspec",
        "sa_scheme": "mysql+pymysql",
        "driver_module": "pymysql",
    },
}


@pytest.mark.parametrize("case", list(CASES.values()), ids=list(CASES))
def test_backend_round_trips(case):
    url = os.environ.get(case["env"])
    if not url:
        pytest.skip(f"{case['env']} not set")
    pytest.importorskip(case["driver_module"])

    schema = case["schema"]
    table = "fsspec_db_pyusers"
    sa_url = case["sa_scheme"] + url[url.index("://") :]
    engine = sqlalchemy.create_engine(sa_url)
    try:
        _seed(engine, schema)

        fs = fsspec.filesystem(case["protocol"], dsn=url)

        # introspection
        relations = fs.ls(f"/{schema}", detail=False)
        assert f"/{schema}/{table}" in relations
        assert fs.info(f"/{schema}/{table}")["kind"] == "table"
        id_col = fs.info(f"/{schema}/{table}/columns/id")
        assert id_col["primary_key"] is True

        # read via query
        queried = fs.query(f"SELECT name FROM {schema}.{table} ORDER BY id")
        assert queried.column("name").to_pylist() == ["ada", "grace"]

        # read via materialized arrow file
        with ipc.open_stream(fs.cat_file(f"/{schema}/{table}.arrow?columns=name&limit=1")) as reader:
            assert reader.read_all().num_rows == 1

        # where= predicate pushdown (validated/canonicalized via sqlparser)
        with ipc.open_stream(fs.cat_file(f"/{schema}/{table}.arrow?where=score > 2")) as reader:
            assert reader.read_all().column("name").to_pylist() == ["grace"]

        # view depends_on facet (extracted from the view definition)
        view = "fsspec_db_pyview"
        view_qualified = f"{schema}.{view}" if schema != "fsspec" else view
        qualified_table = f"{schema}.{table}" if schema != "fsspec" else table
        with engine.begin() as conn:
            conn.execute(sqlalchemy.text(f"CREATE VIEW {view_qualified} AS SELECT id, name FROM {qualified_table}"))
        assert fs.info(f"/{schema}/{view}")["kind"] == "view"
        deps = fs.ls(f"/{schema}/{view}/depends_on", detail=False)
        assert any(dep.endswith(f"/{table}") for dep in deps)

        # write (append) round-trip
        fs.pipe_file(
            f"/{schema}/{table}.arrow",
            _arrow_stream_bytes(pa.table({"name": ["katherine", "dorothy"], "score": [3.5, 4.5]})),
            mode="append",
        )
        after = fs.query(f"SELECT name FROM {schema}.{table} ORDER BY id")
        assert after.column("name").to_pylist() == ["ada", "grace", "katherine", "dorothy"]
    finally:
        engine.dispose()
