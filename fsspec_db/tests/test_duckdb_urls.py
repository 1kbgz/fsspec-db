import fsspec
import pytest

from fsspec_db import DuckDBDatabaseFileSystem


@pytest.mark.parametrize("absolute", [False, True])
@pytest.mark.parametrize("suffix", [".duckdb", ".db"])
def test_parquet_table_url_reads_and_reopens(tmp_path, monkeypatch, absolute, suffix):
    duckdb = pytest.importorskip("duckdb")
    pandas = pytest.importorskip("pandas")
    database = tmp_path / f"{tmp_path.name}{suffix}"
    with duckdb.connect(str(database)) as connection:
        connection.execute("CREATE TABLE orders AS SELECT 42 AS id")
    monkeypatch.chdir(tmp_path)
    source = database.as_posix() if absolute else database.name
    url = f"db+duckdb://{source}/main/orders.parquet"

    assert pandas.read_parquet(url).to_dict("records") == [{"id": 42}]
    fs, path = fsspec.core.url_to_fs(url)
    assert path == "/main/orders.parquet"
    assert fs.unstrip_protocol(path) == url
    assert pandas.read_parquet(fs.unstrip_protocol(path)).to_dict("records") == [{"id": 42}]
    assert pandas.read_parquet(url + "?limit=0").empty

    root_fs, root_path = fsspec.core.url_to_fs(f"db+duckdb://{source}")
    assert root_path == "/"
    assert "/main" in root_fs.ls(root_path, detail=False)


@pytest.mark.parametrize(
    ("url", "database", "path"),
    [
        ("db+duckdb://app.DUCKDB/main/orders.parquet?limit=2", "app.DUCKDB", "/main/orders.parquet?limit=2"),
        ("db+duckdb://C:/data/app.duckdb/main/orders", "C:/data/app.duckdb", "/main/orders"),
        ("db+duckdb:///data/app.duckdb/", "/data/app.duckdb", "/"),
        ("db+duckdb://data/warehouse", "data/warehouse", "/"),
        ("db+duckdb://:memory:", ":memory:", "/"),
        ("db+duckdb://", None, "/"),
    ],
)
def test_database_url_boundaries(url, database, path):
    assert DuckDBDatabaseFileSystem._get_kwargs_from_urls(url) == ({} if database is None else {"database": database})
    assert DuckDBDatabaseFileSystem._strip_protocol(url) == path


def test_bare_relation_path_is_not_a_database_url():
    assert DuckDBDatabaseFileSystem._strip_protocol("/main/orders.parquet") == "/main/orders.parquet"
