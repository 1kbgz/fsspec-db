import io
import json
import sqlite3

import fsspec
import pyarrow.parquet as pq
import pytest
from fsspec_data import DataViewFileSystem, Facet

from fsspec_db import SQLAlchemyDatabaseFileSystem, SQLiteDatabaseFileSystem
from fsspec_db.spec import _server_url

SHOP = """
create table customers(id integer primary key, name text not null);
create table orders(id integer primary key, customer_id integer references customers(id), amount real);
create index orders_customer on orders(customer_id);
insert into customers values (1, 'a'), (2, 'b');
insert into orders values (1, 1, 10), (2, 2, 20), (3, 1, 5);
create view big_orders as select * from orders where amount > 8;
"""


@pytest.fixture(params=["native", "sqlalchemy"])
def open_shop(request, tmp_path):
    database = tmp_path / "shop.db"
    connection = sqlite3.connect(database)
    connection.executescript(SHOP)
    connection.commit()
    connection.close()

    def open_shop(**options):
        if request.param == "native":
            return fsspec.core.url_to_fs(f"db+sqlite://{database}", skip_instance_cache=True, **options)[0]
        sqlalchemy = pytest.importorskip("sqlalchemy")
        return SQLAlchemyDatabaseFileSystem(engine=sqlalchemy.create_engine(f"sqlite:///{database}"), skip_instance_cache=True, **options)

    return open_shop


@pytest.fixture
def shop(open_shop):
    return open_shop()


def test_database_filesystems_are_data_sources(shop):
    assert isinstance(shop, (SQLiteDatabaseFileSystem, SQLAlchemyDatabaseFileSystem))
    assert shop.describe("/main/orders").facets == {Facet.CHILDREN, Facet.TABLE, Facet.METADATA}
    assert shop.describe("/main/orders/columns").facets == {Facet.CHILDREN}
    assert shop.describe("/main/orders/columns/amount").facets == {Facet.METADATA}
    assert shop.describe("/main/big_orders/definition.sql").facets == {Facet.RAW}
    assert sorted(node.path for node in shop.children("/main")) == ["/main/big_orders", "/main/customers", "/main/orders"]
    assert shop.schema("/main/orders").names == ["id", "customer_id", "amount"]
    assert sum(batch.num_rows for batch in shop.scan("/main/orders")) == 3
    if isinstance(shop, SQLiteDatabaseFileSystem):
        # Only the native backends have a records view; its rows are the relation's.
        assert shop.describe("/main/orders/records").facets == {Facet.CHILDREN, Facet.TABLE}
        assert sum(batch.num_rows for batch in shop.scan("/main/orders/records")) == 3
    assert shop.metadata("/main/orders/columns/id")["primary_key"] is True


def test_a_data_view_renders_relations_and_their_metadata(shop):
    view = DataViewFileSystem(fs=shop)
    assert view.ls("main", detail=False) == [
        f"main/{relation}{suffix}"
        for relation in ("big_orders", "customers", "orders")
        for suffix in ("", ".metadata.json", ".parquet", ".schema.json")
    ]
    assert pq.read_table(io.BytesIO(view.cat_file("main/big_orders.parquet"))).column("id").to_pylist() == [1, 2]
    columns = json.loads(view.cat_file("main/orders/columns.json"))
    assert [(column["name"], column["primary_key"]) for column in columns] == [("id", True), ("customer_id", False), ("amount", False)]
    assert [index["name"] for index in json.loads(view.cat_file("main/orders/indexes.json"))] == ["orders_customer"]
    assert {constraint["kind"] for constraint in json.loads(view.cat_file("main/orders/constraints.json"))} >= {"pk"}
    assert json.loads(view.cat_file("main/orders.metadata.json"))["kind"] == "table"
    assert view.cat_file("main/big_orders/definition.sql").startswith(b"CREATE VIEW big_orders")
    assert view.resolve("main/orders.parquet").node.path == "/main/orders"


def test_source_urls_reopen_the_same_database_and_path(tmp_path):
    database = tmp_path / "shop.db"
    sqlite3.connect(database).executescript(SHOP)
    fs = fsspec.filesystem("db+sqlite", database=str(database), skip_instance_cache=True)

    url = fs.unstrip_protocol("/main/orders")
    assert url == f"db+sqlite://{database}/main/orders"
    reopened, path = fsspec.core.url_to_fs(url, skip_instance_cache=True)
    assert (reopened.database, path) == (str(database), "/main/orders")
    assert fs.unstrip_protocol("/") == f"db+sqlite://{database}"
    assert DataViewFileSystem(fs=fs).resolve("main/orders.parquet").url == url


@pytest.mark.parametrize(
    ("dsn", "path", "url"),
    [
        ("postgresql://ada:secret@db.example:5432/app", "/public/kv", "db+postgresql://ada@db.example:5432/app/public/kv"),
        ("postgresql://db.example/app", "/", "db+postgresql://db.example/app"),
        ("mysql://root@127.0.0.1:3306/app", "app/kv", "db+mysql://root@127.0.0.1:3306/app/app/kv"),
        ("host=db.example dbname=app", "/public/kv", None),
    ],
)
def test_server_urls_name_the_database_without_its_password(dsn, path, url):
    protocol = "db+mysql" if dsn.startswith("mysql") else "db+postgresql"
    assert _server_url(protocol, dsn, path) == url


def test_column_metadata_has_arrow_types_and_optional_exact_statistics(open_shop):
    columns = {column["name"]: column for column in json.loads(DataViewFileSystem(fs=open_shop()).cat_file("main/orders/columns.json"))}
    assert {name: column["arrow_type"] for name, column in columns.items()} == {"id": "int64", "customer_id": "int64", "amount": "double"}
    assert "distinct_count" not in columns["amount"]

    fs = open_shop(column_statistics="exact")
    assert {key: fs.metadata("/main/orders/columns/customer_id")[key] for key in ("distinct_count", "null_count", "min", "max")} == {
        "distinct_count": 2,
        "null_count": 0,
        "min": 1,
        "max": 2,
    }
    assert fs.metadata("/main/orders/columns/amount")["max"] == 20.0
    reads = []
    query = fs.query
    fs.query = lambda sql, params=None: reads.append(sql) or query(sql, params)
    fs.metadata("/main/orders/columns/id")
    assert reads == []
    fs.invalidate_cache()
    fs.metadata("/main/orders/columns/id")
    assert len(reads) == 1
