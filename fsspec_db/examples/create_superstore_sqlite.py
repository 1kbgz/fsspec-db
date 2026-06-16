from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import superstore

DEFAULT_OUTPUT = Path(__file__).with_name("superstore.sqlite")


def create_database(output: Path, seed: int, count: int) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        output.unlink()

    rows = superstore.superstore(seed=seed, count=count)

    with sqlite3.connect(output) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(
            """
            CREATE TABLE orders (
                "Row ID" INTEGER PRIMARY KEY,
                "Order ID" TEXT NOT NULL,
                "Order Date" TEXT NOT NULL,
                "Ship Date" TEXT NOT NULL,
                "Ship Mode" TEXT NOT NULL,
                "Customer ID" TEXT NOT NULL,
                "Segment" TEXT NOT NULL,
                "Country" TEXT NOT NULL,
                "City" TEXT NOT NULL,
                "State" TEXT NOT NULL,
                "Postal Code" TEXT NOT NULL,
                "Region" TEXT NOT NULL,
                "Product ID" TEXT NOT NULL,
                "Category" TEXT NOT NULL,
                "Sub-Category" TEXT NOT NULL,
                "Item Status" TEXT NOT NULL,
                "Item Price" REAL NOT NULL,
                "Sales" INTEGER NOT NULL,
                "Quantity" INTEGER NOT NULL,
                "Discount" REAL NOT NULL,
                "Profit" REAL NOT NULL
            )
            """
        )
        rows.to_sql("orders", conn, if_exists="append", index=False)
        conn.execute('CREATE INDEX idx_orders_region ON orders ("Region")')
        conn.execute('CREATE INDEX idx_orders_category ON orders ("Category")')
        conn.execute('CREATE INDEX idx_orders_order_date ON orders ("Order Date")')
        conn.execute(
            """
            CREATE VIEW sales_by_region AS
            SELECT
                "Region",
                COUNT(*) AS order_count,
                SUM("Sales") AS total_sales,
                SUM("Profit") AS total_profit,
                AVG("Discount") AS average_discount
            FROM orders
            GROUP BY "Region"
            """
        )
        conn.execute(
            """
            CREATE VIEW profit_by_category AS
            SELECT
                "Category",
                "Sub-Category",
                COUNT(*) AS order_count,
                SUM("Quantity") AS total_quantity,
                SUM("Sales") AS total_sales,
                SUM("Profit") AS total_profit
            FROM orders
            GROUP BY "Category", "Sub-Category"
            """
        )
        conn.execute("PRAGMA user_version = 1")


def main() -> None:
    parser = argparse.ArgumentParser(description="Create a SQLite Superstore sample database.")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--count", type=int, default=1000)
    args = parser.parse_args()

    create_database(args.output, args.seed, args.count)
    print(args.output)


if __name__ == "__main__":
    main()
