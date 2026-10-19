"""End-to-end SQL test of bronze -> silver -> gold in a throwaway `warehouse_test` database.

The `warehouse_test_dsn` fixture creates and drops that database, so the demo warehouse is
never touched. The test replays the DAG steps - DDL, bronze load, silver transforms, gold
builds, checks - through psycopg2 using the same helpers and .sql files the DAG uses, asserts
the expected outputs from `data/samples/`, and proves the loads are idempotent. It is skipped
when the sample files or the warehouse connection are not available.
"""
from __future__ import annotations

from pathlib import Path

import psycopg2
import pytest
from jinja2 import Template
from psycopg2.extras import execute_values

pytestmark = pytest.mark.integration

DS = "2000-01-01"  # a partition no real run will ever use
RUN_ID = "pytest__integration"

# name, landing file, record_path, bronze table, silver SQL (mirrors the DAG's per-source tasks)
SOURCES = [
    ("products", "products.json", (), "bronze.raw_products", "silver/products.sql"),
    ("users", "users.json", ("users",), "bronze.raw_users", "silver/users.sql"),
    ("store_catalog", "store.json", ("store",), "bronze.raw_store_catalog", "silver/store_catalog.sql"),
]


def render(sql_dir: Path, rel: str) -> str:
    return Template((sql_dir / rel).read_text()).render(ds=DS)


def run_sql_file(conn, sql_dir: Path, rel: str) -> None:
    with conn.cursor() as cur:
        cur.execute(render(sql_dir, rel))
    conn.commit()


def scalar(conn, sql: str):
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]


def rows(conn, sql: str):
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def load_bronze(conn, module, samples_dir: Path, bronze_dir: Path) -> None:
    for name, landing_file, record_path, bronze_table, _ in SOURCES:
        landed = module.land_file(name, landing_file, record_path, samples_dir, bronze_dir, DS)
        data = module.bronze_rows(landed, record_path, DS, RUN_ID)
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {bronze_table} WHERE logical_date = %s", (DS,))
            execute_values(
                cur,
                f"INSERT INTO {bronze_table} "
                "(logical_date, record_index, payload, source_file, file_sha256, dag_run_id) VALUES %s",
                data,
                template="(%s, %s, %s::jsonb, %s, %s, %s)",
            )
        conn.commit()


def run_silver_and_gold(conn, sql_dir: Path) -> None:
    for *_, silver_sql in SOURCES:
        run_sql_file(conn, sql_dir, silver_sql)
    for model in (
        "user_activity_summary",
        "product_variation_flat",
        "inventory_value_by_category",
        "product_total_value",
    ):
        run_sql_file(conn, sql_dir, f"gold/{model}.sql")


@pytest.fixture(scope="module")
def conn(warehouse_test_dsn):
    c = psycopg2.connect(warehouse_test_dsn)
    yield c
    c.close()


@pytest.fixture(scope="module", autouse=True)
def pipeline(conn, sql_dir, pipeline_module, samples_dir, tmp_path_factory):
    for ddl in ("bronze/ddl.sql", "silver/ddl.sql", "gold/ddl.sql", "gold/trace_views.sql", "ops/ddl.sql"):
        run_sql_file(conn, sql_dir, ddl)

    bronze_dir = tmp_path_factory.mktemp("bronze")
    load_bronze(conn, pipeline_module, samples_dir, bronze_dir)
    run_silver_and_gold(conn, sql_dir)
    return sql_dir


def test_bronze_partition_counts(conn):
    assert scalar(conn, f"SELECT COUNT(*) FROM bronze.raw_products WHERE logical_date = '{DS}'") == 4
    assert scalar(conn, f"SELECT COUNT(*) FROM bronze.raw_users WHERE logical_date = '{DS}'") == 2
    assert scalar(conn, f"SELECT COUNT(*) FROM bronze.raw_store_catalog WHERE logical_date = '{DS}'") == 1


def test_silver_users_normalized(conn):
    assert rows(conn, "SELECT user_id, user_name, age, email FROM silver.users ORDER BY user_id") == [
        (1, "Alice", 25, "alice@example.com"),
        (2, "Bob", 30, "bob@example.com"),
    ]
    assert rows(
        conn,
        "SELECT user_id, activity_seq, activity_type, duration_minutes "
        "FROM silver.user_activities ORDER BY user_id, activity_seq",
    ) == [
        (1, 1, "running", 30),
        (1, 2, "reading", 60),
        (2, 1, "cycling", 45),
        (2, 2, "gaming", 120),
    ]


def test_silver_store_catalog_normalized(conn):
    assert rows(conn, "SELECT store_name, location FROM silver.stores") == [("SuperMart", "Downtown")]
    assert rows(conn, "SELECT product_id, product_name, category FROM silver.store_products ORDER BY 1") == [
        ("p1", "Shirt", "Apparel"),
        ("p2", "Laptop", "Electronics"),
    ]
    assert scalar(conn, "SELECT COUNT(*) FROM silver.product_variations") == 4
    assert rows(
        conn,
        "SELECT product_id, variation_seq, spec_name, spec_value "
        "FROM silver.variation_specifications ORDER BY 1, 2, 3",
    ) == [
        ("p2", 1, "ram", "8GB"),
        ("p2", 1, "storage", "256GB"),
        ("p2", 2, "ram", "16GB"),
        ("p2", 2, "storage", "512GB"),
    ]


def test_gold_user_activity_summary(conn):
    assert rows(
        conn, "SELECT user_id, name, total_activity_duration FROM gold.user_activity_summary ORDER BY 1"
    ) == [(1, "Alice", 90), (2, "Bob", 165)]


def test_gold_product_variation_flat(conn):
    result = rows(
        conn,
        "SELECT product_id, variation_color, variation_size, variation_model, "
        "variation_specifications_ram, variation_specifications_storage, variation_price, variation_inventory "
        "FROM gold.product_variation_flat ORDER BY product_id, variation_seq",
    )
    assert [(*r[:6], float(r[6]), r[7]) for r in result] == [
        ("p1", "Red", "M", None, None, None, 19.99, 50),
        ("p1", "Blue", "L", None, None, None, 21.99, 30),
        ("p2", None, None, "X123", "8GB", "256GB", 999.99, 15),
        ("p2", None, None, "X456", "16GB", "512GB", 1299.99, 10),
    ]


def test_gold_product_total_value(conn):
    result = rows(conn, "SELECT product_id, total_value FROM gold.product_total_value ORDER BY 1")
    # price * quantity from products.json
    assert [(pid, float(v)) for pid, v in result] == [(1, 5000.0), (2, 600.0), (3, 4000.0), (4, 750.0)]


def test_gold_inventory_value_by_category(conn):
    result = rows(
        conn,
        "SELECT product_category, variation_count, total_inventory, inventory_value "
        "FROM gold.inventory_value_by_category ORDER BY 1",
    )
    assert [(c, n, inv, float(val)) for c, n, inv, val in result] == [
        ("Apparel", 2, 80, 1659.20),        # 19.99*50 + 21.99*30
        ("Electronics", 2, 25, 27999.75),  # 999.99*15 + 1299.99*10
    ]


def test_reconciliation_checks_pass(conn, pipeline):
    for check in ("checks/gold_activity_reconciliation.sql", "checks/gold_catalog_reconciliation.sql"):
        (record,) = rows(conn, render(pipeline, check))
        assert all(record), f"{check} failed: {record}"


def test_rerun_is_idempotent(conn, pipeline, pipeline_module, samples_dir, tmp_path):
    before = {
        t: scalar(conn, f"SELECT COUNT(*) FROM {t}")
        for t in (
            "silver.products",
            "silver.users",
            "silver.user_activities",
            "silver.stores",
            "silver.store_products",
            "silver.product_variations",
            "silver.variation_specifications",
            "gold.user_activity_summary",
            "gold.product_variation_flat",
            "gold.v_trace_product_total_value",
            "gold.v_trace_product_variation_flat",
            "gold.v_trace_user_activity_summary",
            "gold.v_trace_inventory_value_by_category",
        )
    }
    load_bronze(conn, pipeline_module, samples_dir, tmp_path)
    run_silver_and_gold(conn, pipeline)
    after = {t: scalar(conn, f"SELECT COUNT(*) FROM {t}") for t in before}
    assert after == before
    assert scalar(conn, f"SELECT COUNT(*) FROM bronze.raw_users WHERE logical_date = '{DS}'") == 2


# silver table -> bronze table its rows are derived from
SILVER_TO_BRONZE = [
    ("silver.products", "bronze.raw_products"),
    ("silver.users", "bronze.raw_users"),
    ("silver.user_activities", "bronze.raw_users"),
    ("silver.stores", "bronze.raw_store_catalog"),
    ("silver.store_products", "bronze.raw_store_catalog"),
    ("silver.product_variations", "bronze.raw_store_catalog"),
    ("silver.variation_specifications", "bronze.raw_store_catalog"),
]


@pytest.mark.parametrize("silver_table,bronze_table", SILVER_TO_BRONZE)
def test_silver_rows_reference_existing_bronze_records(conn, silver_table, bronze_table):
    assert scalar(conn, f"SELECT COUNT(*) FROM {silver_table}") > 0
    orphans = scalar(
        conn,
        f"SELECT COUNT(*) FROM {silver_table} s WHERE NOT EXISTS ("
        f"SELECT 1 FROM {bronze_table} b "
        "WHERE b.logical_date = s.bronze_logical_date AND b.record_index = s.bronze_record_index)",
    )
    assert orphans == 0


def test_trace_user_activity_summary_reaches_raw_user(conn):
    (alice,) = rows(
        conn,
        "SELECT source_table, payload->>'name', source_file IS NOT NULL, file_sha256 IS NOT NULL "
        "FROM gold.v_trace_user_activity_summary WHERE user_id = 1",
    )
    assert alice == ("raw_users", "Alice", True, True)
    # the summary carries the user's own bronze_* columns, so each row traces to exactly one record
    assert rows(
        conn,
        "SELECT bronze_logical_date::TEXT, bronze_record_index FROM gold.user_activity_summary ORDER BY user_id",
    ) == [(DS, 0), (DS, 1)]
    # every summary row traces to a raw user record carrying the same name
    assert scalar(conn, "SELECT COUNT(*) FROM gold.user_activity_summary") == scalar(
        conn, "SELECT COUNT(*) FROM gold.v_trace_user_activity_summary"
    )
    assert scalar(conn, "SELECT COUNT(*) FROM gold.v_trace_user_activity_summary WHERE payload->>'name' <> name") == 0


def test_trace_product_total_value_reaches_raw_product(conn):
    assert rows(
        conn,
        "SELECT product_id, payload->>'name' FROM gold.v_trace_product_total_value ORDER BY 1",
    ) == [(1, "Product_A"), (2, "Product_B"), (3, "Product_C"), (4, "Product_D")]
    assert scalar(conn, f"SELECT COUNT(*) FROM gold.v_trace_product_total_value WHERE logical_date <> '{DS}'") == 0


def test_trace_product_variation_flat_reaches_raw_store(conn):
    result = rows(
        conn,
        "SELECT product_id, variation_seq, payload->>'name' FROM gold.v_trace_product_variation_flat ORDER BY 1, 2",
    )
    assert result == [("p1", 1, "SuperMart"), ("p1", 2, "SuperMart"), ("p2", 1, "SuperMart"), ("p2", 2, "SuperMart")]


def test_trace_inventory_value_by_category_reaches_raw_store(conn):
    # aggregates trace to every contributing record: no gold row may be left untraceable
    untraceable = scalar(
        conn,
        "SELECT COUNT(*) FROM gold.inventory_value_by_category g WHERE NOT EXISTS ("
        "SELECT 1 FROM gold.v_trace_inventory_value_by_category v "
        "WHERE v.store_name = g.store_name AND v.product_category = g.product_category)",
    )
    assert untraceable == 0
    assert rows(
        conn,
        "SELECT DISTINCT product_category, payload->>'name' FROM gold.v_trace_inventory_value_by_category ORDER BY 1",
    ) == [("Apparel", "SuperMart"), ("Electronics", "SuperMart")]
