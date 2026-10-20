"""
json_medallion_pipeline.py

Landing JSON -> immutable Bronze -> SCD-2 Silver -> released Gold, on PostgreSQL.

Where to change things:
    * Task SQL:        sql/<layer>/<name>.sql  (every task below names its own file)
    * Data checks:     sql/checks/*.sql        (SQLCheckOperator: every column of row 1 must be true)
    * JSON contracts:  schemas/<source>.v1.json
    * Python helpers:  pipeline_helpers.py     (file checks, bronze ingest, release publish/hold)

Target tables:
    * bronze.arrival, bronze.record
    * silver.customers, silver.orders, silver.order_items, silver.products, silver.categories,
      silver.product_prices, silver.rejected_record
    * gold.order_lines, gold.daily_sales_by_category, gold.customer_summary, gold.product_performance
    * ops.release, ops.destination, ops.quality_issue

Consumer views (follow ops.destination.active_release_id):
    * analytics.order_lines, analytics.daily_sales_by_category, analytics.customer_summary,
      analytics.product_performance
    * finance.daily_sales_by_category, finance.product_performance

Source data:
    * data/landing/orders.json    (order_service:   orders, customers, items)
    * data/landing/products.json  (catalog_service: products, categories, prices)

Load type:
    * Bronze: append-only ledger (ACCEPTED / QUARANTINED / DUPLICATE per delivery)
    * Silver: SCD-2 customers (effective time) and products (delivery time); orders/items upserted
    * Gold:   full rebuild into a new release; publish = flip one pointer
ETL engine: PostgreSQL (SQL files run by Airflow)

Failure policy:
    * Bad file  -> quarantined, its lane is skipped, the release is HELD and the run fails.
    * Bad record -> silver.rejected_record + ERROR issue; the rest of the file loads.
    * Gold check fails -> release HELD; consumers keep the last published release.
"""

import os
from contextlib import closing
from dataclasses import asdict
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.baseoperator import chain, cross_downstream
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.state import TaskInstanceState
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from json_medallion_pipeline.pipeline_helpers import (
    ORDERS,
    PRODUCTS,
    create_release,
    decide_release,
    hold_release,
    ingest_source,
    lane_arrivals,
    publish_release,
    release_row_counts,
)

# Change the following arguments for your DAG
DAG_ID = "json_medallion_pipeline"
DAG_DESCRIPTION = "Landing JSON -> immutable bronze -> SCD-2 silver -> released gold"
DAG_START_DATE = pendulum.datetime(2026, 1, 1, tz="UTC")
SCHEDULE = "0 2 * * 1-5"  # weekdays 02:00 UTC
TAGS = ["medallion", "postgres", "json"]
################################################################

WAREHOUSE_CONN_ID = "warehouse_postgres"
SQL_PATH = str(Path(__file__).parent / "sql")

DATA_DIR = Path(os.environ.get("PIPELINE_DATA_DIR", "/opt/airflow/data"))
LANDING_DIR = DATA_DIR / "landing"
BRONZE_DIR = DATA_DIR / "bronze"
QUARANTINE_DIR = DATA_DIR / "quarantine"

SOURCES_BY_NAME = {ORDERS.name: ORDERS, PRODUCTS.name: PRODUCTS}

# Shared by every SQL task: run the whole .sql file as ONE transaction.
SQL_TASK_ARGS = {
    "conn_id": WAREHOUSE_CONN_ID,
    "split_statements": False,
    "autocommit": False,
    "do_xcom_push": False,
}

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


# --------------------------------------------------------------------------------------
# Python callables (thin wrappers: the logic lives in pipeline_helpers.py)
# --------------------------------------------------------------------------------------
def ingest_landing_file(source, ds, run_id):
    """Record one landing file in bronze. A quarantined file skips only its own lane."""
    with closing(PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).get_conn()) as conn:
        result = ingest_source(conn, SOURCES_BY_NAME[source], LANDING_DIR, BRONZE_DIR, QUARANTINE_DIR, ds, run_id)
    if result.status == "QUARANTINED":
        raise AirflowSkipException(f"{source} quarantined: {result.reason}")
    return asdict(result)


def start_release(ds, run_id):
    """Create (or, on retry, reuse) the BUILDING release; gold SQL reads its id from XCom."""
    with closing(PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).get_conn()) as conn:
        return create_release(conn, ds, run_id)


def publish_or_hold_release(ti, ds, run_id, dag_run):
    """Publish if every lane arrived and every gold task succeeded; otherwise HOLD and fail the run."""
    release_id = ti.xcom_pull(task_ids="create_release")
    if release_id is None:
        raise AirflowFailException("create_release produced no release id")
    gold_tis = [t for t in dag_run.get_task_instances() if t.task_id.startswith("gold.")]
    checks_ok = all(t.state == TaskInstanceState.SUCCESS for t in gold_tis)

    with closing(PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).get_conn()) as conn:
        lanes = lane_arrivals(conn, ds, run_id)
        input_arrivals = {src: lane["arrival_id"] for src, lane in lanes.items()}
        row_counts = release_row_counts(conn, release_id)
        status, reason = decide_release({src: lane["ok"] for src, lane in lanes.items()}, checks_ok)
        if status == "PUBLISHED":
            publish_release(conn, release_id, row_counts, input_arrivals)
            return release_id
        hold_release(conn, release_id, reason, row_counts, input_arrivals)
    raise AirflowFailException(f"release {release_id} HELD: {reason}")


with DAG(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    doc_md=__doc__,
    start_date=DAG_START_DATE,
    schedule=SCHEDULE,
    default_args=default_args,
    template_searchpath=[SQL_PATH],
    catchup=False,
    max_active_runs=1,
    tags=TAGS,
) as dag:

    dag_start = EmptyOperator(task_id="dag_run_start")
    dag_finished = EmptyOperator(task_id="dag_finished")

    # ---------- 0. Setup: create schemas, tables and views if they do not exist ----------
    with TaskGroup("setup"):
        create_bronze_tables = SQLExecuteQueryOperator(
            task_id="create_bronze_tables", sql="bronze/ddl.sql", **SQL_TASK_ARGS
        )
        create_ops_tables = SQLExecuteQueryOperator(
            task_id="create_ops_tables", sql="ops/ddl.sql", **SQL_TASK_ARGS
        )
        create_silver_tables = SQLExecuteQueryOperator(
            task_id="create_silver_tables", sql="silver/ddl.sql", **SQL_TASK_ARGS
        )
        create_gold_tables = SQLExecuteQueryOperator(
            task_id="create_gold_tables", sql="gold/ddl.sql", **SQL_TASK_ARGS
        )
        create_trace_views = SQLExecuteQueryOperator(
            task_id="create_trace_views", sql="gold/trace_views.sql", **SQL_TASK_ARGS
        )
        create_consumer_views = SQLExecuteQueryOperator(
            task_id="create_consumer_views", sql="gold/consumer_views.sql", **SQL_TASK_ARGS
        )

    # Open a BUILDING release; gold tables are keyed by its release_id.
    create_release_task = PythonOperator(
        task_id="create_release",
        python_callable=start_release,
    )

    # ---------- 1. Bronze: hash + schema-check each landing file, record the delivery ----------
    with TaskGroup("bronze"):
        ingest_orders = PythonOperator(
            task_id="ingest_orders",
            python_callable=ingest_landing_file,
            op_kwargs={"source": "orders"},
        )
        ingest_products = PythonOperator(
            task_id="ingest_products",
            python_callable=ingest_landing_file,
            op_kwargs={"source": "products"},
        )

    # ---------- 2. Silver: typed 3NF + SCD-2, business rules reject single records ----------
    with TaskGroup("silver"):
        transform_products = SQLExecuteQueryOperator(
            task_id="transform_products", sql="silver/products.sql", **SQL_TASK_ARGS
        )
        check_products = SQLCheckOperator(
            task_id="check_products", sql="checks/silver_products.sql", conn_id=WAREHOUSE_CONN_ID
        )
        transform_orders = SQLExecuteQueryOperator(
            task_id="transform_orders", sql="silver/orders.sql", **SQL_TASK_ARGS
        )
        check_orders = SQLCheckOperator(
            task_id="check_orders", sql="checks/silver_orders.sql", conn_id=WAREHOUSE_CONN_ID
        )

    # ---------- 3. Gold: rebuild every model into this run's release, then reconcile ----------
    with TaskGroup("gold"):
        build_order_lines = SQLExecuteQueryOperator(
            task_id="build_order_lines", sql="gold/order_lines.sql", **SQL_TASK_ARGS
        )
        build_daily_sales_by_category = SQLExecuteQueryOperator(
            task_id="build_daily_sales_by_category", sql="gold/daily_sales_by_category.sql", **SQL_TASK_ARGS
        )
        build_customer_summary = SQLExecuteQueryOperator(
            task_id="build_customer_summary", sql="gold/customer_summary.sql", **SQL_TASK_ARGS
        )
        build_product_performance = SQLExecuteQueryOperator(
            task_id="build_product_performance", sql="gold/product_performance.sql", **SQL_TASK_ARGS
        )
        check_reconciliation = SQLCheckOperator(
            task_id="check_reconciliation", sql="checks/gold_reconciliation.sql", conn_id=WAREHOUSE_CONN_ID
        )

    # ---------- 4. Publish: flip the consumer pointer, or HOLD and fail the run ----------
    # ALL_DONE so it still runs (and records HELD) when a lane was skipped or a gold task failed.
    publish_release_task = PythonOperator(
        task_id="publish_release",
        python_callable=publish_or_hold_release,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ---------- Dependencies ----------
    chain(
        dag_start,
        create_bronze_tables,
        create_ops_tables,
        create_silver_tables,
        create_gold_tables,
        create_trace_views,
        create_consumer_views,
        [create_release_task, ingest_orders, ingest_products],
    )

    chain(ingest_products, transform_products, check_products)
    chain(ingest_orders, transform_orders, check_orders)
    # Silver orders parks items whose product is unknown, so it reads silver.products first.
    # Default all_success on purpose: a quarantined products lane must also skip this transform.
    check_products >> transform_orders

    gold_builds = [build_order_lines, build_daily_sales_by_category, build_customer_summary, build_product_performance]
    cross_downstream([check_orders, check_products, create_release_task], gold_builds)
    gold_builds >> check_reconciliation

    chain(check_reconciliation, publish_release_task, dag_finished)
    create_release_task >> publish_release_task
