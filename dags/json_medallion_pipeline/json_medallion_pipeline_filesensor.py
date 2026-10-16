"""Alternative trigger: wait for files in the landing zone, then run the same pipeline.

Self-contained duplicate of `json_medallion_pipeline` (no shared imports), gated by one
FileSensor per source (`fs_landing` connection; mode="reschedule" frees the worker slot
between pokes). The task graph below the sensors is identical to the primary DAG.

Layers (all in the `warehouse_postgres` database):
- **landing -> bronze files**: each source file in `data/landing/` is validated, then copied to
  `data/bronze/<source>/dt=<ds>/<file>` (sha256 + record count captured).
- **bronze (JSONB)**: one row per record in `bronze.raw_<source>`; the partition for the
  logical date is deleted and re-inserted in a single transaction, so re-runs are idempotent.
- **silver (3NF)**: `silver/*.sql` upserts normalized tables, followed by column-level checks.
- **gold**: `gold/*.sql` full-refresh consumer models, followed by reconciliation checks.
- **ops**: `record_load_audit` writes row counts for every layer into `ops.load_audit`.

Unpause whichever trigger style you want; running both for the same logical date is harmless
(idempotent) but redundant.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import asdict, dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLColumnCheckOperator,
    SQLExecuteQueryOperator,
    SQLTableCheckOperator,
)
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from psycopg2.extras import execute_values

SQL_DIR = str(Path(__file__).parent / "sql")
WAREHOUSE_CONN_ID = "warehouse_postgres"
DATA_DIR = Path(os.environ.get("PIPELINE_DATA_DIR", "/opt/airflow/data"))
LANDING_DIR = DATA_DIR / "landing"
BRONZE_DIR = DATA_DIR / "bronze"

SILVER_TABLES = [
    "silver.products",
    "silver.users",
    "silver.user_activities",
    "silver.stores",
    "silver.store_products",
    "silver.product_variations",
    "silver.variation_specifications",
]
GOLD_TABLES = [
    "gold.user_activity_summary",
    "gold.product_variation_flat",
    "gold.inventory_value_by_category",
    "gold.product_total_value",
]

# Gold model -> silver lane(s) it depends on (enables per-lane parallelism).
GOLD_DEPENDENCIES = {
    "user_activity_summary": ["users"],
    "product_total_value": ["products"],
    "product_variation_flat": ["store_catalog"],
    "inventory_value_by_category": ["store_catalog"],
}

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


# --------------------------------------------------------------------------------------
# Plain-Python helpers (no Airflow state, importable from tests)
# --------------------------------------------------------------------------------------
@dataclass(frozen=True)
class LandedFile:
    source_name: str
    source_file: str
    bronze_path: str
    sha256: str


def sha256_of_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def extract_records(document: Any, record_path: tuple[str, ...]) -> list[dict[str, Any]]:
    """Walk `record_path` into `document` and return a list of record dicts."""
    node = document
    for key in record_path:
        if not isinstance(node, dict) or key not in node:
            raise ValueError(f"record_path {record_path!r} not found in document")
        node = node[key]

    if isinstance(node, list):
        bad = [i for i, r in enumerate(node) if not isinstance(r, dict)]
        if bad:
            raise ValueError(f"non-object records at indexes {bad[:5]}")
        return node
    if isinstance(node, dict):
        return [node]
    raise ValueError(f"record node must be a list or object, got {type(node).__name__}")


def land_file(
    source_name: str,
    landing_file: str,
    record_path: tuple[str, ...],
    landing_dir: Path,
    bronze_dir: Path,
    ds: str,
) -> LandedFile:
    """Copy the landing file into <bronze_dir>/<source_name>/dt=<ds>/<landing_file>.

    The payload is validated first so malformed files never reach bronze. Re-running the
    same `ds` overwrites the partition, which keeps the file zone idempotent.
    """
    src = landing_dir / landing_file
    if not src.is_file():
        raise FileNotFoundError(f"landing file missing: {src}")

    with src.open("r", encoding="utf-8") as fh:
        document = json.load(fh)
    extract_records(document, record_path)

    partition_dir = bronze_dir / source_name / f"dt={ds}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    dest = partition_dir / landing_file
    shutil.copy2(src, dest)

    return LandedFile(
        source_name=source_name,
        source_file=landing_file,
        bronze_path=str(dest),
        sha256=sha256_of_file(dest),
    )


def read_records(landed: LandedFile, record_path: tuple[str, ...]) -> list[dict[str, Any]]:
    with Path(landed.bronze_path).open("r", encoding="utf-8") as fh:
        return extract_records(json.load(fh), record_path)


def bronze_rows(
    landed: LandedFile, record_path: tuple[str, ...], ds: str, dag_run_id: str
) -> list[tuple[str, int, str, str, str, str]]:
    """Rows shaped for bronze.raw_<source>: one per record, payload as JSON text."""
    return [
        (ds, idx, json.dumps(record, separators=(",", ":")), landed.source_file, landed.sha256, dag_run_id)
        for idx, record in enumerate(read_records(landed, record_path))
    ]


def replace_bronze_partition(table: str, rows: list[tuple], ds: str) -> int:
    """Delete the `ds` partition and insert `rows` in ONE transaction (idempotent load)."""
    hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
    with hook.get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"DELETE FROM {table} WHERE logical_date = %s", (ds,))
        execute_values(
            cur,
            f"INSERT INTO {table} "
            "(logical_date, record_index, payload, source_file, file_sha256, dag_run_id) VALUES %s",
            rows,
            template="(%s, %s, %s::jsonb, %s, %s, %s)",
        )
        conn.commit()
    return len(rows)


def _sql(task_id: str, sql, **kwargs) -> SQLExecuteQueryOperator:
    """Run SQL file(s) from the DAG's template_searchpath as ONE transaction."""
    return SQLExecuteQueryOperator(
        task_id=task_id,
        conn_id=WAREHOUSE_CONN_ID,
        sql=sql,
        split_statements=False,
        autocommit=False,
        do_xcom_push=False,
        **kwargs,
    )


# --------------------------------------------------------------------------------------
# Python tasks
# --------------------------------------------------------------------------------------
@task
def land_products(ds: str | None = None) -> dict:
    """Validate landing/products.json and copy it into data/bronze/products/dt=<ds>/."""
    return asdict(land_file("products", "products.json", (), LANDING_DIR, BRONZE_DIR, ds))


@task
def land_users(ds: str | None = None) -> dict:
    """Validate landing/users.json and copy it into data/bronze/users/dt=<ds>/."""
    return asdict(land_file("users", "users.json", ("users",), LANDING_DIR, BRONZE_DIR, ds))


@task
def land_store_catalog(ds: str | None = None) -> dict:
    """Validate landing/store.json and copy it into data/bronze/store_catalog/dt=<ds>/."""
    return asdict(land_file("store_catalog", "store.json", ("store",), LANDING_DIR, BRONZE_DIR, ds))


@task
def load_products(landed: dict, ds: str | None = None, run_id: str | None = None) -> int:
    """Replace the bronze.raw_products partition for `ds` with one JSONB row per record."""
    rows = bronze_rows(LandedFile(**landed), (), ds, run_id)
    return replace_bronze_partition("bronze.raw_products", rows, ds)


@task
def load_users(landed: dict, ds: str | None = None, run_id: str | None = None) -> int:
    """Replace the bronze.raw_users partition for `ds` with one JSONB row per record."""
    rows = bronze_rows(LandedFile(**landed), ("users",), ds, run_id)
    return replace_bronze_partition("bronze.raw_users", rows, ds)


@task
def load_store_catalog(landed: dict, ds: str | None = None, run_id: str | None = None) -> int:
    """Replace the bronze.raw_store_catalog partition for `ds` with one JSONB row per record."""
    rows = bronze_rows(LandedFile(**landed), ("store",), ds, run_id)
    return replace_bronze_partition("bronze.raw_store_catalog", rows, ds)


@task
def record_load_audit(ds: str | None = None, run_id: str | None = None) -> None:
    """Persist post-load row counts for every table touched by this run."""
    hook = PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID)
    counts: list[tuple[str, str, int]] = []

    for table in ("bronze.raw_products", "bronze.raw_users", "bronze.raw_store_catalog"):
        n = hook.get_first(f"SELECT COUNT(*) FROM {table} WHERE logical_date = %s", (ds,))[0]
        counts.append(("bronze", table, n))
    for table in SILVER_TABLES:
        counts.append(("silver", table, hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]))
    for table in GOLD_TABLES:
        counts.append(("gold", table, hook.get_first(f"SELECT COUNT(*) FROM {table}")[0]))

    with hook.get_conn() as conn, conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO ops.load_audit (dag_run_id, logical_date, layer, table_name, row_count) VALUES %s "
            "ON CONFLICT (dag_run_id, layer, table_name) "
            "DO UPDATE SET row_count = EXCLUDED.row_count, loaded_at = now()",
            [(run_id, ds, layer, table, n) for layer, table, n in counts],
        )
        conn.commit()


# --------------------------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------------------------
with DAG(
    dag_id="json_medallion_pipeline_filesensor",
    description="FileSensor-gated variant of json_medallion_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    template_searchpath=[SQL_DIR],
    tags=["medallion", "postgres", "json", "alternative"],
    doc_md=__doc__,
) as dag:
    # ---- gate: wait for every landing file before touching the warehouse -------------
    wait_for_products = FileSensor(
        task_id="wait_for_products",
        fs_conn_id="fs_landing",
        filepath="products.json",
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,
    )
    wait_for_users = FileSensor(
        task_id="wait_for_users",
        fs_conn_id="fs_landing",
        filepath="users.json",
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,
    )
    wait_for_store_catalog = FileSensor(
        task_id="wait_for_store_catalog",
        fs_conn_id="fs_landing",
        filepath="store.json",
        mode="reschedule",
        poke_interval=30,
        timeout=60 * 60,
    )

    setup_schemas = _sql(
        "setup_schemas",
        sql=["bronze/ddl.sql", "silver/ddl.sql", "gold/ddl.sql", "ops/ddl.sql"],
    )

    # ---- bronze: land files, load JSONB partitions, check row counts -----------------
    with TaskGroup("bronze", tooltip="Land raw files + load JSONB partitions"):
        landed_products = land_products()
        loaded_products = load_products(landed_products)
        check_bronze_products = SQLTableCheckOperator(
            task_id="check_products",
            conn_id=WAREHOUSE_CONN_ID,
            table="bronze.raw_products",
            partition_clause="logical_date = '{{ ds }}'",
            checks={"row_count_check": {"check_statement": "COUNT(*) > 0"}},
        )

        landed_users = land_users()
        loaded_users = load_users(landed_users)
        check_bronze_users = SQLTableCheckOperator(
            task_id="check_users",
            conn_id=WAREHOUSE_CONN_ID,
            table="bronze.raw_users",
            partition_clause="logical_date = '{{ ds }}'",
            checks={"row_count_check": {"check_statement": "COUNT(*) > 0"}},
        )

        landed_store_catalog = land_store_catalog()
        loaded_store_catalog = load_store_catalog(landed_store_catalog)
        check_bronze_store_catalog = SQLTableCheckOperator(
            task_id="check_store_catalog",
            conn_id=WAREHOUSE_CONN_ID,
            table="bronze.raw_store_catalog",
            partition_clause="logical_date = '{{ ds }}'",
            checks={"row_count_check": {"check_statement": "COUNT(*) > 0"}},
        )

    [wait_for_products, wait_for_users, wait_for_store_catalog] >> setup_schemas
    setup_schemas >> [landed_products, landed_users, landed_store_catalog]
    loaded_products >> check_bronze_products
    loaded_users >> check_bronze_users
    loaded_store_catalog >> check_bronze_store_catalog

    # ---- silver: idempotent upserts + column checks ----------------------------------
    with TaskGroup("silver", tooltip="Normalize into 3NF tables (idempotent upserts)"):
        transform_products = _sql("transform_products", sql="silver/products.sql")
        check_silver_products = SQLColumnCheckOperator(
            task_id="check_products",
            conn_id=WAREHOUSE_CONN_ID,
            table="silver.products",
            column_mapping={
                "product_id": {"null_check": {"equal_to": 0}},
                "price": {"min": {"geq_to": 0}},
                "quantity": {"min": {"geq_to": 0}},
            },
        )

        transform_users = _sql("transform_users", sql="silver/users.sql")
        check_silver_users = SQLColumnCheckOperator(
            task_id="check_users",
            conn_id=WAREHOUSE_CONN_ID,
            table="silver.users",
            column_mapping={
                "email": {"null_check": {"equal_to": 0}, "unique_check": {"equal_to": 0}},
                "age": {"min": {"geq_to": 0}},
            },
        )

        transform_store_catalog = _sql("transform_store_catalog", sql="silver/store_catalog.sql")
        check_silver_store_catalog = SQLColumnCheckOperator(
            task_id="check_store_catalog",
            conn_id=WAREHOUSE_CONN_ID,
            table="silver.product_variations",
            column_mapping={
                "price": {"min": {"geq_to": 0}},
                "inventory": {"min": {"geq_to": 0}},
            },
        )

    check_bronze_products >> transform_products >> check_silver_products
    check_bronze_users >> transform_users >> check_silver_users
    check_bronze_store_catalog >> transform_store_catalog >> check_silver_store_catalog

    # ---- gold: full-refresh models + reconciliation checks ---------------------------
    with TaskGroup("gold", tooltip="Consumer-ready models (full refresh)"):
        build_user_activity_summary = _sql(
            "build_user_activity_summary", sql="gold/user_activity_summary.sql"
        )
        build_product_total_value = _sql("build_product_total_value", sql="gold/product_total_value.sql")
        build_product_variation_flat = _sql(
            "build_product_variation_flat", sql="gold/product_variation_flat.sql"
        )
        build_inventory_value_by_category = _sql(
            "build_inventory_value_by_category", sql="gold/inventory_value_by_category.sql"
        )

        check_activity_reconciliation = SQLCheckOperator(
            task_id="check_activity_reconciliation",
            conn_id=WAREHOUSE_CONN_ID,
            sql="checks/gold_activity_reconciliation.sql",
        )
        check_catalog_reconciliation = SQLCheckOperator(
            task_id="check_catalog_reconciliation",
            conn_id=WAREHOUSE_CONN_ID,
            sql="checks/gold_catalog_reconciliation.sql",
        )

    # Mirrors GOLD_DEPENDENCIES: each gold model waits only for the silver lane it reads.
    check_silver_users >> build_user_activity_summary
    check_silver_products >> build_product_total_value
    check_silver_store_catalog >> [build_product_variation_flat, build_inventory_value_by_category]

    build_user_activity_summary >> check_activity_reconciliation
    [
        build_product_variation_flat,
        build_inventory_value_by_category,
        build_product_total_value,
    ] >> check_catalog_reconciliation

    # ---- ops: audit is the single sink -----------------------------------------------
    audit = record_load_audit()
    [check_activity_reconciliation, check_catalog_reconciliation] >> audit
