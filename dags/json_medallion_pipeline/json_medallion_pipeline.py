"""JSON medallion pipeline: landing JSON -> bronze (immutable) -> silver (SCD-2) -> gold (releases).

Bronze   Every delivery is recorded in bronze.arrival (ACCEPTED / QUARANTINED / DUPLICATE) and its
         records in bronze.record. Rows are only ever inserted. Files are COPIED into
         data/bronze (accepted) or data/quarantine (rejected), keyed by logical date and run id.
         A bad file (invalid JSON, schema violation, no records) skips only its own source lane.
Silver   Parents (customers, products) are SCD-2: a changed row_hash closes the current version and
         inserts a new one. Orders, items, categories and prices are current-state (upserted or
         replaced per changed parent). Business-rule violations reject one ORDER or one PRODUCT
         (silver.rejected_record + an ERROR in ops.quality_issue), not the file. An order item whose
         product has no current silver row is PARKED: kept in silver, excluded from gold.
         Silver reads only the latest delivery through bronze.v_latest_accepted.
Gold     Every run builds an immutable release (ops.release). Gold tables are keyed by release_id;
         consumers read analytics.* / finance.* views that follow ops.destination.active_release_id.
         publish_release flips that pointer only if all lanes arrived and all gold tasks passed;
         otherwise the release is HELD, the destinations keep their previous release, and the run fails.

Layout:  sql/<layer>/*.sql  (all SQL)   schemas/<source>.v1.json  (bronze contracts)
"""

import hashlib
import json
import os
import re
from contextlib import closing
from dataclasses import asdict, dataclass
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.state import TaskInstanceState
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from jsonschema import Draft202012Validator
from psycopg2.extras import Json, execute_values

PIPELINE_DIR = Path(__file__).parent
SQL_DIR = PIPELINE_DIR / "sql"
SCHEMAS_DIR = PIPELINE_DIR / "schemas"
WAREHOUSE_CONN_ID = "warehouse_postgres"

DATA_DIR = Path(os.environ.get("PIPELINE_DATA_DIR", "/opt/airflow/data"))
LANDING_DIR = DATA_DIR / "landing"
BRONZE_DIR = DATA_DIR / "bronze"
QUARANTINE_DIR = DATA_DIR / "quarantine"

# A new file whose record count differs from the previous accepted one by more than this is a WARN.
EXPECTED_ROWS_TOLERANCE = 0.30


# --------------------------------------------------------------------------------------
# Sources
# --------------------------------------------------------------------------------------
@dataclass(frozen=True)
class SourceSpec:
    name: str
    filename: str
    record_path: tuple  # keys leading to the record(s); () = the document itself is the list
    schema_version: str  # schemas/<schema_version>.json


# Both files are envelopes: {source, batch_id, records: [...]}.
ORDERS = SourceSpec("orders", "orders.json", ("records",), "orders.v1")
PRODUCTS = SourceSpec("products", "products.json", ("records",), "products.v1")
SOURCES = (ORDERS, PRODUCTS)


# --------------------------------------------------------------------------------------
# Pure helpers (no Airflow state, no database)
# --------------------------------------------------------------------------------------
def extract_records(doc, record_path):
    """Return the list of records at record_path; a single object counts as one record."""
    node = doc
    for key in record_path:
        if not isinstance(node, dict) or key not in node:
            raise ValueError(f"missing key {key!r}")
        node = node[key]
    if isinstance(node, dict):
        return [node]
    if isinstance(node, list):
        return node
    raise ValueError(f"expected object or array, got {type(node).__name__}")


def safe_run_id(run_id):
    """Run ids contain ':' and '+'; keep only characters that are safe in a path."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def zone_path(zone_dir, source, ds, run_id, filename):
    """<zone>/<source>/dt=<ds>/<run id>/<file>: one directory per delivery attempt."""
    return Path(zone_dir) / source / f"dt={ds}" / safe_run_id(run_id) / filename


def json_path(parts):
    """('records', 0, 'items', 0, 'quantity') -> $.records[0].items[0].quantity"""
    return "$" + "".join(f"[{p}]" if isinstance(p, int) else f".{p}" for p in parts)


def first_schema_error(doc, schema, schema_version):
    """First schema violation (ordered by JSON path) as a readable reason, or None."""
    errors = sorted(Draft202012Validator(schema).iter_errors(doc), key=lambda e: json_path(e.absolute_path))
    if not errors:
        return None
    err = errors[0]
    return f"schema {schema_version}: {json_path(err.absolute_path)}: {err.message[:300]}"


def expected_rows_warning(previous_count, new_count, tolerance=EXPECTED_ROWS_TOLERANCE):
    """WARN text when the record count moved more than +/- tolerance vs the previous accepted file."""
    if not previous_count:
        return None
    change = (new_count - previous_count) / previous_count
    if abs(change) <= tolerance:
        return None
    return f"record_count {new_count} vs previous accepted {previous_count} ({change:+.0%}, tolerance +/-{tolerance:.0%})"


def decide_release(lane_ok, checks_ok):
    """(status, reason): publish only if every source lane arrived and every gold task succeeded."""
    missing = sorted(source for source, ok in lane_ok.items() if not ok)
    if missing:
        return "HELD", f"no accepted arrival for: {', '.join(missing)}"
    if not checks_ok:
        return "HELD", "a gold build or check task did not succeed"
    return "PUBLISHED", ""


@dataclass
class FileCheck:
    sha256: str
    size: int
    status: str  # ACCEPTED | QUARANTINED
    reason: str | None
    record_count: int | None
    records: list
    content: bytes
    batch_id: str | None = None  # envelope batch_id; None when unparseable or not a string
    envelope_source: str | None = None  # envelope "source"; same rule


def inspect_file(path, spec, schemas_dir=SCHEMAS_DIR):
    """Read the file once; hash, parse and validate those same bytes.

    Order: invalid JSON -> schema violation -> no records. The caller stores `content`, so the
    stored copy is byte-identical to what was hashed.
    """
    content = Path(path).read_bytes()  # FileNotFoundError propagates: a missing file is not a delivery
    sha256, size = hashlib.sha256(content).hexdigest(), len(content)
    batch_id = envelope_source = None  # filled in once the document parses (read by result() below)

    def result(status, reason, records=(), record_count=None):
        return FileCheck(sha256, size, status, reason, record_count, list(records), content,
                         batch_id, envelope_source)

    try:
        doc = json.loads(content)
    except ValueError as exc:  # JSONDecodeError and UnicodeDecodeError
        return result("QUARANTINED", f"invalid JSON: {exc}")

    if isinstance(doc, dict):
        batch_id = doc.get("batch_id") if isinstance(doc.get("batch_id"), str) else None
        envelope_source = doc.get("source") if isinstance(doc.get("source"), str) else None

    try:
        records = extract_records(doc, spec.record_path)
    except ValueError:
        records = None

    schema = json.loads((Path(schemas_dir) / f"{spec.schema_version}.json").read_text())
    reason = first_schema_error(doc, schema, spec.schema_version)
    if reason:
        return result("QUARANTINED", reason, record_count=None if records is None else len(records))
    if records is None:
        return result("QUARANTINED", "invalid record shape")
    if not records:
        return result("QUARANTINED", f"no records: {json_path(spec.record_path)} is empty", record_count=0)
    return result("ACCEPTED", None, records, len(records))


def _write_copy(dest, content):
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(content)


# --------------------------------------------------------------------------------------
# Database helpers (plain functions on a psycopg2 connection; each is one transaction)
# --------------------------------------------------------------------------------------
@dataclass
class IngestResult:
    arrival_id: int
    status: str
    reason: str | None
    record_count: int | None
    stored_path: str
    warnings: list  # WARN texts recorded as bronze quality issues (expected-rows, batch re-delivery)


def ingest_source(conn, spec, landing_dir, bronze_dir, quarantine_dir, ds, run_id):
    """Record one delivery in bronze. Only INSERTs; arrival + records commit together.

    ACCEPTED: file copied to the bronze zone, records inserted.
    DUPLICATE: same (source, logical_date, sha256) already ACCEPTED -> points at it, no records.
    QUARANTINED: file copied to the quarantine zone, ERROR quality issue recorded.
    WARN (ACCEPTED only): record count moved > +/-30% vs the previous accepted file, or the same
    batch_id was delivered before with different content.
    """
    check = inspect_file(Path(landing_dir) / spec.filename, spec)
    status, duplicate_of, warnings = check.status, None, []

    with conn, conn.cursor() as cur:
        if status == "ACCEPTED":
            cur.execute(
                """SELECT arrival_id, stored_path FROM bronze.arrival
                   WHERE source = %s AND logical_date = %s AND file_sha256 = %s AND status = 'ACCEPTED'
                   ORDER BY arrival_id LIMIT 1""",
                (spec.name, ds, check.sha256),
            )
            original = cur.fetchone()
            if original:
                status, (duplicate_of, stored_path) = "DUPLICATE", original
            else:
                cur.execute(
                    """SELECT record_count FROM bronze.arrival
                       WHERE source = %s AND status = 'ACCEPTED' ORDER BY arrival_id DESC LIMIT 1""",
                    (spec.name,),
                )
                previous = cur.fetchone()
                warning = expected_rows_warning(previous[0] if previous else None, check.record_count)
                if warning:
                    warnings.append(warning)
                if check.batch_id is not None:
                    cur.execute(
                        """SELECT 1 FROM bronze.arrival
                           WHERE source = %s AND batch_id = %s AND file_sha256 <> %s
                             AND status IN ('ACCEPTED', 'DUPLICATE') LIMIT 1""",
                        (spec.name, check.batch_id, check.sha256),
                    )
                    if cur.fetchone():
                        warnings.append(f"batch_id {check.batch_id} re-delivered with different content")
                stored = zone_path(bronze_dir, spec.name, ds, run_id, spec.filename)
                _write_copy(stored, check.content)
                stored_path = str(stored)
        else:
            stored = zone_path(quarantine_dir, spec.name, ds, run_id, spec.filename)
            _write_copy(stored, check.content)
            stored_path = str(stored)

        cur.execute(
            """INSERT INTO bronze.arrival
                   (source, logical_date, dag_run_id, source_file, stored_path, file_sha256, file_bytes,
                    schema_version, batch_id, envelope_source, record_count, status, quarantine_reason,
                    duplicate_of)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING arrival_id""",
            (spec.name, ds, run_id, spec.filename, stored_path, check.sha256, check.size,
             spec.schema_version, check.batch_id, check.envelope_source, check.record_count, status,
             check.reason, duplicate_of),
        )
        arrival_id = cur.fetchone()[0]

        if status == "ACCEPTED":
            execute_values(
                cur,
                "INSERT INTO bronze.record (arrival_id, record_index, payload) VALUES %s",
                [(arrival_id, i, Json(rec)) for i, rec in enumerate(check.records)],
            )
        issues = [("ERROR", check.reason)] if status == "QUARANTINED" else [("WARN", w) for w in warnings]
        for severity, reason in issues:
            cur.execute(
                """INSERT INTO ops.quality_issue
                       (dag_run_id, logical_date, layer, source_or_model, severity, reason, arrival_id)
                   VALUES (%s, %s, 'bronze', %s, %s, %s, %s)""",
                (run_id, ds, spec.name, severity, reason, arrival_id),
            )

    return IngestResult(arrival_id, status, check.reason, check.record_count, stored_path, warnings)


def create_release(conn, ds, run_id):
    """New BUILDING release `<ds>.<n>`; a retry within the same run reuses its BUILDING release."""
    with conn, conn.cursor() as cur:
        cur.execute(
            """SELECT release_id FROM ops.release
               WHERE dag_run_id = %s AND logical_date = %s AND status = 'BUILDING'
               ORDER BY release_id DESC LIMIT 1""",
            (run_id, ds),
        )
        existing = cur.fetchone()
        if existing:
            return existing[0]
        cur.execute("SELECT COUNT(*) FROM ops.release WHERE logical_date = %s", (ds,))
        version = f"{ds}.{cur.fetchone()[0] + 1}"
        cur.execute(
            """INSERT INTO ops.release (release_version, logical_date, dag_run_id, status)
               VALUES (%s, %s, %s, 'BUILDING') RETURNING release_id""",
            (version, ds, run_id),
        )
        return cur.fetchone()[0]


def lane_arrivals(conn, ds, run_id):
    """Latest arrival per source for this run: {source: {'arrival_id', 'ok'}}; ok = not quarantined.

    arrival_id resolves a DUPLICATE to the arrival it duplicates (that is where the records live).
    """
    with conn, conn.cursor() as cur:
        cur.execute(
            """SELECT DISTINCT ON (source) source, COALESCE(duplicate_of, arrival_id), status
               FROM bronze.arrival WHERE logical_date = %s AND dag_run_id = %s
               ORDER BY source, arrival_id DESC""",
            (ds, run_id),
        )
        found = {src: {"arrival_id": aid, "ok": status != "QUARANTINED"} for src, aid, status in cur.fetchall()}
    return {spec.name: found.get(spec.name, {"arrival_id": None, "ok": False}) for spec in SOURCES}


GOLD_MODELS = ("order_lines", "daily_sales_by_category", "customer_summary", "product_performance")


def release_row_counts(conn, release_id):
    """Rows per gold model in this release."""
    counts = {}
    with conn, conn.cursor() as cur:
        for model in GOLD_MODELS:  # constant names, never user input
            cur.execute(f"SELECT COUNT(*) FROM gold.{model} WHERE release_id = %s", (release_id,))
            counts[model] = cur.fetchone()[0]
    return counts


def publish_release(conn, release_id, row_counts, input_arrivals):
    """Mark PUBLISHED and flip BOTH destinations to this release in one transaction."""
    with conn, conn.cursor() as cur:
        cur.execute(
            """UPDATE ops.release SET status = 'PUBLISHED', released_at = now(), row_counts = %s,
                      input_arrivals = %s WHERE release_id = %s""",
            (Json(row_counts), Json(input_arrivals), release_id),
        )
        cur.execute("UPDATE ops.destination SET active_release_id = %s, updated_at = now()", (release_id,))


def hold_release(conn, release_id, reason, row_counts, input_arrivals):
    """Mark HELD and record why; destinations are left on their previous release."""
    with conn, conn.cursor() as cur:
        cur.execute(
            """UPDATE ops.release SET status = 'HELD', hold_reason = %s, row_counts = %s,
                      input_arrivals = %s WHERE release_id = %s""",
            (reason, Json(row_counts), Json(input_arrivals), release_id),
        )
        cur.execute(
            """INSERT INTO ops.quality_issue
                   (dag_run_id, logical_date, layer, source_or_model, severity, reason, release_id)
               SELECT dag_run_id, logical_date, 'gold', 'release', 'ERROR', %s, release_id
               FROM ops.release WHERE release_id = %s""",
            (reason, release_id),
        )


# --------------------------------------------------------------------------------------
# Airflow tasks
# --------------------------------------------------------------------------------------
def _ingest(spec, ds, run_id):
    with closing(PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).get_conn()) as conn:
        result = ingest_source(conn, spec, LANDING_DIR, BRONZE_DIR, QUARANTINE_DIR, ds, run_id)
    if result.status == "QUARANTINED":
        # Skip only this source's lane; the release will be HELD.
        raise AirflowSkipException(f"{spec.name} quarantined: {result.reason}")
    return asdict(result)


@task(task_id="ingest_orders")
def ingest_orders(ds=None, run_id=None):
    return _ingest(ORDERS, ds, run_id)


@task(task_id="ingest_products")
def ingest_products(ds=None, run_id=None):
    return _ingest(PRODUCTS, ds, run_id)


@task(task_id="create_release")
def create_release_task(ds=None, run_id=None):
    with closing(PostgresHook(postgres_conn_id=WAREHOUSE_CONN_ID).get_conn()) as conn:
        return create_release(conn, ds, run_id)


@task(task_id="publish_release", trigger_rule=TriggerRule.ALL_DONE)
def publish_release_task(release_id, ds=None, run_id=None, dag_run=None):
    """Single sink: publish or hold. A hold fails the run so it is visible."""
    if release_id is None:
        raise AirflowFailException("create_release produced no release id")
    gold_tis = [ti for ti in dag_run.get_task_instances() if ti.task_id.startswith("gold.")]
    checks_ok = all(ti.state == TaskInstanceState.SUCCESS for ti in gold_tis)

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


def _sql(task_id, sql):
    """Run a .sql file as one transaction (templated: {{ ds }}, ti.xcom_pull)."""
    return SQLExecuteQueryOperator(
        task_id=task_id,
        conn_id=WAREHOUSE_CONN_ID,
        sql=sql,
        split_statements=False,
        autocommit=False,
        do_xcom_push=False,
    )


def _check(task_id, sql):
    """Pass only if every column of the first row is true."""
    return SQLCheckOperator(task_id=task_id, conn_id=WAREHOUSE_CONN_ID, sql=sql)


# --------------------------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------------------------
with DAG(
    dag_id="json_medallion_pipeline",
    description="Landing JSON -> immutable bronze -> SCD-2 silver -> released gold",
    doc_md=__doc__,
    schedule="0 2 * * 1-5",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=1)},
    template_searchpath=[str(SQL_DIR)],
    tags=["medallion", "postgres", "json"],
) as dag:
    setup_schemas = _sql(
        "setup_schemas",
        [
            "bronze/ddl.sql",
            "ops/ddl.sql",
            "silver/ddl.sql",
            "gold/ddl.sql",
            "gold/trace_views.sql",
            "gold/consumer_views.sql",
        ],
    )
    release = create_release_task()
    setup_schemas >> release

    with TaskGroup("bronze"):
        ingest_o, ingest_p = ingest_orders(), ingest_products()

    with TaskGroup("silver"):
        transform_p = _sql("transform_products", "silver/products.sql")
        check_p = _check("check_products", "checks/silver_products.sql")
        transform_o = _sql("transform_orders", "silver/orders.sql")
        check_o = _check("check_orders", "checks/silver_orders.sql")

    with TaskGroup("gold"):
        build_lines = _sql("build_order_lines", "gold/order_lines.sql")
        build_daily = _sql("build_daily_sales_by_category", "gold/daily_sales_by_category.sql")
        build_customers = _sql("build_customer_summary", "gold/customer_summary.sql")
        build_products = _sql("build_product_performance", "gold/product_performance.sql")
        check_reconciliation = _check("check_reconciliation", "checks/gold_reconciliation.sql")

    publish = publish_release_task(release)

    # Lanes are independent until silver orders, which reads silver.products (items with an unknown product are parked),
    # so transform_orders also waits for check_products. Default all_success on purpose: a quarantined
    # orders lane skips its own tasks, but a quarantined products lane must also skip the orders
    # transform (none_failed would wrongly let it run).
    setup_schemas >> ingest_p >> transform_p >> check_p
    setup_schemas >> ingest_o >> transform_o >> check_o
    check_p >> transform_o

    # Gold models read silver only, so all four build in parallel once both silver lanes are checked.
    for build in (build_lines, build_daily, build_customers, build_products):
        [check_o, check_p, release] >> build
    [build_lines, build_daily, build_customers, build_products] >> check_reconciliation
    check_reconciliation >> publish
