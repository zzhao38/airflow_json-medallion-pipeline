"""End-to-end SQL tests against a throwaway `warehouse_test` database (see conftest).

The test drives the same helpers and SQL files the DAG uses, in the same order, without Airflow:
ingest -> silver -> create release -> gold -> checks -> publish/hold.
Sample data: 4 order records (O1002 sent twice, O1003 has quantity -1) and 3 products.
"""
import json
import shutil
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import psycopg2
import pytest
from jinja2 import Template

pytestmark = pytest.mark.integration

DS = "2026-10-19"
DDL_ORDER = ["bronze/ddl.sql", "ops/ddl.sql", "silver/ddl.sql", "gold/ddl.sql",
             "gold/trace_views.sql", "gold/consumer_views.sql"]
GOLD_BUILDS = [f"gold/{m}.sql" for m in
               ("order_lines", "daily_sales_by_category", "customer_summary", "product_performance")]
RUN_1, RUN_2, RUN_3 = "manual__run1", "manual__run2", "manual__run3"


@dataclass
class Env:
    conn: object
    module: object
    sql_dir: Path
    landing: Path
    scenarios: Path
    bronze: Path
    quarantine: Path


@dataclass
class Run:
    release_id: int
    status: str
    reason: str
    ingests: dict


@pytest.fixture
def env(warehouse_test_dsn, pipeline_module, sql_dir, samples_dir, tmp_path):
    conn = psycopg2.connect(warehouse_test_dsn)
    with conn, conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS bronze, silver, gold, ops, analytics, finance CASCADE")
    landing = tmp_path / "landing"
    landing.mkdir()
    for sample in samples_dir.glob("*.json"):  # top-level only, like `make seed`
        shutil.copy(sample, landing / sample.name)
    environment = Env(conn, pipeline_module, sql_dir, landing, samples_dir / "scenarios",
                      tmp_path / "bronze", tmp_path / "quarantine")
    for ddl in DDL_ORDER:
        execute(environment, ddl)
    yield environment
    conn.close()


def render(env, path, release_id=None, run_id=RUN_1):
    ti = SimpleNamespace(xcom_pull=lambda task_ids: release_id)
    return Template((env.sql_dir / path).read_text()).render(ds=DS, run_id=run_id, ti=ti)


def execute(env, path, release_id=None, run_id=RUN_1):
    with env.conn, env.conn.cursor() as cur:
        cur.execute(render(env, path, release_id, run_id))


def check(env, path, release_id=None):
    """SQLCheckOperator semantics: every column of the first row must be truthy."""
    with env.conn, env.conn.cursor() as cur:
        cur.execute(render(env, path, release_id))
        row = cur.fetchone()
    return bool(row) and all(row)


def fetch(env, query, params=None):
    with env.conn, env.conn.cursor() as cur:
        cur.execute(query, params)
        return cur.fetchall()


def run_pipeline(env, run_id):
    """One DAG run, mirroring the task edges.

    products lane: ingest -> transform -> check.  orders lane: ingest -> (after check_products)
    transform -> check. A quarantined lane skips its own downstream tasks like the skipped tasks do,
    and the orders transform is skipped too when the products lane did not succeed (all_success).
    """
    m = env.module
    ingests = {spec.name: m.ingest_source(env.conn, spec, env.landing, env.bronze, env.quarantine, DS, run_id)
               for spec in (m.PRODUCTS, m.ORDERS)}
    release_id = m.create_release(env.conn, DS, run_id)

    products_ok = ingests["products"].status != "QUARANTINED"
    if products_ok:
        execute(env, "silver/products.sql", run_id=run_id)
        products_ok = check(env, "checks/silver_products.sql")
    orders_ok = ingests["orders"].status != "QUARANTINED" and products_ok
    if orders_ok:
        execute(env, "silver/orders.sql", run_id=run_id)
        orders_ok = check(env, "checks/silver_orders.sql")

    gold_ok = products_ok and orders_ok
    if gold_ok:
        for build in GOLD_BUILDS:
            execute(env, build, release_id)
        gold_ok = check(env, "checks/gold_reconciliation.sql", release_id)

    lanes = m.lane_arrivals(env.conn, DS, run_id)
    inputs = {name: lane["arrival_id"] for name, lane in lanes.items()}
    counts = m.release_row_counts(env.conn, release_id)
    status, reason = m.decide_release({name: lane["ok"] for name, lane in lanes.items()}, gold_ok)
    if status == "PUBLISHED":
        m.publish_release(env.conn, release_id, counts, inputs)
    else:
        m.hold_release(env.conn, release_id, reason, counts, inputs)
    return Run(release_id, status, reason, ingests)


def edit_landing(env, filename, edit):
    path = env.landing / filename
    doc = json.loads(path.read_text())
    edit(doc)
    path.write_text(json.dumps(doc))


def use_scenario(env, scenario, as_file):
    shutil.copy(env.scenarios / scenario, env.landing / as_file)


def active_releases(env):
    return dict(fetch(env, "SELECT destination, active_release_id FROM ops.destination"))


def consumer_snapshot(env):
    return {
        "lines": fetch(env, "SELECT order_item_id, line_revenue FROM analytics.order_lines ORDER BY 1"),
        "daily": fetch(env, "SELECT order_date, category_id, units, revenue FROM finance.daily_sales_by_category ORDER BY 1, 2"),
        "customers": fetch(env, "SELECT customer_id, order_count, revenue FROM analytics.customer_summary ORDER BY 1"),
        "products": fetch(env, "SELECT product_id, units, revenue, price_variance FROM analytics.product_performance ORDER BY 1"),
    }


def test_first_run_publishes_a_traceable_release(env):
    run = run_pipeline(env, RUN_1)
    assert run.status == "PUBLISHED", run.reason

    arrivals = fetch(env, "SELECT source, status, schema_version, batch_id, envelope_source, record_count, "
                          "length(file_sha256), stored_path FROM bronze.arrival ORDER BY source")
    assert [a[:7] for a in arrivals] == [
        ("orders", "ACCEPTED", "orders.v1", "20260902_orders", "order_service", 4, 64),
        ("products", "ACCEPTED", "products.v1", "20260902_products", "catalog_service", 3, 64),
    ]
    assert all(Path(a[7]).is_file() and str(env.bronze) in a[7] for a in arrivals)
    assert fetch(env, "SELECT COUNT(*) FROM bronze.record") == [(7,)]

    assert fetch(env, "SELECT release_version, status FROM ops.release") == [(f"{DS}.1", "PUBLISHED")]
    assert active_releases(env) == {"analytics": run.release_id, "finance": run.release_id}

    # silver: duplicate O1002 collapsed, O1003 (quantity -1) rejected as a record, not as a file
    assert fetch(env, "SELECT order_id FROM silver.orders ORDER BY 1") == [("O1001",), ("O1002",)]
    assert fetch(env, "SELECT COUNT(*) FROM silver.order_items") == [(3,)]
    assert fetch(env, "SELECT entity_key, reason FROM silver.rejected_record") == [
        ("O1003", "item OI-1003-1: quantity -1 must be > 0")]

    snap = consumer_snapshot(env)
    assert snap["lines"] == [("OI-1001-1", Decimal("49.99")), ("OI-1001-2", Decimal("20.00")),
                             ("OI-1002-1", Decimal("50.00"))]
    assert [(c, u, r) for _, c, u, r in snap["daily"]] == [
        ("ELECTRICAL", 2, Decimal("50.00")), ("SAFETY", 2, Decimal("20.00")), ("TOOLS", 1, Decimal("49.99"))]
    assert snap["customers"] == [("C001", 1, Decimal("69.99")), ("C002", 1, Decimal("50.00"))]
    assert snap["products"] == [("P100", 1, Decimal("49.99"), Decimal("0.00")),
                                ("P200", 2, Decimal("20.00"), Decimal("0.00")),
                                ("P300", 2, Decimal("50.00"), Decimal("0.00"))]

    issues = fetch(env, "SELECT layer, source_or_model, severity, reason FROM ops.quality_issue ORDER BY issue_id")
    assert [i[:3] for i in issues] == [("silver", "orders", "WARN"), ("silver", "orders", "ERROR")]
    assert "O1002" in issues[0][3] and "identical" in issues[0][3]
    assert "O1003" in issues[1][3]


def test_every_gold_row_traces_back_to_the_raw_payload(env):
    run_pipeline(env, RUN_1)
    for model in ("order_lines", "daily_sales_by_category", "customer_summary", "product_performance"):
        rows = fetch(env, f"SELECT release_version, source_file, file_sha256, schema_version, bronze_payload "
                          f"FROM gold.v_trace_{model}")
        assert rows, model
        for version, source_file, sha, schema_version, payload in rows:
            assert version == f"{DS}.1" and source_file.endswith(".json")
            assert len(sha) == 64 and schema_version.endswith(".v1") and payload

    payload = fetch(env, "SELECT bronze_payload FROM gold.v_trace_order_lines WHERE order_item_id = 'OI-1001-2'")[0][0]
    assert payload["order_id"] == "O1001" and payload["customer"]["name"] == "Jordan Lee"
    # a product's lineage is its own catalog record plus the order records that sold it
    sources = {r[0] for r in fetch(env, "SELECT schema_version FROM gold.v_trace_product_performance "
                                        "WHERE product_id = 'P100'")}
    assert sources == {"orders.v1", "products.v1"}


def test_rerun_with_identical_files_is_idempotent(env):
    first = run_pipeline(env, RUN_1)
    before = consumer_snapshot(env)
    silver_tables = ("customers", "orders", "order_items", "rejected_record", "products", "categories",
                     "product_prices")
    silver_rows = {t: fetch(env, f"SELECT COUNT(*) FROM silver.{t}") for t in silver_tables}
    issues_before = fetch(env, "SELECT COUNT(*) FROM ops.quality_issue")

    second = run_pipeline(env, RUN_2)
    assert second.status == "PUBLISHED" and second.release_id != first.release_id
    assert {r.status for r in second.ingests.values()} == {"DUPLICATE"}

    assert fetch(env, "SELECT release_version, status FROM ops.release ORDER BY release_id") == [
        (f"{DS}.1", "PUBLISHED"), (f"{DS}.2", "PUBLISHED")]
    assert fetch(env, "SELECT COUNT(*) FROM bronze.record") == [(7,)]  # duplicates add no records
    assert fetch(env, "SELECT COUNT(*) FROM bronze.arrival WHERE status = 'DUPLICATE' "
                      "AND duplicate_of IS NOT NULL") == [(2,)]
    assert silver_rows == {t: fetch(env, f"SELECT COUNT(*) FROM silver.{t}") for t in silver_tables}
    assert consumer_snapshot(env) == before
    assert active_releases(env) == {"analytics": second.release_id, "finance": second.release_id}
    # the run-scoped WARN/ERROR rows are written once per run, so a second run adds one set (not two)
    assert fetch(env, "SELECT COUNT(*) FROM ops.quality_issue") == [(issues_before[0][0] * 2,)]


def test_replaying_the_same_run_id_does_not_duplicate_quality_issues(env):
    run_pipeline(env, RUN_1)
    n = fetch(env, "SELECT COUNT(*) FROM ops.quality_issue")
    execute(env, "silver/orders.sql", run_id=RUN_1)
    assert fetch(env, "SELECT COUNT(*) FROM ops.quality_issue") == n
    assert fetch(env, "SELECT COUNT(*) FROM silver.rejected_record") == [(1,)]


def test_price_change_creates_a_new_scd2_version_and_a_new_release(env):
    first = run_pipeline(env, RUN_1)
    use_scenario(env, "products_price_change.json", "products.json")
    second = run_pipeline(env, RUN_2)

    assert second.status == "PUBLISHED"
    assert second.ingests["products"].status == "ACCEPTED"
    assert second.ingests["orders"].status == "DUPLICATE"

    versions = fetch(env, "SELECT is_current, valid_to IS NULL FROM silver.products "
                          "WHERE product_id = 'P100' ORDER BY product_sk")
    assert versions == [(False, False), (True, True)]
    assert fetch(env, "SELECT COUNT(*) FROM silver.products WHERE product_id = 'P200'") == [(1,)]
    assert fetch(env, "SELECT amount FROM silver.product_prices WHERE product_id = 'P100'") == [(Decimal("54.99"),)]

    # P100 sold at 49.99 against a catalog price of 54.99 -> variance -5.00 in the new release only
    assert fetch(env, "SELECT price_variance FROM analytics.product_performance WHERE product_id = 'P100'") == [
        (Decimal("-5.00"),)]
    assert fetch(env, "SELECT price_variance FROM gold.product_performance WHERE release_id = %s "
                      "AND product_id = 'P100'", (first.release_id,)) == [(Decimal("0.00"),)]


def test_redelivered_batch_id_with_different_content_warns(env):
    run_pipeline(env, RUN_1)
    edit_landing(env, "products.json", lambda doc: doc["records"][0]["prices"][0].update(amount=60))
    second = run_pipeline(env, RUN_2)

    assert second.status == "PUBLISHED"
    assert any("batch_id 20260902_products re-delivered" in w for w in second.ingests["products"].warnings)
    assert fetch(env, "SELECT COUNT(*) FROM ops.quality_issue WHERE layer = 'bronze' AND severity = 'WARN' "
                      "AND dag_run_id = %s", (RUN_2,)) == [(1,)]


@pytest.mark.parametrize(("scenario", "reason_part"), [
    ("orders_bad_schema.json", "$.records[0].items[0].quantity"),
    ("orders_wrong_source.json", "$.source"),
])
def test_invalid_orders_file_is_quarantined_and_the_release_is_held(env, scenario, reason_part):
    first = run_pipeline(env, RUN_1)
    before = consumer_snapshot(env)
    use_scenario(env, scenario, "orders.json")
    second = run_pipeline(env, RUN_2)

    orders = second.ingests["orders"]
    assert orders.status == "QUARANTINED" and reason_part in orders.reason
    assert str(env.quarantine) in orders.stored_path and Path(orders.stored_path).is_file()
    assert second.ingests["products"].status == "DUPLICATE"

    assert second.status == "HELD" and "orders" in second.reason
    assert fetch(env, "SELECT status, hold_reason IS NOT NULL FROM ops.release WHERE release_id = %s",
                 (second.release_id,)) == [("HELD", True)]
    assert fetch(env, "SELECT COUNT(*) FROM bronze.record r JOIN bronze.arrival a USING (arrival_id) "
                      "WHERE a.source = 'orders'") == [(4,)]  # nothing from the bad file reached bronze.record
    assert fetch(env, "SELECT COUNT(*) FROM silver.orders") == [(2,)]  # silver untouched

    issues = fetch(env, "SELECT layer, source_or_model, severity FROM ops.quality_issue WHERE dag_run_id = %s "
                        "ORDER BY issue_id", (RUN_2,))
    assert issues == [("bronze", "orders", "ERROR"), ("gold", "release", "ERROR")]

    assert active_releases(env) == {"analytics": first.release_id, "finance": first.release_id}
    assert consumer_snapshot(env) == before  # consumers still see the last good release
    assert fetch(env, "SELECT DISTINCT release_version, released_at IS NOT NULL "
                      "FROM finance.product_performance") == [(f"{DS}.1", True)]


def test_quarantined_products_also_skip_the_orders_transform(env):
    edit_landing(env, "products.json", lambda doc: doc.update(source="wrong"))
    run = run_pipeline(env, RUN_1)

    assert run.ingests["products"].status == "QUARANTINED"
    assert run.status == "HELD" and "products" in run.reason
    assert fetch(env, "SELECT COUNT(*) FROM silver.orders") == [(0,)]
    assert active_releases(env) == {"analytics": None, "finance": None}


def test_fixed_file_publishes_after_a_hold(env):
    run_pipeline(env, RUN_1)
    use_scenario(env, "orders_bad_schema.json", "orders.json")
    assert run_pipeline(env, RUN_2).status == "HELD"

    shutil.copy(env.scenarios.parent / "orders.json", env.landing / "orders.json")
    third = run_pipeline(env, RUN_3)
    assert third.status == "PUBLISHED"
    assert third.ingests["orders"].status == "DUPLICATE"  # same bytes as the accepted delivery
    assert active_releases(env) == {"analytics": third.release_id, "finance": third.release_id}


def test_item_with_an_unknown_product_is_parked_and_kept_out_of_gold(env):
    def add_unknown_item(doc):
        doc["records"][0]["items"].append(
            {"order_item_id": "OI-1001-3", "product_id": "P999", "quantity": 1, "unit_price": 5.0})

    edit_landing(env, "orders.json", add_unknown_item)
    run = run_pipeline(env, RUN_1)

    assert run.status == "PUBLISHED"  # the reconciliation counts only items with a current product
    assert fetch(env, "SELECT COUNT(*) FROM silver.order_items WHERE order_item_id = 'OI-1001-3'") == [(1,)]
    assert fetch(env, "SELECT order_item_id, product_id FROM silver.v_parked_order_items") == [("OI-1001-3", "P999")]
    assert fetch(env, "SELECT COUNT(*) FROM analytics.order_lines WHERE order_item_id = 'OI-1001-3'") == [(0,)]
    assert fetch(env, "SELECT COUNT(*) FROM analytics.order_lines") == [(3,)]
    assert fetch(env, "SELECT COUNT(*) FROM finance.daily_sales_by_category WHERE category_id = 'UNKNOWN'") == [(0,)]
    warns = fetch(env, "SELECT reason FROM ops.quality_issue WHERE layer = 'silver' AND severity = 'WARN' "
                       "AND reason LIKE '%P999%'")
    assert len(warns) == 1 and "parked" in warns[0][0]


def test_parked_item_reaches_gold_once_its_product_arrives(env):
    run_pipeline(env, RUN_1)
    use_scenario(env, "orders_unknown_product.json", "orders.json")
    second = run_pipeline(env, RUN_2)
    assert second.status == "PUBLISHED"
    assert fetch(env, "SELECT order_item_id FROM silver.v_parked_order_items") == [("OI-4013-2",)]
    assert fetch(env, "SELECT COUNT(*) FROM analytics.order_lines WHERE order_item_id = 'OI-4013-2'") == [(0,)]
    assert fetch(env, "SELECT COUNT(*) FROM analytics.order_lines WHERE order_item_id = 'OI-4013-1'") == [(1,)]

    use_scenario(env, "products_new_product.json", "products.json")
    third = run_pipeline(env, RUN_3)
    assert third.status == "PUBLISHED"
    assert third.ingests["orders"].status == "DUPLICATE"
    assert fetch(env, "SELECT COUNT(*) FROM silver.v_parked_order_items") == [(0,)]
    assert fetch(env, "SELECT line_revenue FROM analytics.order_lines WHERE order_item_id = 'OI-4013-2'") == [
        (Decimal("15.00"),)]


def test_rejected_product_update_keeps_the_previous_version(env):
    run_pipeline(env, RUN_1)
    use_scenario(env, "products_negative_price.json", "products.json")
    second = run_pipeline(env, RUN_2)

    assert second.status == "PUBLISHED"
    assert fetch(env, "SELECT is_current FROM silver.products WHERE product_id = 'P100'") == [(True,)]
    assert fetch(env, "SELECT amount FROM silver.product_prices WHERE product_id = 'P100'") == [(Decimal("49.99"),)]
    assert fetch(env, "SELECT COUNT(*) FROM silver.products WHERE product_id = 'P400'") == [(0,)]


def _all_scenarios():
    path = Path(__file__).resolve().parents[1] / "data" / "samples" / "scenarios" / "manifest.json"
    return json.loads(path.read_text())["scenarios"]


@pytest.mark.parametrize("scenario", _all_scenarios(), ids=lambda s: s["name"])
def test_manifest_scenario_outcome(env, scenario):
    """Clean first run, then the scenario file replaces its source on the same logical date.

    Pipeline-level entries (layer=pipeline) have no file; the automated ones have their own tests below.
    """
    if "file" not in scenario:
        pytest.skip(f"pipeline-level scenario, not file-driven ({scenario.get('test', 'manual walk-through')})")
    first = run_pipeline(env, RUN_1)
    assert first.status == "PUBLISHED"
    before = consumer_snapshot(env)
    use_scenario(env, scenario["file"], f"{scenario['source']}.json")
    run = run_pipeline(env, RUN_2)

    ingest = run.ingests[scenario["source"]]
    assert ingest.status == scenario["bronze"]["status"]
    for text in scenario["bronze"].get("warns", []):
        assert any(text in w for w in ingest.warnings), (text, ingest.warnings)
    assert run.status == scenario["release"], run.reason
    if scenario["release"] == "HELD":
        assert consumer_snapshot(env) == before  # consumers keep the last good release
        return

    table, key = ("silver.orders", "order_id") if scenario["source"] == "orders" else ("silver.products", "product_id")
    current = " AND is_current" if scenario["source"] == "products" else ""
    loaded = fetch(env, f"SELECT {key} FROM {table} WHERE bronze_arrival_id = %s{current} ORDER BY 1",
                   (ingest.arrival_id,))
    assert [r[0] for r in loaded] == sorted(scenario.get("loaded", []))

    rejected = fetch(env, "SELECT entity_key, reason FROM silver.rejected_record WHERE arrival_id = %s",
                     (ingest.arrival_id,))
    expected = scenario.get("rejected", {})
    assert {k for k, _ in rejected} == set(expected)
    for entity_key, reason in rejected:
        assert expected[entity_key] in reason, (entity_key, reason)

    parked = fetch(env, "SELECT order_item_id FROM silver.v_parked_order_items ORDER BY 1")
    assert [r[0] for r in parked] == sorted(scenario.get("parked_items", []))

    warns = [r[0] for r in fetch(env, "SELECT reason FROM ops.quality_issue WHERE layer = 'silver' "
                                      "AND severity = 'WARN' AND dag_run_id = %s", (RUN_2,))]
    for text in scenario.get("warns", []):
        assert any(text in w for w in warns), (text, warns)

    rejected_issues = fetch(env, "SELECT COUNT(*) FROM ops.quality_issue WHERE layer = 'silver' "
                                 "AND severity = 'ERROR' AND dag_run_id = %s", (RUN_2,))[0][0]
    assert rejected_issues >= len(expected)


def test_unusual_row_count_warns_without_quarantining(env):
    run_pipeline(env, RUN_1)

    def many_more(doc):  # 3 -> 11 records (+267%), well over the +/-30% tolerance
        template = doc["records"][2]
        doc["records"] += [{**template, "product_id": f"PX{i}"} for i in range(8)]
        doc["batch_id"] = "20260903_products"

    edit_landing(env, "products.json", many_more)
    second = run_pipeline(env, RUN_2)

    assert second.ingests["products"].status == "ACCEPTED" and second.ingests["products"].warnings
    assert second.status == "PUBLISHED"
    assert fetch(env, "SELECT layer, source_or_model, severity FROM ops.quality_issue "
                      "WHERE layer = 'bronze' AND dag_run_id = %s", (RUN_2,)) == [("bronze", "products", "WARN")]


# --------------------------------------------------------------------------------------
# Pipeline-level scenarios (manifest entries with layer=pipeline)
# --------------------------------------------------------------------------------------
def test_tampered_gold_is_held_and_destinations_stay_put(env):
    first = run_pipeline(env, RUN_1)
    before = consumer_snapshot(env)
    m = env.module

    # second run by hand: build gold for a new BUILDING release, then corrupt one gold row
    for spec in (m.PRODUCTS, m.ORDERS):
        m.ingest_source(env.conn, spec, env.landing, env.bronze, env.quarantine, DS, RUN_2)
    release_id = m.create_release(env.conn, DS, RUN_2)
    execute(env, "silver/products.sql", run_id=RUN_2)
    execute(env, "silver/orders.sql", run_id=RUN_2)
    for build in GOLD_BUILDS:
        execute(env, build, release_id)
    assert check(env, "checks/gold_reconciliation.sql", release_id) is True  # intact build passes

    with env.conn, env.conn.cursor() as cur:
        cur.execute("UPDATE gold.order_lines SET line_revenue = line_revenue + 1000 "
                    "WHERE release_id = %s AND order_item_id = 'OI-1001-1'", (release_id,))
    assert check(env, "checks/gold_reconciliation.sql", release_id) is False

    lanes = m.lane_arrivals(env.conn, DS, RUN_2)
    status, reason = m.decide_release({n: lane["ok"] for n, lane in lanes.items()}, False)
    assert status == "HELD" and "gold" in reason
    m.hold_release(env.conn, release_id, reason, m.release_row_counts(env.conn, release_id),
                   {n: lane["arrival_id"] for n, lane in lanes.items()})

    assert fetch(env, "SELECT status FROM ops.release WHERE release_id = %s", (release_id,)) == [("HELD",)]
    assert active_releases(env) == {"analytics": first.release_id, "finance": first.release_id}
    assert consumer_snapshot(env) == before  # the tampered rows are invisible to consumers


def test_create_release_is_reused_on_retry(env):
    m = env.module
    first = m.create_release(env.conn, DS, RUN_1)
    assert m.create_release(env.conn, DS, RUN_1) == first
    assert fetch(env, "SELECT COUNT(*) FROM ops.release") == [(1,)]
    assert m.create_release(env.conn, DS, RUN_2) != first  # a different run gets its own release
    assert fetch(env, "SELECT release_version FROM ops.release ORDER BY release_id") == [(f"{DS}.1",), (f"{DS}.2",)]


def test_additive_extra_field_is_accepted_and_loads(env):
    def add_field(doc):
        for record in doc["records"]:
            record["lifecycle_stage"] = "active"  # unknown to the schema and to silver

    edit_landing(env, "products.json", add_field)
    run = run_pipeline(env, RUN_1)

    assert run.ingests["products"].status == "ACCEPTED" and run.status == "PUBLISHED"
    assert fetch(env, "SELECT COUNT(*) FROM silver.products WHERE is_current") == [(3,)]
    assert fetch(env, "SELECT COUNT(*) FROM bronze.record r JOIN bronze.arrival a USING (arrival_id) "
                      "WHERE a.source = 'products' AND r.payload ? 'lifecycle_stage'") == [(3,)]


def test_missing_landing_file_raises_and_records_no_arrival(env):
    (env.landing / "orders.json").unlink()
    with pytest.raises(FileNotFoundError):
        env.module.ingest_source(env.conn, env.module.ORDERS, env.landing, env.bronze, env.quarantine, DS, RUN_1)
    assert fetch(env, "SELECT COUNT(*) FROM bronze.arrival") == [(0,)]
    assert fetch(env, "SELECT COUNT(*) FROM ops.quality_issue") == [(0,)]
