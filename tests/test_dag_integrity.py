"""DAG integrity tests: parse the DAG folder the same way the scheduler does."""
import pytest
from airflow.models import DagBag
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "json_medallion_pipeline"
SOURCES = ("orders", "products")
GOLD_BUILDS = ("order_lines", "daily_sales_by_category", "customer_summary", "product_performance")


@pytest.fixture(scope="module")
def dagbag(pipeline_dir) -> DagBag:
    return DagBag(dag_folder=str(pipeline_dir), include_examples=False)


@pytest.fixture(scope="module")
def dag(dagbag: DagBag):
    return dagbag.get_dag(DAG_ID)


def test_no_import_errors(dagbag: DagBag):
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"


def test_expected_dags_present(dagbag: DagBag):
    assert set(dagbag.dag_ids) == {DAG_ID}


def test_dag_basics(dag):
    assert dag is not None
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.default_args["retries"] >= 1
    assert "medallion" in dag.tags
    assert dag.template_searchpath and dag.template_searchpath[0].endswith("/sql")


def test_task_ids(dag):
    expected = {"setup_schemas", "create_release", "publish_release", "gold.check_reconciliation"}
    for src in SOURCES:
        expected |= {f"bronze.ingest_{src}", f"silver.transform_{src}", f"silver.check_{src}"}
    expected |= {f"gold.build_{model}" for model in GOLD_BUILDS}
    assert set(dag.task_ids) == expected


def test_lanes(dag):
    for src in SOURCES:
        assert dag.get_task(f"bronze.ingest_{src}").upstream_task_ids == {"setup_schemas"}
        assert dag.get_task(f"silver.check_{src}").upstream_task_ids == {f"silver.transform_{src}"}
    assert dag.get_task("silver.transform_products").upstream_task_ids == {"bronze.ingest_products"}


def test_orders_transform_waits_for_the_products_check_with_all_success(dag):
    """Silver orders reads silver.products (unknown-product items are parked), so it follows the products lane."""
    transform = dag.get_task("silver.transform_orders")
    assert transform.upstream_task_ids == {"bronze.ingest_orders", "silver.check_products"}
    assert transform.trigger_rule == TriggerRule.ALL_SUCCESS


def test_gold_builds_read_silver_and_run_in_parallel(dag):
    expected = {"silver.check_orders", "silver.check_products", "create_release"}
    for model in GOLD_BUILDS:
        assert dag.get_task(f"gold.build_{model}").upstream_task_ids == expected, model
    assert dag.get_task("gold.check_reconciliation").upstream_task_ids == {f"gold.build_{m}" for m in GOLD_BUILDS}


def test_publish_release_is_the_single_sink(dag):
    leaves = {t.task_id for t in dag.tasks if not t.downstream_task_ids}
    assert leaves == {"publish_release"}
    publish = dag.get_task("publish_release")
    assert publish.trigger_rule == TriggerRule.ALL_DONE
    assert publish.upstream_task_ids == {"create_release", "gold.check_reconciliation"}


def test_gold_task_ids_keep_the_prefix_publish_relies_on(dag):
    gold = [t for t in dag.task_ids if t.startswith("gold.")]
    assert len(gold) == len(GOLD_BUILDS) + 1


def test_dag_root_is_setup(dag):
    roots = {t.task_id for t in dag.tasks if not t.upstream_task_ids}
    assert roots == {"setup_schemas"}


def test_setup_ddl_order_and_views(dag):
    text = "\n".join(dag.get_task("setup_schemas").sql)
    markers = (
        "CREATE SCHEMA IF NOT EXISTS bronze", "CREATE SCHEMA IF NOT EXISTS ops",
        "CREATE SCHEMA IF NOT EXISTS silver", "CREATE SCHEMA IF NOT EXISTS gold",
        "gold.v_trace_order_lines", "analytics.order_lines",
    )
    positions = [text.index(marker) for marker in markers]
    assert positions == sorted(positions)
    for model in GOLD_BUILDS:
        assert f"gold.v_trace_{model}" in text


def test_sql_files_exist(sql_dir, pipeline_dir):
    refs = ["bronze/ddl.sql", "ops/ddl.sql", "silver/ddl.sql", "gold/ddl.sql",
            "gold/trace_views.sql", "gold/consumer_views.sql", "checks/gold_reconciliation.sql"]
    refs += [f"checks/silver_{src}.sql" for src in SOURCES]
    refs += [f"silver/{src}.sql" for src in SOURCES]
    refs += [f"gold/{m}.sql" for m in GOLD_BUILDS]
    assert [p for p in refs if not (sql_dir / p).is_file()] == []
    assert [s for s in ("orders.v1", "products.v1")
            if not (pipeline_dir / "schemas" / f"{s}.json").is_file()] == []


def test_sql_templates_resolved_at_parse_time(dag):
    """DagBag resolves `template_ext` file references via template_searchpath.

    A misspelled path would leave the operator's `sql` as the literal '<path>.sql' string.
    """
    unresolved = []
    for t in dag.tasks:
        sql = getattr(t, "sql", None)
        if sql is None:
            continue
        for stmt in sql if isinstance(sql, list) else [sql]:
            if stmt.strip().endswith(".sql"):
                unresolved.append((t.task_id, stmt))
    assert unresolved == []
    assert "silver.orders" in dag.get_task("silver.transform_orders").sql
    assert "release_id" in dag.get_task("gold.check_reconciliation").sql
