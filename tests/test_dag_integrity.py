"""DAG integrity tests: parse the DAG folder the same way the scheduler does."""
import pytest
from airflow.models import DagBag

MAIN_DAG_ID = "json_medallion_pipeline"
FILESENSOR_DAG_ID = "json_medallion_pipeline_filesensor"
EXPECTED_DAG_IDS = {MAIN_DAG_ID, FILESENSOR_DAG_ID}


@pytest.fixture(scope="module")
def dagbag(pipeline_dir) -> DagBag:
    return DagBag(dag_folder=str(pipeline_dir), include_examples=False)


def test_no_import_errors(dagbag: DagBag):
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"


def test_expected_dags_present(dagbag: DagBag):
    assert set(dagbag.dag_ids) == EXPECTED_DAG_IDS


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAG_IDS))
def test_dag_basics(dagbag: DagBag, dag_id: str):
    dag = dagbag.get_dag(dag_id)
    assert dag is not None
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.default_args["retries"] >= 1
    assert "medallion" in dag.tags
    assert dag.template_searchpath and dag.template_searchpath[0].endswith("/sql")


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAG_IDS))
def test_dag_structure(dagbag: DagBag, dag_id: str):
    dag = dagbag.get_dag(dag_id)
    task_ids = set(dag.task_ids)

    assert "setup_schemas" in task_ids
    assert "record_load_audit" in task_ids
    for src in ("products", "users", "store_catalog"):
        assert {f"bronze.land_{src}", f"bronze.load_{src}", f"bronze.check_{src}"} <= task_ids
        assert {f"silver.transform_{src}", f"silver.check_{src}"} <= task_ids
    for model in (
        "user_activity_summary",
        "product_variation_flat",
        "inventory_value_by_category",
        "product_total_value",
    ):
        assert f"gold.build_{model}" in task_ids

    # Lane isolation: the users lane never waits on the products lane.
    silver_users = dag.get_task("silver.transform_users")
    assert silver_users.upstream_task_ids == {"bronze.check_users"}

    # Gold depends only on the silver it reads from.
    flat = dag.get_task("gold.build_product_variation_flat")
    assert flat.upstream_task_ids == {"silver.check_store_catalog"}

    # The audit is the single sink.
    leaves = {t.task_id for t in dag.tasks if not t.downstream_task_ids}
    assert leaves == {"record_load_audit"}


def test_scheduled_dag_root_is_setup(dagbag: DagBag):
    dag = dagbag.get_dag(MAIN_DAG_ID)
    roots = {t.task_id for t in dag.tasks if not t.upstream_task_ids}
    assert roots == {"setup_schemas"}


def test_filesensor_dag_gates_setup(dagbag: DagBag):
    dag = dagbag.get_dag(FILESENSOR_DAG_ID)
    setup = dag.get_task("setup_schemas")
    assert setup.upstream_task_ids == {
        "wait_for_products",
        "wait_for_users",
        "wait_for_store_catalog",
    }
    sensor = dag.get_task("wait_for_users")
    assert sensor.mode == "reschedule"
    assert sensor.fs_conn_id == "fs_landing"


def test_sql_files_exist(sql_dir, pipeline_module):
    """Every SQL path the pipeline references must resolve under sql/."""
    refs = ["bronze/ddl.sql", "silver/ddl.sql", "gold/ddl.sql", "ops/ddl.sql"]
    refs += ["checks/gold_activity_reconciliation.sql", "checks/gold_catalog_reconciliation.sql"]
    refs += [f"silver/{name}.sql" for name in ("products", "users", "store_catalog")]
    refs += [f"gold/{m}.sql" for m in pipeline_module.GOLD_DEPENDENCIES]

    assert [p for p in refs if not (sql_dir / p).is_file()] == []


def test_filesensor_gold_dependencies_match(pipeline_module, filesensor_module):
    assert filesensor_module.GOLD_DEPENDENCIES == pipeline_module.GOLD_DEPENDENCIES


@pytest.mark.parametrize("dag_id", sorted(EXPECTED_DAG_IDS))
def test_sql_templates_resolved_at_parse_time(dagbag: DagBag, dag_id: str):
    """DagBag resolves `template_ext` file references via template_searchpath.

    If a path were misspelled or outside the searchpath, the operator's `sql`
    would still be the literal '<path>.sql' string instead of SQL text.
    """
    dag = dagbag.get_dag(dag_id)
    unresolved = []
    for t in dag.tasks:
        sql = getattr(t, "sql", None)
        if sql is None:
            continue
        for stmt in sql if isinstance(sql, list) else [sql]:
            if stmt.strip().endswith(".sql"):
                unresolved.append((t.task_id, stmt))
    assert unresolved == []
    assert "CREATE SCHEMA" in "\n".join(dag.get_task("setup_schemas").sql)
    assert "silver.users" in dag.get_task("silver.transform_users").sql
