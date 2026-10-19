import importlib.util
import os
import sys
from collections.abc import Iterator
from pathlib import Path

import psycopg2
import pytest
from psycopg2 import sql
from psycopg2.extensions import make_dsn

REPO_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = REPO_ROOT / "dags"
PIPELINE_DIR = DAGS_DIR / "json_medallion_pipeline"
TEST_DB_NAME = "warehouse_test"
REQUIRED_SAMPLES = ("products.json", "users.json", "store.json")


def load_dag_module(path: Path, name: str):
    """Import a DAG file by path (the dag folders are not packages)."""
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # dataclasses resolve annotations through sys.modules
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def dags_dir() -> Path:
    return DAGS_DIR


@pytest.fixture(scope="session")
def pipeline_dir() -> Path:
    return PIPELINE_DIR


@pytest.fixture(scope="session")
def sql_dir() -> Path:
    return PIPELINE_DIR / "sql"


@pytest.fixture(scope="session")
def pipeline_module():
    """The DAG module (helpers like land_file / bronze_rows)."""
    return load_dag_module(PIPELINE_DIR / "json_medallion_pipeline.py", "json_medallion_pipeline_module")


@pytest.fixture(scope="session")
def samples_dir() -> Path:
    return REPO_ROOT / "data" / "samples"


@pytest.fixture(scope="session")
def warehouse_test_dsn(samples_dir) -> Iterator[str]:
    """DSN of a throwaway `warehouse_test` database, created for the session and then dropped.

    Integration tests never touch the demo `warehouse` database. The server DSN comes from the
    same env var Airflow uses for its connection; only the database name is swapped.
    """
    base_dsn = os.environ.get("AIRFLOW_CONN_WAREHOUSE_POSTGRES")
    if not base_dsn:
        pytest.skip("AIRFLOW_CONN_WAREHOUSE_POSTGRES not set; integration tests need a Postgres server")
    missing = [name for name in REQUIRED_SAMPLES if not (samples_dir / name).is_file()]
    if missing:
        pytest.skip(f"sample files missing in data/samples/: {', '.join(missing)}")

    drop_stmt = sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(TEST_DB_NAME))
    admin = psycopg2.connect(base_dsn)
    admin.autocommit = True  # CREATE/DROP DATABASE cannot run inside a transaction
    try:
        with admin.cursor() as cur:
            cur.execute(drop_stmt)
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(TEST_DB_NAME)))
        yield make_dsn(base_dsn, dbname=TEST_DB_NAME)
    finally:
        with admin.cursor() as cur:
            cur.execute(drop_stmt)
        admin.close()
