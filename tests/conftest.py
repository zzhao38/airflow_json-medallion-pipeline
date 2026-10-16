import importlib.util
import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = REPO_ROOT / "dags"
PIPELINE_DIR = DAGS_DIR / "json_medallion_pipeline"


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
    """The scheduled DAG module (helpers + GOLD_DEPENDENCIES)."""
    return load_dag_module(PIPELINE_DIR / "json_medallion_pipeline.py", "json_medallion_pipeline_module")


@pytest.fixture(scope="session")
def filesensor_module():
    return load_dag_module(
        PIPELINE_DIR / "json_medallion_pipeline_filesensor.py", "json_medallion_pipeline_filesensor_module"
    )


@pytest.fixture(scope="session")
def samples_dir() -> Path:
    return REPO_ROOT / "data" / "samples"


@pytest.fixture(scope="session")
def warehouse_dsn() -> str:
    """DSN for the warehouse DB; same env var Airflow uses for the connection."""
    dsn = os.environ.get("AIRFLOW_CONN_WAREHOUSE_POSTGRES")
    if not dsn:
        pytest.skip("AIRFLOW_CONN_WAREHOUSE_POSTGRES not set; integration tests need the warehouse")
    return dsn
