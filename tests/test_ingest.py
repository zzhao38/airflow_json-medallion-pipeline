"""Ingestion helper tests: the plain-Python helpers defined in the scheduled DAG file."""
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

# (landing file, record_path) per source, as hard-coded in the DAG.
SPECS = {
    "products": ("products.json", ()),
    "users": ("users.json", ("users",)),
    "store_catalog": ("store.json", ("store",)),
}
SOURCE_NAMES = list(SPECS)


@pytest.fixture
def impl(pipeline_module):
    return SimpleNamespace(
        extract_records=pipeline_module.extract_records,
        sha256_of_file=pipeline_module.sha256_of_file,
        land=lambda name, landing, bronze, ds: pipeline_module.land_file(
            name, SPECS[name][0], SPECS[name][1], landing, bronze, ds
        ),
        rows=lambda landed, name, ds, run_id: pipeline_module.bronze_rows(
            landed, SPECS[name][1], ds, run_id
        ),
    )


class TestExtractRecords:
    def test_flat_list(self, impl):
        doc = [{"id": 1}, {"id": 2}]
        assert impl.extract_records(doc, ()) == doc

    def test_nested_list(self, impl):
        doc = {"users": [{"id": 1}, {"id": 2}]}
        assert impl.extract_records(doc, ("users",)) == doc["users"]

    def test_single_object_becomes_one_record(self, impl):
        doc = {"store": {"name": "SuperMart"}}
        assert impl.extract_records(doc, ("store",)) == [{"name": "SuperMart"}]

    def test_missing_path_raises(self, impl):
        with pytest.raises(ValueError, match="record_path"):
            impl.extract_records({"users": []}, ("customers",))

    def test_non_object_records_raise(self, impl):
        with pytest.raises(ValueError, match="non-object"):
            impl.extract_records([1, 2, 3], ())

    def test_scalar_node_raises(self, impl):
        with pytest.raises(ValueError, match="list or object"):
            impl.extract_records({"n": 5}, ("n",))


class TestLandFile:
    def test_lands_into_partition_and_hashes(self, impl, tmp_path: Path, samples_dir: Path):
        bronze = tmp_path / "bronze"

        landed = impl.land("users", samples_dir, bronze, "2024-01-01")

        dest = bronze / "users" / "dt=2024-01-01" / "users.json"
        assert Path(landed.bronze_path) == dest
        assert dest.is_file()
        assert landed.sha256 == impl.sha256_of_file(samples_dir / "users.json")
        assert (landed.source_name, landed.source_file) == ("users", "users.json")

    def test_store_catalog_keeps_landing_file_name(self, impl, tmp_path: Path, samples_dir: Path):
        landed = impl.land("store_catalog", samples_dir, tmp_path, "2024-01-01")
        assert Path(landed.bronze_path) == tmp_path / "store_catalog" / "dt=2024-01-01" / "store.json"

    def test_rerun_overwrites_partition(self, impl, tmp_path: Path, samples_dir: Path):
        first = impl.land("products", samples_dir, tmp_path, "2024-01-01")
        second = impl.land("products", samples_dir, tmp_path, "2024-01-01")
        assert first == second
        assert len(list((tmp_path / "products" / "dt=2024-01-01").iterdir())) == 1

    def test_missing_landing_file(self, impl, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            impl.land("products", tmp_path, tmp_path / "bronze", "2024-01-01")

    def test_invalid_json_is_rejected_before_landing(self, impl, tmp_path: Path):
        landing = tmp_path / "landing"
        landing.mkdir()
        (landing / "products.json").write_text("{not json")
        with pytest.raises(json.JSONDecodeError):
            impl.land("products", landing, tmp_path / "bronze", "2024-01-01")
        assert not (tmp_path / "bronze").exists()

    def test_wrong_shape_is_rejected_before_landing(self, impl, tmp_path: Path):
        landing = tmp_path / "landing"
        landing.mkdir()
        (landing / "users.json").write_text('{"customers": []}')
        with pytest.raises(ValueError, match="record_path"):
            impl.land("users", landing, tmp_path / "bronze", "2024-01-01")
        assert not (tmp_path / "bronze").exists()


class TestBronzeRows:
    @pytest.mark.parametrize("name", SOURCE_NAMES)
    def test_one_row_per_record(self, impl, tmp_path: Path, samples_dir: Path, name: str):
        landed = impl.land(name, samples_dir, tmp_path, "2024-01-01")
        rows = impl.rows(landed, name, "2024-01-01", "manual__test")

        assert rows, "sample must yield at least one record"
        for idx, row in enumerate(rows):
            ds, record_index, payload, source_file, sha, run_id = row
            assert (ds, record_index, source_file, sha, run_id) == (
                "2024-01-01", idx, SPECS[name][0], landed.sha256, "manual__test"
            )
            assert isinstance(json.loads(payload), dict)

    def test_sample_record_counts(self, impl, tmp_path: Path, samples_dir: Path):
        counts = {
            name: len(impl.rows(impl.land(name, samples_dir, tmp_path, "2024-01-01"), name, "2024-01-01", "r"))
            for name in SOURCE_NAMES
        }
        assert counts == {"products": 4, "users": 2, "store_catalog": 1}
