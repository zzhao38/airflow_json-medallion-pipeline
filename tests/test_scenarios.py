"""Scenario files in data/samples/scenarios/ against the manifest (no database).

File-level outcomes (ACCEPTED / QUARANTINED + reason) are checked here; the record-level outcomes
(loaded / rejected / parked) are checked in test_pipeline_integration.py.
"""
import json
from pathlib import Path

import pytest

MANIFEST = Path(__file__).resolve().parents[1] / "data" / "samples" / "scenarios" / "manifest.json"


def file_scenarios():
    """Scenarios backed by a file; pipeline-level entries (layer=pipeline) have none."""
    return [s for s in json.loads(MANIFEST.read_text())["scenarios"] if "file" in s]


def scenario_ids():
    return [s["name"] for s in file_scenarios()]


def test_manifest_and_scenario_files_match(manifest, scenarios_dir):
    listed = {s["file"] for s in manifest["scenarios"] if "file" in s}
    on_disk = {p.name for p in scenarios_dir.glob("*.json")} - {"manifest.json"}
    assert listed == on_disk, f"not in manifest: {on_disk - listed}; missing on disk: {listed - on_disk}"
    assert len({s["name"] for s in manifest["scenarios"]}) == len(manifest["scenarios"])


def test_manifest_entries_are_well_formed(manifest):
    for s in manifest["scenarios"]:
        assert s["layer"] in {"bronze", "silver", "pipeline"}, s["name"]
        if s["layer"] == "pipeline":
            assert "file" not in s and isinstance(s["automated"], bool), s["name"]
            assert s["talking_point"].strip(), s["name"]
            assert s["automated"] == ("test" in s), f"{s['name']}: automated entries name their test"
        else:
            assert s["file"] and s["source"] and s["bronze"]["status"] in {"ACCEPTED", "QUARANTINED"}, s["name"]
            assert (s["bronze"]["status"] == "QUARANTINED") == (s["layer"] == "bronze"), s["name"]


@pytest.mark.parametrize("name", scenario_ids())
def test_file_level_outcome(name, manifest, scenarios_dir, pipeline_module):
    scenario = next(s for s in manifest["scenarios"] if s["name"] == name)
    spec = {pipeline_module.ORDERS.name: pipeline_module.ORDERS,
            pipeline_module.PRODUCTS.name: pipeline_module.PRODUCTS}[scenario["source"]]
    check = pipeline_module.inspect_file(scenarios_dir / scenario["file"], spec)

    expected = scenario["bronze"]
    assert check.status == expected["status"], check.reason
    if expected["status"] == "QUARANTINED":
        assert expected["reason_contains"] in check.reason
    else:
        assert check.reason is None and check.record_count > 0
