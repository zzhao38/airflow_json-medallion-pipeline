"""Unit tests for the pure bronze/release helpers in the DAG file (no database, no sample files)."""
import copy
import hashlib
import json

import pytest

ORDERS_DOC = {
    "source": "order_service",
    "batch_id": "b1",
    "records": [
        {
            "order_id": "O1",
            "ordered_at": "2026-09-01T10:15:00Z",
            "customer": {"customer_id": "C1", "name": "A"},
            "shipping": {"city": "X", "state": "IL", "method": "ground"},
            "items": [{"order_item_id": "OI-1", "product_id": "P1", "quantity": 1, "unit_price": 9.99}],
        }
    ],
}
PRODUCTS_DOC = {
    "source": "catalog_service",
    "batch_id": "p1",
    "records": [
        {
            "product_id": "P1",
            "details": {"sku": "S1", "name": "Drill", "brand": "Acme"},
            "category": {"id": "TOOLS", "path": ["Tools", "Power Tools"]},
            "prices": [{"currency": "USD", "amount": 9.99, "is_current": True}],
        }
    ],
}


def write(tmp_path, name, content):
    path = tmp_path / name
    path.write_bytes(content if isinstance(content, bytes) else json.dumps(content).encode())
    return path


def test_accepted_file_exposes_envelope_fields(pipeline_module, tmp_path):
    path = write(tmp_path, "orders.json", ORDERS_DOC)
    check = pipeline_module.inspect_file(path, pipeline_module.ORDERS)

    assert check.status == "ACCEPTED" and check.reason is None
    assert check.record_count == 1 and check.records == ORDERS_DOC["records"]
    assert (check.batch_id, check.envelope_source) == ("b1", "order_service")
    assert check.sha256 == hashlib.sha256(path.read_bytes()).hexdigest()
    assert check.size == len(path.read_bytes()) and check.content == path.read_bytes()


@pytest.mark.parametrize(("spec_name", "doc"), [("ORDERS", ORDERS_DOC), ("PRODUCTS", PRODUCTS_DOC)])
def test_schemas_accept_the_documented_shapes(pipeline_module, tmp_path, spec_name, doc):
    spec = getattr(pipeline_module, spec_name)
    check = pipeline_module.inspect_file(write(tmp_path, spec.filename, doc), spec)
    assert (check.status, check.reason, check.record_count) == ("ACCEPTED", None, 1)


def test_missing_file_raises(pipeline_module, tmp_path):
    with pytest.raises(FileNotFoundError):
        pipeline_module.inspect_file(tmp_path / "nope.json", pipeline_module.ORDERS)


def test_invalid_json_is_quarantined(pipeline_module, tmp_path):
    check = pipeline_module.inspect_file(write(tmp_path, "orders.json", b"{not json"), pipeline_module.ORDERS)
    assert check.status == "QUARANTINED"
    assert check.reason.startswith("invalid JSON: ")
    assert check.record_count is None and check.records == []
    assert (check.batch_id, check.envelope_source) == (None, None)


def test_wrong_envelope_source_is_quarantined_but_still_reports_it(pipeline_module, tmp_path):
    doc = {**ORDERS_DOC, "source": "payment_service"}
    check = pipeline_module.inspect_file(write(tmp_path, "orders.json", doc), pipeline_module.ORDERS)
    assert check.status == "QUARANTINED"
    assert check.reason.startswith("schema orders.v1: $.source: ")
    assert (check.batch_id, check.envelope_source) == ("b1", "payment_service")


def test_wrong_type_is_quarantined_with_json_path(pipeline_module, tmp_path):
    doc = copy.deepcopy(ORDERS_DOC)
    doc["records"][0]["items"][0]["quantity"] = "two"
    check = pipeline_module.inspect_file(write(tmp_path, "orders.json", doc), pipeline_module.ORDERS)

    assert check.status == "QUARANTINED"
    assert check.reason.startswith("schema orders.v1: $.records[0].items[0].quantity: ")
    assert check.record_count == 1  # parseable, so the count is still known


def test_business_rule_violation_is_not_a_file_level_error(pipeline_module, tmp_path):
    """quantity <= 0 is rejected per order in silver; the file contract only checks types."""
    doc = copy.deepcopy(ORDERS_DOC)
    doc["records"][0]["items"][0]["quantity"] = -1
    check = pipeline_module.inspect_file(write(tmp_path, "orders.json", doc), pipeline_module.ORDERS)
    assert check.status == "ACCEPTED"


def test_non_dict_document_is_quarantined_without_envelope_fields(pipeline_module, tmp_path):
    check = pipeline_module.inspect_file(write(tmp_path, "products.json", []), pipeline_module.PRODUCTS)
    assert check.status == "QUARANTINED"
    assert (check.batch_id, check.envelope_source) == (None, None)


def test_no_records_is_quarantined(pipeline_module, tmp_path):
    doc = {**PRODUCTS_DOC, "records": []}
    check = pipeline_module.inspect_file(write(tmp_path, "products.json", doc), pipeline_module.PRODUCTS)
    assert (check.status, check.record_count) == ("QUARANTINED", 0)
    assert check.reason  # either a schema minItems violation or "no records"


def test_first_error_is_deterministic(pipeline_module):
    schema = {"type": "object", "properties": {"b": {"type": "integer"}, "a": {"type": "integer"}}}
    reason = pipeline_module.first_schema_error({"b": "x", "a": "y"}, schema, "t.v1")
    assert reason.startswith("schema t.v1: $.a: ")
    assert pipeline_module.first_schema_error({"a": 1, "b": 2}, schema, "t.v1") is None


def test_zone_path_contains_sanitized_run_id(pipeline_module, tmp_path):
    run_id = "manual__2026-10-19T00:00:00+00:00"
    path = pipeline_module.zone_path(tmp_path / "bronze", "orders", "2026-10-19", run_id, "orders.json")

    assert path.parts[-4:-1] == ("orders", "dt=2026-10-19", "manual__2026-10-19T00_00_00_00_00")
    assert path.name == "orders.json" and ":" not in str(path) and "+" not in str(path)
    quarantine = pipeline_module.zone_path(tmp_path / "quarantine", "orders", "2026-10-19", run_id, "orders.json")
    assert "quarantine" in quarantine.parts


def test_expected_rows_warning(pipeline_module):
    warn = pipeline_module.expected_rows_warning
    assert warn(None, 10) is None  # first delivery
    assert warn(10, 13) is None  # +30% is inside the tolerance
    assert warn(10, 7) is None
    assert "+40%" in warn(10, 14)
    assert "-50%" in warn(10, 5)


def test_decide_release(pipeline_module):
    decide = pipeline_module.decide_release
    all_ok = {"orders": True, "products": True}
    assert decide(all_ok, True) == ("PUBLISHED", "")

    status, reason = decide({**all_ok, "orders": False}, True)
    assert status == "HELD" and "orders" in reason

    status, reason = decide(all_ok, False)
    assert status == "HELD" and "gold" in reason
