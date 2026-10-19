# airflow_json-medallion-pipeline

A local **Airflow 2.11 + PostgreSQL** pipeline that loads two nested JSON feeds (`orders`, `products`)
through **Bronze → Silver → Gold**. Bad files are quarantined, bad records are rejected one by one,
and consumers only ever see a gold **release** that passed every check. Every gold number traces
back to the raw JSON it came from.

## Quickstart

Requires Docker with Compose v2 (Docker Desktop, Rancher Desktop with the **dockerd/moby** engine,
or Docker Engine on Linux), ~4 GB RAM and `make`. No local Python needed.

```bash
make up        # build, migrate, start; copies data/samples/*.json into data/landing/
make trigger   # run the DAG once (all green in about a minute)
make psql      # open the warehouse
```

- **Airflow UI:** http://localhost:8080 (`airflow` / `airflow`)
- **Warehouse:** `postgresql://warehouse:warehouse@localhost:5433/warehouse`
- Ports busy? Set `AIRFLOW_PORT` / `WAREHOUSE_PORT` in `.env` and rerun `make up`.

## How a run flows

```mermaid
flowchart TB
    L[/"data/landing<br/>orders.json · products.json"/] --> V{"hash + parse +<br/>JSON Schema"}
    V -- invalid --> Q["QUARANTINED<br/>file kept, lane skipped"]
    V -- same bytes --> D["DUPLICATE<br/>no new records"]
    V -- valid --> A["ACCEPTED<br/>bronze.arrival + record"]
    A --> S{"silver<br/>business rules"}
    D --> S
    S -- bad record --> RJ["silver.rejected_record"]
    S -- unknown product --> PK["parked item<br/>kept out of gold"]
    S -- ok --> SV["silver 3NF<br/>SCD-2 customers, products"]
    SV --> G["gold build<br/>new release_id"]
    G --> C{"reconciliation OK<br/>and all lanes arrived?"}
    Q -.-> C
    C -- yes --> P["PUBLISHED<br/>flip ops.destination"]
    C -- no --> H["HELD<br/>run fails"]
    P --> CV["analytics.* / finance.* views"]
    H -. "keep last good release" .-> CV

    classDef ok fill:#e3f6e8,stroke:#2e7d32
    classDef warn fill:#fff4d6,stroke:#c08a00
    classDef bad fill:#fde2e1,stroke:#c62828
    class A,D,SV,P,CV ok
    class PK warn
    class Q,RJ,H bad
```

- **Bronze** is append-only: one `bronze.arrival` row per delivery (SHA-256, schema version, `batch_id`, status) and the raw records in `bronze.record`.
- **Silver** is typed 3NF; `customers` and `products` keep full history (SCD-2). A record that breaks a business rule (e.g. `quantity <= 0`) is rejected on its own; the rest of the file loads.
- **Gold** is rebuilt as a new release each run. `publish_release` flips one pointer, so consumers switch atomically or not at all.

The DAG is `json_medallion_pipeline` (weekdays 02:00 UTC, `catchup=False`); SQL lives in `dags/json_medallion_pipeline/sql/`, contracts in `schemas/`.

## What to look at

```sql
SELECT * FROM analytics.customer_summary;                              -- consumer view, with release_version
SELECT source, status, quarantine_reason FROM bronze.arrival ORDER BY arrival_id;
SELECT entity_key, reason FROM silver.rejected_record;                 -- O1003: quantity -1 must be > 0
SELECT release_version, status, hold_reason FROM ops.release ORDER BY release_id;
SELECT order_item_id, source_file, bronze_payload FROM gold.v_trace_order_lines LIMIT 3;  -- gold -> raw JSON
```

The sample orders file is dirty on purpose: `O1002` is sent twice (deduplicated with a WARN) and `O1003` has a negative quantity (rejected).

## Try a failure

```bash
cp data/samples/scenarios/orders_bad_schema.json data/landing/orders.json   # quantity: "two"
make trigger    # orders quarantined, release HELD, run fails; consumers keep the last release
cp data/samples/orders.json data/landing/orders.json && make trigger        # recovers
```

Every scenario (file-level, record-level, warning-only) is listed with its expected outcome in `data/samples/scenarios/manifest.json`.

## Design decisions

- **Immutable bronze:** a corrected file is a new arrival; identical bytes are a `DUPLICATE`, so reruns change nothing.
- **Contract at the door, rules per record:** JSON Schema quarantines a broken file; business rules in silver SQL reject only the bad record.
- **Release gating:** gold is built, reconciled against silver, then published by flipping `ops.destination`; a failed build never reaches consumers and rollback is a pointer change.
- **History and lineage:** hash-gated SCD-2 in silver; every gold row resolves to its bronze record via `gold.v_trace_*`.
- **Explicit over clever:** one readable DAG file with every task written out, SQL in files, `NUMERIC` money.

## Tests

```bash
make test               # everything, inside the Airflow image
make test-unit          # helpers, schemas, scenario files, DAG integrity (no database)
make test-integration   # end-to-end SQL in a throwaway warehouse_test database
```

## Layout

```
dags/json_medallion_pipeline/   # the DAG, schemas/ (JSON Schema), sql/ (bronze, silver, gold, ops, checks)
data/samples/                   # input files and scenarios/ (one file per failure case + manifest.json)
tests/                          # unit, DAG integrity, scenario and integration tests
docker-compose.yml, Dockerfile  # Airflow (LocalExecutor), metadata Postgres, warehouse Postgres
Makefile                        # make help
```
