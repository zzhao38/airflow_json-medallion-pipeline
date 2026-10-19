# airflow_json-medallion-pipeline

An end-to-end JSON pipeline on **Apache Airflow 2.11** and **PostgreSQL**, organized into
**Bronze → Silver → Gold** layers. Two nested JSON documents (`orders`, `products`)
are schema-validated, stored immutably, normalized to 3NF with history (SCD-2), and rolled up into
analytics models that consumers only see once a whole **release** has passed every check.
Includes quarantine of bad files, idempotent reruns, and row-level lineage from a gold number back to
the raw JSON. Runs locally in Docker.

**Stack:** Airflow (LocalExecutor) · Postgres (separate metadata DB and warehouse) · SQL-first transforms · JSON Schema · pytest · Docker Compose.

## Architecture

```mermaid
flowchart LR
    subgraph Landing["data/landing (hosted files)"]
        F1[products.json]
        F2[orders.json]
    end

    subgraph Airflow["Airflow · LocalExecutor"]
        direction LR
        S[setup_schemas] --> B
        S --> R[create_release]
        subgraph B["bronze"]
            direction TB
            I[ingest_*]
        end
        B --> SV
        subgraph SV["silver"]
            direction TB
            T[transform_*] --> SC[check_*]
        end
        SV --> G
        R --> G
        subgraph G["gold"]
            direction TB
            GB[build_*] --> GC[check_*_reconciliation]
        end
        G --> P[publish_release]
    end

    subgraph PG["Postgres · warehouse"]
        direction TB
        BZ[("bronze.arrival / record<br/>append-only, JSONB")]
        SL[("silver.*<br/>3NF, SCD-2 parents")]
        GD[("gold.*<br/>one build per release")]
        OPS[("ops.release / destination<br/>ops.quality_issue")]
        CV[("analytics.* / finance.*<br/>active release only")]
    end

    Landing --> I
    I -. "valid: copy" .-> BF[/"data/bronze/&lt;source&gt;/dt=&lt;ds&gt;/&lt;run&gt;/"/]
    I -. "invalid: copy" .-> QF[/"data/quarantine/..."/]
    I --> BZ
    T --> SL
    GB --> GD
    P --> OPS
    OPS --> CV
    GD --> CV
```

| Layer | Storage | Contract |
|---|---|---|
| **Landing** | `data/landing/*.json` | Files as delivered; untrusted. |
| **Bronze** | `bronze.arrival` + `bronze.record` (JSONB) and `data/bronze/<source>/dt=<ds>/<run>/` | Append-only record of every delivery. Each file is hashed (SHA-256) and validated against `schemas/<source>.v1.json`. Valid → `ACCEPTED`; identical bytes for the same date → `DUPLICATE` (no new records); invalid → `QUARANTINED` (file kept in `data/quarantine/`, nothing reaches `bronze.record`). |
| **Silver** | `silver.*` | Typed 3NF tables. Parents (`customers`, `products`) are **SCD-2**: a new version only when the row hash changes. `orders` / `order_items` are upserted by key; `categories` and `product_prices` hold the catalog hierarchy and price history. Invalid orders go to `silver.rejected_record` (the rest of the file still loads). Every row carries `bronze_arrival_id` / `bronze_record_index`. |
| **Gold** | `gold.*` | Question-shaped models, keyed `(release_id, …)`. Each run builds a **new release**; earlier releases are never rewritten. Traceable to raw JSON via `gold.v_trace_*`. |
| **Consumers** | `analytics.*`, `finance.*` | Views over the destination's **active release** (`ops.destination`). Rows carry `release_version` and `released_at`. A held build is invisible: consumers keep seeing the last good release. |
| **Ops** | `ops.release`, `ops.destination`, `ops.quality_issue` | Release status (`BUILDING` / `PUBLISHED` / `HELD`), input arrivals, row counts, hold reason, WARN/ERROR issues. |

**DAG** (`json_medallion_pipeline`): `setup_schemas` → `bronze.ingest_{orders,products}` → `silver.transform_products` → `silver.check_products` → `silver.transform_orders` → `silver.check_orders`; `create_release` plus both silver checks feed the four `gold.build_*` tasks (`order_lines`, `daily_sales_by_category`, `customer_summary`, `product_performance`) → `gold.check_reconciliation` → `publish_release`.

`json_medallion_pipeline` runs `@daily` (`catchup=False`, `max_active_runs=1`, 2 retries, 1-minute delay); trigger it manually with `make trigger`.

### Failure behaviour

- A **quarantined** file skips its own lane. Orders also wait for the products lane (the orders transform looks up products; items with an unknown product are parked), so a quarantined products file skips the orders transform too; a quarantined orders file never blocks products.
- `publish_release` is the single sink (`ALL_DONE`). If any lane was quarantined or any gold task did not succeed, the release is marked `HELD` with a reason, an ERROR is recorded in `ops.quality_issue`, **the run fails** (so it is visible), and consumers stay on the previous release.
- Fix the file and trigger again: it creates release `<ds>.<n+1>` and publishes it (both destinations flip in one transaction).
- A record count that differs from the previous accepted file by more than 30% is a **WARN** only: the file is still accepted and the release publishes.

## Prerequisites

- Docker Engine with Compose v2 (`docker compose`) via Docker Desktop, Rancher Desktop, or Docker Engine on Linux; ~4 GB RAM for the VM/engine; GNU `make`. No local Python needed (tests run in the image).
- Free host ports 8080 (UI) and 5433 (warehouse), or set `AIRFLOW_PORT` / `WAREHOUSE_PORT` in `.env`.
- Rancher Desktop: choose the **dockerd (moby)** container engine (Preferences > Container Engine; containerd/nerdctl has no `docker compose`) and make sure `docker context show` prints `rancher-desktop`.
- Linux Docker Engine: set `AIRFLOW_UID=$(id -u)` in `.env` if you hit permission errors on `logs/` or `data/`.

## Quickstart

`make up` copies `data/samples/{orders,products}.json` into `data/landing/` (never overwrites existing files). Both are envelopes with `source`, `batch_id` and `records`:

- `products.json`: records of `{product_id, details: {sku, name, brand}, category: {id, path: [...]}, prices: [{currency, amount, is_current}]}`
- `orders.json`: records of `{order_id, ordered_at, customer: {customer_id, name}, shipping: {city, state, method}, items: [{order_item_id, product_id, quantity, unit_price}]}`

```bash
make up        # create .env, build, migrate, start the stack (copies samples into data/landing only if present)
make trigger   # unpause + run json_medallion_pipeline once
make psql      # query the warehouse
```

- **Airflow UI:** http://localhost:8080 (`airflow` / `airflow`)
- **Warehouse:** `postgresql://warehouse:warehouse@localhost:5433/warehouse`
- Ports come from `.env` (`AIRFLOW_PORT`, `WAREHOUSE_PORT`; defaults 8080 / 5433); edit and re-run `make up` if they clash.
- On Docker socket errors with Rancher Desktop, restart Rancher Desktop.

Other targets (`make help`): `down`, `restart`, `logs`, `ps`, `seed`, `unpause`, `backfill START=… END=…`, `clean`, `reset` (wipes DB volumes).

Consumers query the views, not `gold.*`:

```sql
SELECT * FROM analytics.customer_summary;            -- includes release_version, released_at
SELECT * FROM analytics.product_performance;         -- price_variance = order price - catalog price
SELECT * FROM finance.daily_sales_by_category;
SELECT release_version, status, hold_reason FROM ops.release ORDER BY release_id;
```

## Key design decisions

- **Append-only bronze, idempotent everywhere.** Bronze only inserts; a corrected file is a new arrival, and re-delivering identical bytes is a `DUPLICATE` that adds nothing. Silver versions a parent only when its hash changes, so replaying a delivery changes nothing. Gold builds delete and re-insert only their own `release_id`.
- **Release gating instead of in-place overwrite.** Gold is built into a new release, checked, then published by flipping one pointer (`ops.destination.active_release_id`). Failed builds never reach consumers, and old releases stay queryable.
- **Validate at the door.** JSON Schema per source (`dags/json_medallion_pipeline/schemas/`) plus a "no records" rule; failures are quarantined with the first error and its JSON path (e.g. `$.records[0].items[0].quantity`).
- **SCD-2 silver with 3NF.** Nested arrays are unnested with `WITH ORDINALITY` into child tables; open-ended `specifications` are an EAV table so new keys need no migration. Money is `NUMERIC`, never float.
- **Data-quality gates as tasks.** Silver null/unique/range/orphan checks and gold-to-silver reconciliation (scoped to the release being built) stop downstream work on failure and show in the UI, alongside PK / FK / CHECK constraints.
- **Row-level lineage.** Silver and per-row gold tables carry `bronze_arrival_id` / `bronze_record_index`; the category aggregate carries a `bronze_lineage` JSONB array. `gold.v_trace_*` joins any gold row back to the arrival (file, SHA-256, DAG run) and its raw JSON.
- **SQL-first with explicit per-source tasks.** Transforms live in `dags/json_medallion_pipeline/sql/**`, rendered with Airflow templating (`{{ ds }}`, `ti.xcom_pull`), favouring readability over a config-driven factory.

```sql
SELECT * FROM gold.v_trace_order_lines LIMIT 1;   -- gold row -> arrival (file, SHA-256, run) -> raw JSON
```

## Tests

```bash
make test               # everything, inside the Airflow image
make test-unit          # helper + DAG-integrity tests (no warehouse needed; ignores tests/local)
make test-integration   # end-to-end SQL tests (12; stack must be up)
```

Integration tests run in a throwaway `warehouse_test` database (created and dropped per session; the demo warehouse is untouched) and need the sample and scenario files in `data/samples/`; without them they are skipped. They cover: first release and values, lineage to the raw payload, rerun idempotency, SCD-2 versioning on a changed record, quarantine plus a held release, recovery after a fix, and the row-count WARN.

## What to inspect

```
dags/json_medallion_pipeline/   # the DAG, JSON Schemas, and all SQL (bronze/silver/gold/ops/checks)
data/quarantine/                # rejected files, by source / date / run
tests/                          # ingest helpers, DAG integrity, end-to-end SQL
docker-compose.yml, Dockerfile  # metadata DB, warehouse, scheduler, webserver
Makefile                        # make help
```
