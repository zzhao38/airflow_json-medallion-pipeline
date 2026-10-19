# airflow_json-medallion-pipeline

An end-to-end JSON pipeline on **Apache Airflow 2.11** and **PostgreSQL**, organized into
**Bronze → Silver → Gold** layers. Three nested JSON documents (`products`, `users`, `store`)
are landed, loaded raw, normalized to 3NF, and rolled up into analytics models, with
data-quality gates, idempotent reruns, an audit trail and row-level lineage. Runs locally in Docker.

**Stack:** Airflow (LocalExecutor) · Postgres (separate metadata DB and warehouse) · SQL-first transforms · pytest · Docker Compose.

## Architecture

```mermaid
flowchart LR
    subgraph Landing["data/landing (hosted files)"]
        F1[products.json]
        F2[users.json]
        F3[store.json]
    end

    subgraph Airflow["Airflow · LocalExecutor"]
        direction LR
        S[setup_schemas] --> B
        subgraph B["bronze"]
            direction TB
            L[land_*] --> LD[load_*] --> BC[check_*]
        end
        B --> SV
        subgraph SV["silver"]
            direction TB
            T[transform_*] --> SC[check_*]
        end
        SV --> G
        subgraph G["gold"]
            direction TB
            GB[build_*] --> GC[check_*_reconciliation]
        end
        G --> A[record_load_audit]
    end

    subgraph PG["Postgres · warehouse"]
        direction TB
        BZ[("bronze.raw_*<br/>JSONB")]
        SL[("silver.*<br/>3NF, PK/FK/CHECK")]
        GD[("gold.*<br/>consumer models")]
        OPS[("ops.load_audit")]
    end

    Landing --> L
    LD --> BZ
    T --> SL
    GB --> GD
    A --> OPS
```

| Layer | Storage | Contract |
|---|---|---|
| **Landing** | `data/landing/*.json` | Files as delivered; untrusted. |
| **Bronze** | `bronze.raw_<source>` (JSONB) + `data/bronze/<source>/dt=<ds>/` | Raw, replayable copy, one row per record, keyed `(logical_date, record_index)`; each `logical_date` partition is replaced on rerun. |
| **Silver** | `silver.*` | Typed, deduplicated 3NF tables with PK / FK / CHECK. Each row carries `bronze_logical_date` / `bronze_record_index`. |
| **Gold** | `gold.*` | Question-shaped models rebuilt each run. Every gold row traces to its bronze record via `gold.v_trace_*`. |
| **Ops** | `ops.load_audit` | Row counts per layer, table and DAG run. |

`json_medallion_pipeline` runs `@daily` (`catchup=False`, `max_active_runs=1`); trigger it manually with `make trigger`.

## Prerequisites

- Docker Engine with Compose v2 (`docker compose`) via Docker Desktop, Rancher Desktop, or Docker Engine on Linux; ~4 GB RAM for the VM/engine; GNU `make`. No local Python needed (tests run in the image).
- Free host ports 8080 (UI) and 5433 (warehouse), or set `AIRFLOW_PORT` / `WAREHOUSE_PORT` in `.env`.
- Rancher Desktop: choose the **dockerd (moby)** container engine (Preferences > Container Engine; containerd/nerdctl has no `docker compose`) and make sure `docker context show` prints `rancher-desktop`.
- Linux Docker Engine: set `AIRFLOW_UID=$(id -u)` in `.env` if you hit permission errors on `logs/` or `data/`.

## Quickstart

Input files are not generated for you: provide `products.json`, `users.json` and `store.json` in `data/landing/`.

- `products.json`: flat list of `{id, name, category, price, quantity}`
- `users.json`: `{"users": [{id, name, profile: {age, email, activities: [{type, duration_minutes}]}}]}`
- `store.json`: `{"store": {name, location, products: [{id, name, category, variations: [...]}]}}`

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

## Key design decisions

- **Idempotent at every layer.** Bronze delete-then-inserts the `logical_date` partition; silver upserts on natural keys in one transaction; gold is `TRUNCATE + INSERT` in one transaction; the audit upserts per run. Reruns and backfills leave the same state.
- **3NF with EAV for open specs.** Nested arrays are unnested with `WITH ORDINALITY` into child tables; open-ended `specifications` are an EAV table so new keys need no migration. Money is `NUMERIC`, never float.
- **Data-quality gates as tasks.** Bronze row counts, silver null/unique/range checks and gold-to-silver reconciliation stop downstream layers on failure and show in the UI, alongside PK/FK/CHECK constraints.
- **Row-level lineage and audit.** Silver and per-row gold tables carry `bronze_logical_date` / `bronze_record_index`; the category aggregate carries a `bronze_lineage` JSONB array. Bronze stores source file, SHA-256 and DAG run id. `gold.v_trace_*` joins any gold row back to its raw JSON.
- **SQL-first with explicit per-source tasks.** Transforms live in `dags/json_medallion_pipeline/sql/**`, rendered with Airflow templating, favouring readability over a config-driven factory.

```sql
SELECT user_id, name, source_file, file_sha256, payload FROM gold.v_trace_user_activity_summary WHERE user_id = 1;
```

## Tests

```bash
make test               # everything, inside the Airflow image
make test-unit          # helper + DAG-integrity tests (no warehouse needed)
make test-integration   # end-to-end SQL tests incl. rerun idempotency and lineage (stack must be up)
```

Integration tests run in a throwaway `warehouse_test` database (created and dropped per session; the demo warehouse is untouched) and need the three sample files in `data/samples/`; without them they are skipped.

## What to inspect

```
dags/json_medallion_pipeline/   # the DAG and all SQL (bronze/silver/gold/ops/checks, gold/trace_views.sql)
tests/                          # ingest helpers, DAG integrity, end-to-end SQL
docker-compose.yml, Dockerfile  # metadata DB, warehouse, scheduler, webserver
Makefile                        # make help
```
