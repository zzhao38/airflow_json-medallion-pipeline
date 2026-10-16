# airflow_json-medallion-pipeline

An end-to-end JSON pipeline built with **Apache Airflow 2.11** and **PostgreSQL**, organized
into **Bronze → Silver → Gold** (medallion) layers.

Three nested JSON documents (`products`, `users`, `store`) are landed as hosted files, loaded
raw, normalized into 3NF relational tables, and rolled up into analytics-ready models — with
data-quality checks, idempotent reruns, an audit trail and a test suite. Everything runs
locally in Docker.

**Stack:** Airflow (LocalExecutor) · Postgres (separate metadata DB and warehouse) · SQL-first
transforms · `apache-airflow-providers-common-sql` checks · pytest · Docker Compose.

---

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
| **Landing** | `data/landing/*.json` | Files as delivered. Nothing here is trusted. |
| **Bronze** | `data/bronze/<source>/dt=<ds>/` + `bronze.raw_<source>` (JSONB, one row per record) | Immutable, replayable copy of what arrived, keyed by `(logical_date, record_index)`. Schema-on-read. |
| **Silver** | `silver.*` | Typed, deduplicated, normalized (3NF) tables with PK / FK / CHECK constraints. Business keys, no aggregation. |
| **Gold** | `gold.*` | Denormalized, question-shaped models, rebuilt each run from silver. |
| **Ops** | `ops.load_audit` | Row counts per layer / table per DAG run. |

Each source gets its own lane inside three `TaskGroup`s (bronze / silver / gold), so a failure
in one source does not block the others.

### DAGs

| DAG | Trigger |
|---|---|
| `json_medallion_pipeline` | `@daily` schedule (`catchup=False`, `max_active_runs=1`) |
| `json_medallion_pipeline_filesensor` | file arrival — same task graph, gated by three `FileSensor`s |

Both are self-contained single files with explicit per-source tasks (some deliberate
duplication) in favour of readability over a config-driven factory. Unpause only one of them.

---

## Key design decisions

- **Idempotency at every layer.** Bronze: delete-then-insert the `logical_date` partition
  (plus `UNIQUE(logical_date, record_index)`). Silver: upsert parents on natural keys, replace
  children scoped to the parents in the batch, all in a single transaction. Gold:
  `TRUNCATE + INSERT` in one transaction. Audit: upsert on `(dag_run_id, layer, table_name)`.
  Re-running a task, run or date range leaves the warehouse in the same state.
- **Normalization.** Nested arrays are unnested with `jsonb_array_elements … WITH ORDINALITY`
  into child tables keyed by parent + ordinal. The open-ended `specifications` object is stored
  as an **EAV** child table, so new spec keys need no migration; fixed attributes stay typed.
  Money is `NUMERIC(12,2)`, never float.
- **Data quality as tasks.** Failed checks stop downstream layers and show in the UI:
  bronze partition row count > 0 (`SQLTableCheckOperator`); silver null / unique /
  non-negative checks (`SQLColumnCheckOperator`); gold ↔ silver reconciliation
  (`SQLCheckOperator`); plus PK / FK / UNIQUE / CHECK constraints and pre-load file / JSON /
  `record_path` validation.
- **Auditability.** The landed file's SHA-256, source path and DAG run id are stored with every
  bronze row, and per-run row counts land in `ops.load_audit`.
- **SQL-first.** All transforms live in `dags/json_medallion_pipeline/sql/**`, rendered with
  Airflow templating (`{{ ds }}`).
- **Two Postgres instances on purpose:** Airflow metadata and the warehouse never share a database.

### Gold models

| Model | Grain |
|---|---|
| `gold.user_activity_summary` | one row per user (activity count, total minutes) |
| `gold.product_total_value` | one row per product (`price * quantity`) |
| `gold.product_variation_flat` | one row per variation, specs pivoted to columns |
| `gold.inventory_value_by_category` | one row per store × category |

---

## Running locally

**Prerequisites:** Docker Engine + Compose v2 (Rancher Desktop or Docker Desktop), `make`, and
free ports **8080** (Airflow UI) and **5433** (warehouse). No local Python needed; tests run in
the Airflow image.

The input JSON files are **not included in the repository**. Provide your own
`products.json`, `users.json` and `store.json` in `data/landing/` (git-ignored):

- `products.json` — flat list of `{id, name, category, price, quantity}`
- `users.json` — `{ "users": [ {id, name, profile: {age, email, activities: [{type, duration_minutes}]}} ] }`
- `store.json` — `{ "store": {name, location, products: [ {id, name, category, variations: [...]} ]} }`

```bash
make init              # .env from template, runtime dirs, build image, migrate Airflow DB
# place products.json, users.json, store.json in data/landing/
docker compose up -d   # start the stack
make trigger           # unpause + trigger one run of json_medallion_pipeline
```

> `make up` also starts the stack but first runs `make seed`, which copies
> `data/samples/*.json` into `data/landing/`; use it only if you keep sample files at that path.

- **Airflow UI:** http://localhost:8080 (`airflow` / `airflow`)
- **Warehouse:** `make psql`, or `postgresql://warehouse:warehouse@localhost:5433/warehouse`

```sql
SELECT * FROM gold.user_activity_summary;
SELECT * FROM gold.inventory_value_by_category;
SELECT * FROM ops.load_audit ORDER BY audit_id DESC LIMIT 20;
```

Useful targets (`make help` lists all): `make down`, `make logs`, `make ps`, `make psql`,
`make backfill START=2026-10-01 END=2026-10-03` (re-run a date range to see idempotency),
`make clean` (remove landed / bronze files and logs), `make reset` (full wipe incl. DB volumes).

---

## Tests

```bash
make test               # all tests, inside the Airflow image
make test-unit          # helper + DAG-integrity tests (no warehouse needed)
make test-integration   # end-to-end SQL tests against the running warehouse (stack must be up)
```

| File | Covers |
|---|---|
| `tests/test_ingest.py` | record extraction for flat / nested / single-object documents, error paths, landing path + checksum, overwrite on rerun, bronze row shaping |
| `tests/test_dag_integrity.py` | DagBag loads cleanly; expected task ids and dependencies; DAG settings; FileSensor gating; referenced SQL files exist |
| `tests/test_pipeline_integration.py` | DDL → bronze → silver → gold for a fixture date; asserts exact values, reconciliation checks, and that a **second run leaves every count unchanged** |

---

## What to inspect

```
dags/json_medallion_pipeline/   # scheduled + file-arrival DAGs and all SQL (bronze/silver/gold/ops/checks)
tests/                          # ingest helpers, DAG integrity, end-to-end SQL
docker-compose.yml, Dockerfile  # metadata DB, warehouse, scheduler, webserver (Airflow 2.11)
Makefile                        # make help
```
