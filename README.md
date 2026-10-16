# airflow_json-medallion-pipeline

An end-to-end JSON data pipeline using **Apache Airflow 2** and **PostgreSQL**, organized into
**Bronze → Silver → Gold** (medallion) layers.

Three nested JSON documents are landed as separate hosted files, loaded raw, normalized into
3NF relational tables, and rolled up into analytics-ready models — with data-quality checks,
idempotent reruns, an audit trail and a test suite. Everything runs locally with one command.

```bash
make up && make trigger
```

---

## Contents

- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quickstart](#quickstart)
- [What the DAG does](#what-the-dag-does)
- [Data model](#data-model)
- [Idempotency & reruns](#idempotency--reruns)
- [Data-quality checks](#data-quality-checks)
- [Tests](#tests)
- [Repository layout](#repository-layout)
- [Configuration](#configuration)
- [Adding / removing a source](#adding--removing-a-source)
- [Troubleshooting](#troubleshooting)

---

## Architecture

```mermaid
flowchart LR
    subgraph Landing["data/landing (hosted files)"]
        F1[products.json]
        F2[users.json]
        F3[store.json]
    end

    subgraph Airflow["Airflow 2.11 · LocalExecutor"]
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
        BZ[("bronze.raw_*<br/>JSONB, partitioned by logical_date")]
        SL[("silver.*<br/>3NF, PK/FK/CHECK")]
        GD[("gold.*<br/>consumer models")]
        OPS[("ops.load_audit")]
    end

    Landing --> L
    L -. copy .-> BF[/"data/bronze/&lt;source&gt;/dt=YYYY-MM-DD/"/]
    LD --> BZ
    T --> SL
    GB --> GD
    A --> OPS
```

| Layer | Storage | Contract |
|---|---|---|
| **Landing** | `data/landing/*.json` | Files as delivered. Nothing here is trusted. |
| **Bronze** | `data/bronze/<source>/dt=<ds>/<file>` + `bronze.raw_<source>` (JSONB, one row per record) | Immutable, replayable copy of exactly what arrived, keyed by `(logical_date, record_index)`. Schema-on-read. |
| **Silver** | `silver.*` | Typed, deduplicated, normalized (3NF) tables with PK / FK / CHECK constraints. Business keys, no aggregation. |
| **Gold** | `gold.*` | Denormalized, question-shaped models rebuilt on every run from silver. |
| **Ops** | `ops.load_audit` | Row counts per layer / table per DAG run. |

Two Postgres instances run on purpose: one is Airflow's own metadata DB, the other is the
**warehouse** the pipeline writes to. They never share a database.

---

## Prerequisites

- **Docker Engine + Docker Compose v2** — via **Rancher Desktop** (tested, `dockerd/moby`
  runtime) or Docker Desktop. ~4 GB RAM for the VM is plenty.
- `make` (ships with macOS Xcode CLT / most Linux distros).
- Free host ports **8080** (Airflow UI) and **5433** (warehouse) — both configurable in `.env`.

No local Python is required; tests run inside the Airflow image.

> **Rancher Desktop:** make sure the `rancher-desktop` Docker context is active
> (`docker context ls`). Apple-silicon and x86 both work — the compose file pulls
> multi-arch images.

---

## Quickstart

```bash
git clone https://github.com/zzhao38/airflow_json-medallion-pipeline.git
cd airflow_json-medallion-pipeline

make up        # .env from template, build image, migrate Airflow DB, start stack, seed landing files
make trigger   # unpause + trigger one run of json_medallion_pipeline
```

Then:

- **Airflow UI** → http://localhost:8080 (`airflow` / `airflow`) — open DAG
  `json_medallion_pipeline` → *Graph* to watch the run.
- **Warehouse** → `make psql`, or any client at `postgresql://warehouse:warehouse@localhost:5433/warehouse`.

```sql
SELECT * FROM gold.user_activity_summary;
SELECT * FROM gold.product_total_value;
SELECT * FROM gold.inventory_value_by_category;
SELECT * FROM gold.product_variation_flat;
SELECT * FROM ops.load_audit ORDER BY audit_id DESC LIMIT 20;
```

Useful targets (`make help` lists all):

| Target | Purpose |
|---|---|
| `make up` / `make down` | Start / stop the stack (volumes are kept) |
| `make seed` | Copy `data/samples/*.json` into `data/landing/` |
| `make trigger` | Unpause and trigger the main DAG |
| `make backfill START=2026-10-01 END=2026-10-03` | Re-run a date range (demonstrates idempotency) |
| `make logs` / `make ps` | Tail scheduler + webserver logs / show containers |
| `make psql` | psql shell on the warehouse |
| `make test` / `make test-unit` / `make test-integration` | Run the test suite inside the Airflow image |
| `make clean` | Delete landed / bronze files and logs (keeps DB volumes) |
| `make reset` | `down -v` + clean — full wipe |

---

## What the DAG does

The repo ships **two DAGs** with the same task graph and different triggers:

| DAG | File | Trigger | Default state |
|---|---|---|---|
| `json_medallion_pipeline` | `dags/json_medallion_pipeline/json_medallion_pipeline.py` | `@daily` schedule | paused at creation (compose default); `make trigger` unpauses it |
| `json_medallion_pipeline_filesensor` | `dags/json_medallion_pipeline/json_medallion_pipeline_filesensor.py` | file arrival (`FileSensor`s) | paused (compose default) |

Each file is self-contained: helpers (`land_file`, `bronze_rows`, …) and every task are
spelled out top to bottom, at the cost of some duplication between the two files. (A
config-driven factory variant — shared source registry plus a task-builder module — was
prototyped and set aside in favour of explicit per-source tasks for readability.) Unpause only
one of the two to avoid redundant loads of the same partition.

`json_medallion_pipeline` runs `@daily` (`catchup=False`, `max_active_runs=1`). Each source
gets its own **lane** inside three `TaskGroup`s, so lanes run in parallel and a failure in
one source does not block the others.

| Step | Task(s) | What happens |
|---|---|---|
| 0 | `setup_schemas` | Applies all `CREATE … IF NOT EXISTS` DDL (bronze / silver / gold / ops) in one transaction. The pipeline bootstraps an empty database. |
| 1 | `bronze.land_<source>` | Validates the landing file is parseable JSON and that the configured `record_path` resolves, then copies it to `data/bronze/<source>/dt={{ ds }}/`. Returns path + SHA-256 via XCom. |
| 2 | `bronze.load_<source>` | Deletes the `logical_date = {{ ds }}` partition in `bronze.raw_<source>` and inserts one JSONB row per record (`record_index`, `source_file`, `file_sha256`, `dag_run_id`). |
| 3 | `bronze.check_<source>` | `SQLTableCheckOperator`: partition row count > 0. |
| 4 | `silver.transform_<source>` | One SQL file per source, executed as **one transaction**: dedupe the batch (`DISTINCT ON … ORDER BY record_index DESC`), **upsert parents** on their natural key, **replace children** scoped to the parents in the batch, unnest arrays with `jsonb_array_elements … WITH ORDINALITY`, explode open key/value objects into an EAV table. |
| 5 | `silver.check_<source>` | `SQLColumnCheckOperator`: null / unique / non-negative checks on key columns. |
| 6 | `gold.build_<model>` | `TRUNCATE + INSERT … SELECT` from silver. Each model depends only on the silver lane(s) it reads. |
| 7 | `gold.check_*_reconciliation` | `SQLCheckOperator`: gold totals must reconcile to silver (`SUM(duration_minutes)` per user; `SUM(price*inventory)` and counts per category). |
| 8 | `record_load_audit` | Writes post-load row counts for every table to `ops.load_audit` (upsert on `dag_run_id, layer, table_name`). |

All SQL lives in `dags/json_medallion_pipeline/sql/**` and is referenced by relative path;
Airflow resolves it through `template_searchpath` + `template_ext`, so `{{ ds }}` is rendered
into every statement.

### Alternative trigger: file-arrival DAG

`json_medallion_pipeline_filesensor` has the same task graph but gates it on three
`FileSensor`s (`mode="reschedule"`, 30 s poke, 1 h timeout) watching the landing directory.
It is paused by default; unpause it instead of the scheduled DAG for *"run when the files
show up"* semantics. The file is a deliberate duplicate of the scheduled DAG (self-contained);
tests assert both expose the same `GOLD_DEPENDENCIES` and task wiring.

---

## Data model

### Sources

| Source | Shape | `record_path` |
|---|---|---|
| `products.json` | flat list `[ {id, name, category, price, quantity} ]` | `()` — root list |
| `users.json` | `{ users: [ { id, name, profile: { age, email, activities: [ {type, duration_minutes} ] } } ] }` | `("users",)` |
| `store.json` | `{ store: { name, location, products: [ { id, name, category, variations: [ { color/size \| model, specifications{…}, price, inventory } ] } ] } }` | `("store",)` — single object → one record |

### Silver (3NF)

```mermaid
erDiagram
    products {
        int product_id PK
        text product_name
        text category
        numeric price
        int quantity
    }

    users ||--o{ user_activities : has
    users {
        int user_id PK
        text user_name
        int age
        text email UK
    }
    user_activities {
        int user_id FK
        int activity_seq PK
        text activity_type
        int duration_minutes
    }

    stores ||--o{ store_products : sells
    store_products ||--o{ product_variations : has
    product_variations ||--o{ variation_specifications : describes
    stores {
        int store_id PK
        text store_name UK
        text location
    }
    store_products {
        text product_id PK
        int store_id FK
        text product_name
        text category
    }
    product_variations {
        text product_id FK
        int variation_seq PK
        text color
        text size
        text model
        numeric price
        int inventory
    }
    variation_specifications {
        text product_id FK
        int variation_seq FK
        text spec_name PK
        text spec_value
    }
```

Design notes:

- **Natural keys** are used where the source provides them (`product_id`, `user_id`,
  `store_name`). Arrays without a key (`activities`, `variations`) get a stable
  **ordinal** (`*_seq`) from `WITH ORDINALITY`, composite-keyed with the parent.
- **`specifications`** is an open bag of attributes (`ram`, `storage`, …), so it is stored as
  an **EAV child table** rather than one column per spec — new spec keys never need a
  migration. Fixed attributes (`color`, `size`, `model`) stay as nullable typed columns.
- Every silver row carries `logical_date` (the batch that last touched it); parents carry
  `updated_at`.
- `price` is `NUMERIC(12,2)`, never float.

### Gold

| Model | Grain | Answers |
|---|---|---|
| `gold.user_activity_summary` | one row per user | activity count, total minutes |
| `gold.product_total_value` | one row per product | `price * quantity` (the classic "total value" exercise) |
| `gold.product_variation_flat` | one row per variation | store ⨝ product ⨝ variation with specs pivoted back to columns (`ram`, `storage`) — the wide "BI-ready" shape |
| `gold.inventory_value_by_category` | one row per store × category | product count, variation count, total inventory, inventory value, avg price |

---

## Idempotency & reruns

Re-running any task, any run, or any date range yields the same warehouse state:

| Layer | Strategy |
|---|---|
| Bronze files | Copy overwrites `dt=<ds>/<file>`; SHA-256 is recorded so you can prove whether the file changed between runs. |
| Bronze tables | `DELETE WHERE logical_date = ds` then insert; `UNIQUE(logical_date, record_index)` guarantees no duplicates even if the delete were skipped. |
| Silver parents | `INSERT … ON CONFLICT (natural key) DO UPDATE` — last write wins, no duplicates. |
| Silver children | `DELETE … USING batch` then insert, scoped to parents in the batch — a parent whose children shrank keeps no stale rows; parents *not* in the batch are untouched. |
| Gold | `TRUNCATE + INSERT` in one transaction — readers never see a partial table. |
| Audit | Upsert on `(dag_run_id, layer, table_name)`. |

Each silver transform runs as a **single transaction** (`split_statements=False`,
`autocommit=False`, temp tables `ON COMMIT DROP`), so a failure mid-file rolls back cleanly
and Airflow's retry starts from a consistent state.

Try it: run `make trigger` twice, or `make backfill START=… END=…`, then compare
`ops.load_audit` — silver / gold counts do not grow; bronze gains one partition per date.

---

## Data-quality checks

Checks are first-class tasks (from `apache-airflow-providers-common-sql`), so a failed check
stops downstream layers and is visible in the UI:

| Where | Operator | Check |
|---|---|---|
| after bronze load | `SQLTableCheckOperator` | partition has > 0 rows |
| after each silver transform | `SQLColumnCheckOperator` | `product_id` / `email` not null, `email` unique, `price` / `quantity` / `age` / `inventory` ≥ 0 |
| after gold build | `SQLCheckOperator` | gold ↔ silver reconciliation (activity minutes; inventory value & counts per category) |
| schema level | Postgres | PK / FK / UNIQUE / CHECK constraints on every silver table |
| pre-load | Python | file exists, parses as JSON, `record_path` resolves, records are objects |

---

## Tests

```bash
make test               # all of the below, inside the Airflow image
make test-unit          # pure-Python + DAG-integrity tests (no warehouse needed)
make test-integration   # end-to-end SQL tests against the running warehouse
```

| File | Covers |
|---|---|
| `tests/test_ingest.py` | the helpers inside the DAG file: record extraction for flat / nested / single-object documents, error paths, landing path + checksum, overwrite on rerun, invalid-JSON and wrong-shape rejection, bronze row shaping |
| `tests/test_dag_integrity.py` | `DagBag` loads with no import errors and exposes exactly the two DAG ids; expected task ids, dependencies, roots / leaves; DAG settings (`catchup`, `max_active_runs`, retries, tags); FileSensor gating; every referenced `.sql` file exists and templates resolve at parse time |
| `tests/test_pipeline_integration.py` (`-m integration`) | runs DDL → bronze → silver → gold for a fixture date and asserts exact row counts and values (Alice 90 min / Bob 165 min, `Product_A` = 5000.00, Electronics inventory value 27 999.75, …), reconciliation checks return true, and a **second run leaves every count unchanged** |

Integration tests use the fixture date `2000-01-01` and clean up their bronze partition, so
they can run against a warehouse that already holds real data.

---

## Repository layout

```
.
├── dags/
│   └── json_medallion_pipeline/               # self-contained DAGs + their SQL
│       ├── json_medallion_pipeline.py             # scheduled DAG (every task explicit)
│       ├── json_medallion_pipeline_filesensor.py  # file-arrival variant (duplicate, + sensors)
│       └── sql/
│           ├── bronze/ddl.sql
│           ├── silver/ddl.sql, products.sql, users.sql, store_catalog.sql
│           ├── gold/ddl.sql, user_activity_summary.sql, product_total_value.sql,
│           │          product_variation_flat.sql, inventory_value_by_category.sql
│           ├── ops/ddl.sql
│           └── checks/gold_activity_reconciliation.sql, gold_catalog_reconciliation.sql
├── data/
│   ├── samples/              # the three JSON documents from the prompt
│   ├── landing/              # drop zone (contents git-ignored)
│   └── bronze/               # dated raw copies (contents git-ignored)
├── tests/                    # ingest helpers, DAG integrity, end-to-end SQL (integration)
├── docker-compose.yml        # airflow-metadb, warehouse, airflow-init, scheduler, webserver, airflow-cli
├── Dockerfile                # apache/airflow:2.11.2 + requirements.txt
├── requirements.txt
├── Makefile
├── pytest.ini
├── LICENSE
├── .gitignore
└── .env.example
```

---

## Configuration

`make up` copies `.env.example` → `.env` if missing:

| Variable | Default | Purpose |
|---|---|---|
| `AIRFLOW_PORT` | `8080` | host port for the Airflow UI |
| `WAREHOUSE_PORT` | `5433` | host port for the warehouse Postgres |
| `WAREHOUSE_USER` / `WAREHOUSE_PASSWORD` / `WAREHOUSE_DB` | `warehouse` | warehouse credentials |
| `AIRFLOW_UID` | `50000` | uid that owns bind-mounted `logs/` and `data/` |
| `_AIRFLOW_WWW_USER_USERNAME` / `_PASSWORD` | `airflow` | UI login |

Airflow connections are injected as environment variables (reproducible, no UI clicking):

- `AIRFLOW_CONN_WAREHOUSE_POSTGRES` → used by every SQL / check operator
- `AIRFLOW_CONN_FS_LANDING` → used by the FileSensor variant

---

## Adding / removing a source

Nothing is generated, so a new source is explicit in both DAG files. To add one:

1. Drop `newsource.json` into `data/landing/` (and `data/samples/` if `make seed` should copy it).
2. Add `bronze.raw_newsource` to `dags/json_medallion_pipeline/sql/bronze/ddl.sql`, its silver
   tables to `sql/silver/ddl.sql`, and write `sql/silver/newsource.sql`.
3. In **both** `json_medallion_pipeline.py` and `json_medallion_pipeline_filesensor.py`, add the
   `land_newsource` / `load_newsource` / `check_newsource` tasks (with the `record_path` down to
   the list or object of records; `()` for a root list), the `transform_newsource` /
   `silver.check_newsource` tasks, their wiring, and the source's entry in the audit table list.
4. If a gold model reads it, add the dependency to `GOLD_DEPENDENCIES` (and extend the gold SQL).

Removing a source is the reverse.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `Bind for 127.0.0.1:8080/5433 failed: port is already allocated` | Another project owns the port. Change `AIRFLOW_PORT` / `WAREHOUSE_PORT` in `.env` and rerun `make up`. |
| `Cannot connect to the Docker daemon` | Start Rancher Desktop / Docker Desktop; on Rancher check `docker context ls` shows `rancher-desktop *`. |
| `bronze.land_*` fails with `FileNotFoundError` | Landing dir is empty — `make seed` (or drop your own files into `data/landing/`). |
| Permission errors writing `logs/` or `data/` on Linux | Set `AIRFLOW_UID=$(id -u)` in `.env`, then `make down && make up`. |
| Want a clean slate | `make reset` (drops both Postgres volumes). |
