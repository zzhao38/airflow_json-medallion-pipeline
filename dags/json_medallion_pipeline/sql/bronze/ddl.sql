-- Bronze: immutable raw landing. One table per source, schema-on-read via JSONB.
-- (logical_date, record_index) is the idempotency key: a rerun of a partition
-- deletes + reinserts it, so bronze always reflects the latest file for that day.
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.raw_products (
    raw_id        BIGSERIAL    PRIMARY KEY,
    logical_date  DATE         NOT NULL,
    record_index  INTEGER      NOT NULL,
    payload       JSONB        NOT NULL,
    source_file   TEXT         NOT NULL,
    file_sha256   TEXT         NOT NULL,
    dag_run_id    TEXT         NOT NULL,
    ingested_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (logical_date, record_index)
);

CREATE TABLE IF NOT EXISTS bronze.raw_users (
    raw_id        BIGSERIAL    PRIMARY KEY,
    logical_date  DATE         NOT NULL,
    record_index  INTEGER      NOT NULL,
    payload       JSONB        NOT NULL,
    source_file   TEXT         NOT NULL,
    file_sha256   TEXT         NOT NULL,
    dag_run_id    TEXT         NOT NULL,
    ingested_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (logical_date, record_index)
);

CREATE TABLE IF NOT EXISTS bronze.raw_store_catalog (
    raw_id        BIGSERIAL    PRIMARY KEY,
    logical_date  DATE         NOT NULL,
    record_index  INTEGER      NOT NULL,
    payload       JSONB        NOT NULL,
    source_file   TEXT         NOT NULL,
    file_sha256   TEXT         NOT NULL,
    dag_run_id    TEXT         NOT NULL,
    ingested_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (logical_date, record_index)
);

CREATE INDEX IF NOT EXISTS raw_products_payload_gin      ON bronze.raw_products      USING GIN (payload);
CREATE INDEX IF NOT EXISTS raw_users_payload_gin         ON bronze.raw_users         USING GIN (payload);
CREATE INDEX IF NOT EXISTS raw_store_catalog_payload_gin ON bronze.raw_store_catalog USING GIN (payload);
