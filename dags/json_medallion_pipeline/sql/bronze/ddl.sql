-- Bronze: raw, replayable copy of each source file. One table per source, schema-on-read via JSONB.
-- Grain: one row per source record, keyed (logical_date, record_index).
-- Rerun: each logical_date partition is replaced (delete + insert in one transaction).
-- Lineage: file name, sha256 and dag_run_id are stored with every row; silver/gold point back here.
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
