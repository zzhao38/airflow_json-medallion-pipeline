-- Bronze: immutable, replayable, traceable record of everything that arrived.
-- APPEND-ONLY BY CONVENTION: the DAG only ever INSERTs into these tables (never UPDATE/DELETE);
-- a corrected file is a NEW arrival, and silver decides which arrival is current.
-- Grain: bronze.arrival = one row per delivery attempt (accepted, quarantined or duplicate);
--        bronze.record  = one row per source record of an ACCEPTED arrival.
-- Rerun: re-delivering identical bytes for the same (source, logical_date) becomes a DUPLICATE
-- arrival that points at the original and adds no records.
-- Lineage: (arrival_id, record_index) is the key silver and gold carry back to the raw payload.
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.arrival (
    arrival_id        BIGSERIAL    PRIMARY KEY,
    source            TEXT         NOT NULL,
    logical_date      DATE         NOT NULL,
    dag_run_id        TEXT         NOT NULL,
    source_file       TEXT         NOT NULL,
    stored_path       TEXT         NOT NULL,            -- bronze or quarantine copy
    file_sha256       TEXT         NOT NULL,
    file_bytes        BIGINT       NOT NULL,
    schema_version    TEXT         NOT NULL,            -- e.g. products.v1
    batch_id          TEXT,                             -- envelope batch_id; NULL when unparseable
    envelope_source   TEXT,                             -- envelope "source" as sent; NULL when unparseable
    record_count      INTEGER,                          -- NULL when the file could not be parsed
    status            TEXT         NOT NULL CHECK (status IN ('ACCEPTED', 'QUARANTINED', 'DUPLICATE')),
    quarantine_reason TEXT,
    duplicate_of      BIGINT       REFERENCES bronze.arrival (arrival_id),
    arrived_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CHECK ((status = 'QUARANTINED') = (quarantine_reason IS NOT NULL)),
    CHECK ((status = 'DUPLICATE') = (duplicate_of IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS arrival_source_date_status ON bronze.arrival (source, logical_date, status);

CREATE TABLE IF NOT EXISTS bronze.record (
    arrival_id    BIGINT   NOT NULL REFERENCES bronze.arrival (arrival_id),
    record_index  INTEGER  NOT NULL,
    payload       JSONB    NOT NULL,
    PRIMARY KEY (arrival_id, record_index)
);
CREATE INDEX IF NOT EXISTS record_payload_gin ON bronze.record USING GIN (payload);

-- The arrival silver must read for each (source, logical_date): the latest delivery that was not
-- quarantined. A DUPLICATE resolves to the arrival it duplicates, so a re-delivery reads the same
-- records. delivered_at is when that latest delivery happened (used as the SCD-2 valid_from).
CREATE OR REPLACE VIEW bronze.v_latest_accepted AS
SELECT DISTINCT ON (source, logical_date)
       source,
       logical_date,
       COALESCE(duplicate_of, arrival_id) AS arrival_id,
       arrived_at                         AS delivered_at
FROM bronze.arrival
WHERE status IN ('ACCEPTED', 'DUPLICATE')
ORDER BY source, logical_date, arrival_id DESC;
