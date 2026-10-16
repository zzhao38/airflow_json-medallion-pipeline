-- Operational metadata: one row per (run, layer, table) with the row count after load.
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.load_audit (
    audit_id     BIGSERIAL   PRIMARY KEY,
    dag_run_id   TEXT        NOT NULL,
    logical_date DATE        NOT NULL,
    layer        TEXT        NOT NULL CHECK (layer IN ('bronze', 'silver', 'gold')),
    table_name   TEXT        NOT NULL,
    row_count    BIGINT      NOT NULL,
    loaded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (dag_run_id, layer, table_name)
);
