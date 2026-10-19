-- Operational metadata: gold releases, the active-release pointer per destination, quality issues.
-- Grain: ops.release = one immutable gold build; ops.destination = one row per consumer
-- destination; ops.quality_issue = one row per WARN/ERROR raised by the pipeline.
-- Rerun: a rerun creates a NEW release (<ds>.<n>); a published release is never rewritten.
-- Lineage: ops.release.input_arrivals records which bronze arrivals fed the release.
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.release (
    release_id       BIGSERIAL    PRIMARY KEY,
    release_version  TEXT         NOT NULL UNIQUE,      -- <logical_date>.<n>
    logical_date     DATE         NOT NULL,
    dag_run_id       TEXT         NOT NULL,
    status           TEXT         NOT NULL CHECK (status IN ('BUILDING', 'PUBLISHED', 'HELD')),
    input_arrivals   JSONB,                             -- {source: arrival_id}
    row_counts       JSONB,                             -- {gold table: rows}, set at publish/hold
    hold_reason      TEXT,
    built_at         TIMESTAMPTZ  NOT NULL DEFAULT now(),
    released_at      TIMESTAMPTZ                        -- set when PUBLISHED
);

-- Consumers read through views that join this pointer; flipping it is the whole "publish".
CREATE TABLE IF NOT EXISTS ops.destination (
    destination        TEXT         PRIMARY KEY,
    active_release_id  BIGINT       REFERENCES ops.release (release_id),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);
INSERT INTO ops.destination (destination) VALUES ('analytics'), ('finance')
ON CONFLICT (destination) DO NOTHING;

CREATE TABLE IF NOT EXISTS ops.quality_issue (
    issue_id         BIGSERIAL    PRIMARY KEY,
    dag_run_id       TEXT         NOT NULL,
    logical_date     DATE         NOT NULL,
    layer            TEXT         NOT NULL CHECK (layer IN ('bronze', 'silver', 'gold')),
    source_or_model  TEXT         NOT NULL,
    severity         TEXT         NOT NULL CHECK (severity IN ('WARN', 'ERROR')),
    reason           TEXT         NOT NULL,
    arrival_id       BIGINT       REFERENCES bronze.arrival (arrival_id),
    release_id       BIGINT       REFERENCES ops.release (release_id),
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);
