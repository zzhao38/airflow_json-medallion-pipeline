-- Silver: typed, deduplicated, normalized (3NF) entities with keys and constraints.
-- Parents are upserted on their natural key; child rows are replaced per parent.
CREATE SCHEMA IF NOT EXISTS silver;

-- ---------- products.json (flat list) ----------
CREATE TABLE IF NOT EXISTS silver.products (
    product_id    INTEGER        PRIMARY KEY,
    product_name  TEXT           NOT NULL,
    category      TEXT           NOT NULL,
    price         NUMERIC(12, 2) NOT NULL CHECK (price >= 0),
    quantity      INTEGER        NOT NULL CHECK (quantity >= 0),
    logical_date  DATE           NOT NULL,
    updated_at    TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- ---------- users.json (users -> profile -> activities[]) ----------
CREATE TABLE IF NOT EXISTS silver.users (
    user_id       INTEGER     PRIMARY KEY,
    user_name     TEXT        NOT NULL,
    age           INTEGER     CHECK (age >= 0),
    email         TEXT        NOT NULL UNIQUE,
    logical_date  DATE        NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.user_activities (
    user_id          INTEGER NOT NULL REFERENCES silver.users (user_id),
    activity_seq     INTEGER NOT NULL,            -- array ordinality; activities have no natural key
    activity_type    TEXT    NOT NULL,
    duration_minutes INTEGER NOT NULL CHECK (duration_minutes >= 0),
    logical_date     DATE    NOT NULL,
    PRIMARY KEY (user_id, activity_seq)
);

-- ---------- store.json (store -> products[] -> variations[] -> specifications{}) ----------
CREATE TABLE IF NOT EXISTS silver.stores (
    store_id      INTEGER     GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_name    TEXT        NOT NULL UNIQUE,
    location      TEXT,
    logical_date  DATE        NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.store_products (
    product_id    TEXT        PRIMARY KEY,         -- source id e.g. "p1"
    store_id      INTEGER     NOT NULL REFERENCES silver.stores (store_id),
    product_name  TEXT        NOT NULL,
    category      TEXT        NOT NULL,
    logical_date  DATE        NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.product_variations (
    product_id    TEXT           NOT NULL REFERENCES silver.store_products (product_id),
    variation_seq INTEGER        NOT NULL,
    color         TEXT,
    size          TEXT,
    model         TEXT,
    price         NUMERIC(12, 2) NOT NULL CHECK (price >= 0),
    inventory     INTEGER        NOT NULL CHECK (inventory >= 0),
    logical_date  DATE           NOT NULL,
    PRIMARY KEY (product_id, variation_seq)
);

-- Specifications are an open key/value set (ram, storage, ... ) -> EAV child table
-- rather than a column per spec, so new spec keys never require a schema change.
CREATE TABLE IF NOT EXISTS silver.variation_specifications (
    product_id    TEXT    NOT NULL,
    variation_seq INTEGER NOT NULL,
    spec_name     TEXT    NOT NULL,
    spec_value    TEXT    NOT NULL,
    logical_date  DATE    NOT NULL,
    PRIMARY KEY (product_id, variation_seq, spec_name),
    FOREIGN KEY (product_id, variation_seq)
        REFERENCES silver.product_variations (product_id, variation_seq)
);
