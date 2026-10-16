-- Gold: consumer-ready data products. Small aggregates -> full refresh each run
-- (TRUNCATE + INSERT inside one transaction, so readers never see a half-built table).
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.user_activity_summary (
    user_id                 INTEGER     PRIMARY KEY,
    name                    TEXT        NOT NULL,
    email                   TEXT        NOT NULL,
    activity_count          INTEGER     NOT NULL,
    total_activity_duration INTEGER     NOT NULL,
    refreshed_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS gold.product_variation_flat (
    store_name                       TEXT           NOT NULL,
    store_location                   TEXT,
    product_id                       TEXT           NOT NULL,
    product_name                     TEXT           NOT NULL,
    product_category                 TEXT           NOT NULL,
    variation_seq                    INTEGER        NOT NULL,
    variation_color                  TEXT,
    variation_size                   TEXT,
    variation_model                  TEXT,
    variation_specifications_ram     TEXT,
    variation_specifications_storage TEXT,
    variation_price                  NUMERIC(12, 2) NOT NULL,
    variation_inventory              INTEGER        NOT NULL,
    refreshed_at                     TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (product_id, variation_seq)
);

CREATE TABLE IF NOT EXISTS gold.inventory_value_by_category (
    store_name       TEXT           NOT NULL,
    product_category TEXT           NOT NULL,
    product_count    INTEGER        NOT NULL,
    variation_count  INTEGER        NOT NULL,
    total_inventory  BIGINT         NOT NULL,
    inventory_value  NUMERIC(14, 2) NOT NULL,
    avg_price        NUMERIC(12, 2) NOT NULL,
    refreshed_at     TIMESTAMPTZ    NOT NULL DEFAULT now(),
    PRIMARY KEY (store_name, product_category)
);

CREATE TABLE IF NOT EXISTS gold.product_total_value (
    product_id   INTEGER        PRIMARY KEY,
    name         TEXT           NOT NULL,
    category     TEXT           NOT NULL,
    total_value  NUMERIC(14, 2) NOT NULL,
    refreshed_at TIMESTAMPTZ    NOT NULL DEFAULT now()
);
