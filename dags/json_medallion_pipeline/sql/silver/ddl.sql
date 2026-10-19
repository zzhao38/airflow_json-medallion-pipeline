-- Silver: typed, normalized entities. Parents are SCD-2 (full history); children are current-state.
-- Grain: one row per VERSION of a parent (surrogate key <entity>_sk); children and facts are keyed
--        by their natural key and describe the current state only.
-- Rerun: a parent only gets a new version when row_hash changes, so replaying the same arrival
--        changes nothing. Closing a version sets valid_to + is_current = false (never deleted).
-- Lineage: every row carries (bronze_arrival_id, bronze_record_index) -> bronze.record.
CREATE SCHEMA IF NOT EXISTS silver;

-- ---------- orders.json (order -> customer, shipping, items[]) ----------
CREATE TABLE IF NOT EXISTS silver.customers (
    customer_sk         BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id         TEXT        NOT NULL,
    name                TEXT        NOT NULL,
    row_hash            TEXT        NOT NULL,           -- md5 of the business attributes
    valid_from          TIMESTAMPTZ NOT NULL,
    valid_to            TIMESTAMPTZ,
    is_current          BOOLEAN     NOT NULL DEFAULT TRUE,
    bronze_arrival_id   BIGINT      NOT NULL,
    bronze_record_index INTEGER     NOT NULL,
    CHECK (is_current = (valid_to IS NULL)),
    FOREIGN KEY (bronze_arrival_id, bronze_record_index) REFERENCES bronze.record
);
CREATE UNIQUE INDEX IF NOT EXISTS customers_one_current ON silver.customers (customer_id) WHERE is_current;

-- Facts: one row per valid order (upsert on order_id). Rejected orders never land here.
CREATE TABLE IF NOT EXISTS silver.orders (
    order_id            TEXT        PRIMARY KEY,
    customer_id         TEXT        NOT NULL,
    ordered_at          TIMESTAMPTZ NOT NULL,
    order_date          DATE        NOT NULL,           -- UTC date of ordered_at
    ship_city           TEXT        NOT NULL,
    ship_state          TEXT        NOT NULL,
    ship_method         TEXT        NOT NULL,
    bronze_arrival_id   BIGINT      NOT NULL,
    bronze_record_index INTEGER     NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (bronze_arrival_id, bronze_record_index) REFERENCES bronze.record
);

-- Items are replaced per order in the batch (they have a natural key, but a re-sent order may
-- drop or add items). line_amount is derived, so it can never disagree with its inputs.
CREATE TABLE IF NOT EXISTS silver.order_items (
    order_item_id       TEXT           PRIMARY KEY,
    order_id            TEXT           NOT NULL REFERENCES silver.orders (order_id),
    product_id          TEXT           NOT NULL,
    quantity            INTEGER        NOT NULL CHECK (quantity > 0),
    unit_price          NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    line_amount         NUMERIC(12, 2) GENERATED ALWAYS AS (quantity * unit_price) STORED,
    bronze_arrival_id   BIGINT         NOT NULL,
    bronze_record_index INTEGER        NOT NULL,
    FOREIGN KEY (bronze_arrival_id, bronze_record_index) REFERENCES bronze.record
);
CREATE INDEX IF NOT EXISTS order_items_order ON silver.order_items (order_id);

-- Record-level quarantine: a business-rule violation rejects ONE record (the whole order), not
-- the file. Insert-only; the raw payload is kept so the reject can be reviewed and replayed.
CREATE TABLE IF NOT EXISTS silver.rejected_record (
    arrival_id    BIGINT      NOT NULL,
    record_index  INTEGER     NOT NULL,
    source        TEXT        NOT NULL,
    entity_key    TEXT        NOT NULL,                 -- e.g. the order_id
    reason        TEXT        NOT NULL,
    payload       JSONB       NOT NULL,
    rejected_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (arrival_id, record_index),             -- = UNIQUE (arrival_id, record_index)
    FOREIGN KEY (arrival_id, record_index) REFERENCES bronze.record
);

-- ---------- products.json (product -> details, category, prices[]) ----------
-- row_hash also covers the normalized prices array, so a price change versions the product and
-- the price rows always inherit the lineage of the product version they belong to.
CREATE TABLE IF NOT EXISTS silver.products (
    product_sk          BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id          TEXT        NOT NULL,
    sku                 TEXT        NOT NULL,
    name                TEXT        NOT NULL,
    brand               TEXT        NOT NULL,
    category_id         TEXT        NOT NULL,
    row_hash            TEXT        NOT NULL,
    valid_from          TIMESTAMPTZ NOT NULL,
    valid_to            TIMESTAMPTZ,
    is_current          BOOLEAN     NOT NULL DEFAULT TRUE,
    bronze_arrival_id   BIGINT      NOT NULL,
    bronze_record_index INTEGER     NOT NULL,
    CHECK (is_current = (valid_to IS NULL)),
    FOREIGN KEY (bronze_arrival_id, bronze_record_index) REFERENCES bronze.record
);
CREATE UNIQUE INDEX IF NOT EXISTS products_one_current ON silver.products (product_id) WHERE is_current;

-- Category tree flattened to a path array + the first two levels; current-state (upserted).
CREATE TABLE IF NOT EXISTS silver.categories (
    category_id         TEXT    PRIMARY KEY,
    path                TEXT[]  NOT NULL,
    level_1             TEXT    NOT NULL,
    level_2             TEXT,
    bronze_arrival_id   BIGINT  NOT NULL,
    bronze_record_index INTEGER NOT NULL,
    FOREIGN KEY (bronze_arrival_id, bronze_record_index) REFERENCES bronze.record
);

-- Prices of the product's current version; no natural key, so price_seq = array ordinality.
CREATE TABLE IF NOT EXISTS silver.product_prices (
    product_id          TEXT           NOT NULL,
    price_seq           INTEGER        NOT NULL,
    currency            TEXT           NOT NULL,
    amount              NUMERIC(12, 2) NOT NULL CHECK (amount >= 0),
    is_current          BOOLEAN        NOT NULL,
    bronze_arrival_id   BIGINT         NOT NULL,
    bronze_record_index INTEGER        NOT NULL,
    PRIMARY KEY (product_id, price_seq),
    FOREIGN KEY (bronze_arrival_id, bronze_record_index) REFERENCES bronze.record
);

-- PARKED order items: valid items whose product has no CURRENT silver.products row (unknown, or
-- rejected as a product). They stay in silver, are excluded from every gold model, and are
-- released automatically once the product arrives (gold joins products at build time).
CREATE OR REPLACE VIEW silver.v_parked_order_items AS
SELECT i.order_item_id, i.order_id, i.product_id, i.quantity, i.unit_price, i.line_amount,
       o.order_date, i.bronze_arrival_id, i.bronze_record_index
FROM silver.order_items i
JOIN silver.orders o ON o.order_id = i.order_id
WHERE NOT EXISTS (SELECT 1 FROM silver.products p WHERE p.product_id = i.product_id AND p.is_current);
