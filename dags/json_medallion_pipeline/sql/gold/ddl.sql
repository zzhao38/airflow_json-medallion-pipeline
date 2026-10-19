-- Gold: consumer-ready models, built as IMMUTABLE RELEASES.
-- Grain/key: every table is keyed (release_id, <model key>); one release = one full build.
-- Rerun: a build deletes + re-inserts only its own release_id (never TRUNCATE), so published
--        releases are never touched. Consumers do not read these tables directly: the analytics /
--        finance views join ops.destination to the active release (see consumer_views.sql).
-- Lineage: per-row models carry (bronze_arrival_id, bronze_record_index); aggregates carry
--          bronze_lineage, a JSONB array of {arrival_id, record_index}. See trace_views.sql.
CREATE SCHEMA IF NOT EXISTS gold;

-- One row per order item, enriched with customer, product and category. Parked items (product has
-- no CURRENT silver.products row) are not in gold at all.
CREATE TABLE IF NOT EXISTS gold.order_lines (
    release_id          BIGINT         NOT NULL REFERENCES ops.release (release_id),
    order_item_id       TEXT           NOT NULL,
    order_id            TEXT           NOT NULL,
    order_date          DATE           NOT NULL,
    ordered_at          TIMESTAMPTZ    NOT NULL,
    customer_id         TEXT           NOT NULL,
    customer_name       TEXT           NOT NULL,
    ship_state          TEXT           NOT NULL,
    ship_method         TEXT           NOT NULL,
    product_id          TEXT           NOT NULL,
    sku                 TEXT           NOT NULL,
    product_name        TEXT           NOT NULL,
    brand               TEXT           NOT NULL,
    category_id         TEXT           NOT NULL,
    category_level_1    TEXT           NOT NULL,
    quantity            INTEGER        NOT NULL,
    unit_price          NUMERIC(12, 2) NOT NULL,
    line_revenue        NUMERIC(12, 2) NOT NULL,
    bronze_arrival_id   BIGINT         NOT NULL,
    bronze_record_index INTEGER        NOT NULL,
    PRIMARY KEY (release_id, order_item_id)
);

CREATE TABLE IF NOT EXISTS gold.daily_sales_by_category (
    release_id       BIGINT         NOT NULL REFERENCES ops.release (release_id),
    order_date       DATE           NOT NULL,
    category_id      TEXT           NOT NULL,
    category_level_1 TEXT           NOT NULL,
    order_count      INTEGER        NOT NULL,
    units            BIGINT         NOT NULL,
    revenue          NUMERIC(14, 2) NOT NULL,
    bronze_lineage   JSONB          NOT NULL,
    PRIMARY KEY (release_id, order_date, category_id)
);

CREATE TABLE IF NOT EXISTS gold.customer_summary (
    release_id      BIGINT         NOT NULL REFERENCES ops.release (release_id),
    customer_id     TEXT           NOT NULL,
    customer_name   TEXT           NOT NULL,
    order_count     INTEGER        NOT NULL,
    units           BIGINT         NOT NULL,
    revenue         NUMERIC(14, 2) NOT NULL,
    first_order_at  TIMESTAMPTZ    NOT NULL,
    last_order_at   TIMESTAMPTZ    NOT NULL,
    last_ship_state TEXT           NOT NULL,
    bronze_lineage  JSONB          NOT NULL,
    PRIMARY KEY (release_id, customer_id)
);

-- One row per CURRENT catalog product (products with no sales show 0 / NULL). price_variance is
-- avg_order_unit_price - catalog_price_usd; both NULL-able, so the variance is too.
CREATE TABLE IF NOT EXISTS gold.product_performance (
    release_id           BIGINT         NOT NULL REFERENCES ops.release (release_id),
    product_id           TEXT           NOT NULL,
    sku                  TEXT           NOT NULL,
    product_name         TEXT           NOT NULL,
    brand                TEXT           NOT NULL,
    category_id          TEXT           NOT NULL,
    units                BIGINT         NOT NULL,
    revenue              NUMERIC(14, 2) NOT NULL,
    catalog_price_usd    NUMERIC(12, 2),
    avg_order_unit_price NUMERIC(12, 2),
    price_variance       NUMERIC(12, 2),
    bronze_lineage       JSONB          NOT NULL,
    PRIMARY KEY (release_id, product_id)
);
