-- Consumer-facing views. Consumers query analytics.* / finance.*, never gold.* directly.
-- Each view returns only the destination's ACTIVE release (ops.destination.active_release_id),
-- so a failed or held build is invisible and publishing is a single pointer flip.
-- Every row carries release_version + released_at, so consumers can see which release they read
-- and how old it is (a HELD run keeps serving the previous release).
-- Grain: same as the underlying gold table, minus release_id / lineage columns.
-- Before the first publish active_release_id is NULL, so the views return no rows.
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS finance;

CREATE OR REPLACE VIEW analytics.order_lines AS
SELECT g.order_item_id, g.order_id, g.order_date, g.ordered_at, g.customer_id, g.customer_name,
       g.ship_state, g.ship_method, g.product_id, g.sku, g.product_name, g.brand,
       g.category_id, g.category_level_1, g.quantity, g.unit_price, g.line_revenue,
       r.release_version, r.released_at
FROM gold.order_lines g
JOIN ops.destination d ON d.destination = 'analytics' AND g.release_id = d.active_release_id
JOIN ops.release r ON r.release_id = d.active_release_id;

CREATE OR REPLACE VIEW analytics.daily_sales_by_category AS
SELECT g.order_date, g.category_id, g.category_level_1, g.order_count, g.units, g.revenue,
       r.release_version, r.released_at
FROM gold.daily_sales_by_category g
JOIN ops.destination d ON d.destination = 'analytics' AND g.release_id = d.active_release_id
JOIN ops.release r ON r.release_id = d.active_release_id;

CREATE OR REPLACE VIEW analytics.customer_summary AS
SELECT g.customer_id, g.customer_name, g.order_count, g.units, g.revenue,
       g.first_order_at, g.last_order_at, g.last_ship_state,
       r.release_version, r.released_at
FROM gold.customer_summary g
JOIN ops.destination d ON d.destination = 'analytics' AND g.release_id = d.active_release_id
JOIN ops.release r ON r.release_id = d.active_release_id;

CREATE OR REPLACE VIEW analytics.product_performance AS
SELECT g.product_id, g.sku, g.product_name, g.brand, g.category_id, g.units, g.revenue,
       g.catalog_price_usd, g.avg_order_unit_price, g.price_variance,
       r.release_version, r.released_at
FROM gold.product_performance g
JOIN ops.destination d ON d.destination = 'analytics' AND g.release_id = d.active_release_id
JOIN ops.release r ON r.release_id = d.active_release_id;

CREATE OR REPLACE VIEW finance.daily_sales_by_category AS
SELECT g.order_date, g.category_id, g.category_level_1, g.order_count, g.units, g.revenue,
       r.release_version, r.released_at
FROM gold.daily_sales_by_category g
JOIN ops.destination d ON d.destination = 'finance' AND g.release_id = d.active_release_id
JOIN ops.release r ON r.release_id = d.active_release_id;

CREATE OR REPLACE VIEW finance.product_performance AS
SELECT g.product_id, g.sku, g.product_name, g.brand, g.category_id, g.units, g.revenue,
       g.catalog_price_usd, g.avg_order_unit_price, g.price_variance,
       r.release_version, r.released_at
FROM gold.product_performance g
JOIN ops.destination d ON d.destination = 'finance' AND g.release_id = d.active_release_id
JOIN ops.release r ON r.release_id = d.active_release_id;
