-- gold.order_lines: one row per silver order item, enriched with the customer version valid at ordered_at,
-- plus the current product and category.
-- Grain/key: (release_id, order_item_id). Reads silver only (never another gold model), so the four
-- gold builds can run in parallel and the reconciliation check stays independent of them.
-- Rerun: delete + insert for THIS release_id only; other releases are never touched.
-- Lineage: the order record behind the item (silver.order_items.bronze_*).
-- Parked items (product has no CURRENT silver.products row) are excluded: every gold model inner-joins
-- the current product, so they never reach consumers (see silver.v_parked_order_items).
{% set release_id = ti.xcom_pull(task_ids='create_release') %}
DELETE FROM gold.order_lines WHERE release_id = {{ release_id }};

INSERT INTO gold.order_lines
    (release_id, order_item_id, order_id, order_date, ordered_at, customer_id, customer_name,
     ship_state, ship_method, product_id, sku, product_name, brand, category_id, category_level_1,
     quantity, unit_price, line_revenue, bronze_arrival_id, bronze_record_index)
SELECT {{ release_id }},
       i.order_item_id, i.order_id, o.order_date, o.ordered_at,
       o.customer_id, c.name,
       o.ship_state, o.ship_method,
       i.product_id,
       p.sku, p.name, p.brand,
       p.category_id, cat.level_1,
       i.quantity, i.unit_price, i.line_amount,
       i.bronze_arrival_id, i.bronze_record_index
FROM silver.order_items i
JOIN silver.orders o     ON o.order_id = i.order_id
JOIN silver.customers c  ON c.customer_id = o.customer_id
                          AND c.valid_from <= o.ordered_at
                          AND (c.valid_to IS NULL OR o.ordered_at < c.valid_to)
JOIN silver.products p    ON p.product_id = i.product_id AND p.is_current
JOIN silver.categories cat ON cat.category_id = p.category_id;
