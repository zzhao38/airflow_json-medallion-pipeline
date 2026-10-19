-- gold.daily_sales_by_category: revenue, units and order count per order date and category.
-- Grain/key: (release_id, order_date, category_id). Parked items (no CURRENT product) are excluded, so
-- the total reconciles with silver.order_items minus silver.v_parked_order_items.
-- Rerun: delete + insert for THIS release_id only.
-- Lineage: bronze_lineage = distinct {arrival_id, record_index} of the order records that contributed.
{% set release_id = ti.xcom_pull(task_ids='create_release') %}
DELETE FROM gold.daily_sales_by_category WHERE release_id = {{ release_id }};

INSERT INTO gold.daily_sales_by_category
    (release_id, order_date, category_id, category_level_1, order_count, units, revenue, bronze_lineage)
SELECT {{ release_id }},
       o.order_date,
       p.category_id,
       MIN(cat.level_1),
       COUNT(DISTINCT o.order_id),
       SUM(i.quantity),
       SUM(i.line_amount),
       jsonb_agg(DISTINCT jsonb_build_object('arrival_id', i.bronze_arrival_id, 'record_index', i.bronze_record_index))
FROM silver.order_items i
JOIN silver.orders o ON o.order_id = i.order_id
JOIN silver.products p     ON p.product_id = i.product_id AND p.is_current
JOIN silver.categories cat ON cat.category_id = p.category_id
GROUP BY o.order_date, p.category_id;
