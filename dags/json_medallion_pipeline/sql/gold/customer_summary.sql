-- gold.customer_summary: order count, units, revenue and first/last order per customer.
-- Grain/key: (release_id, customer_id). Uses the CURRENT customer version for the name. Parked items
-- (no CURRENT product) are excluded, so a customer with only parked items has no row.
-- Rerun: delete + insert for THIS release_id only.
-- Lineage: bronze_lineage = distinct order records that contributed.
{% set release_id = ti.xcom_pull(task_ids='create_release') %}
DELETE FROM gold.customer_summary WHERE release_id = {{ release_id }};

INSERT INTO gold.customer_summary
    (release_id, customer_id, customer_name, order_count, units, revenue,
     first_order_at, last_order_at, last_ship_state, bronze_lineage)
SELECT {{ release_id }},
       o.customer_id,
       c.name,
       COUNT(DISTINCT o.order_id),
       SUM(i.quantity),
       SUM(i.line_amount),
       MIN(o.ordered_at),
       MAX(o.ordered_at),
       -- state of the most recent order (ties broken by order_id so reruns are deterministic)
       (array_agg(o.ship_state ORDER BY o.ordered_at DESC, o.order_id DESC))[1],
       jsonb_agg(DISTINCT jsonb_build_object('arrival_id', i.bronze_arrival_id, 'record_index', i.bronze_record_index))
FROM silver.order_items i
JOIN silver.products p  ON p.product_id = i.product_id AND p.is_current
JOIN silver.orders o    ON o.order_id = i.order_id
JOIN silver.customers c ON c.customer_id = o.customer_id AND c.is_current
GROUP BY o.customer_id, c.name;
