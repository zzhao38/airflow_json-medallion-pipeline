-- gold.product_performance: units, revenue and price variance for every CURRENT catalog product.
-- Grain/key: (release_id, product_id). Products with no sales appear with units = 0, revenue = 0.
-- price_variance = avg unit price actually charged - current USD catalog price (NULL if either is missing).
-- Rerun: delete + insert for THIS release_id only.
-- Lineage: the product's own catalog record + the order records that sold it.
{% set release_id = ti.xcom_pull(task_ids='create_release') %}
DELETE FROM gold.product_performance WHERE release_id = {{ release_id }};

WITH sales AS (
    SELECT i.product_id,
           SUM(i.quantity)    AS units,
           SUM(i.line_amount) AS revenue
    FROM silver.order_items i
    JOIN silver.products sp ON sp.product_id = i.product_id AND sp.is_current  -- parked items excluded
    GROUP BY i.product_id
),
lineage AS (
    SELECT p.product_id, p.bronze_arrival_id AS arrival_id, p.bronze_record_index AS record_index
    FROM silver.products p
    WHERE p.is_current
    UNION
    SELECT i.product_id, i.bronze_arrival_id, i.bronze_record_index
    FROM silver.order_items i
    JOIN silver.products sp ON sp.product_id = i.product_id AND sp.is_current
),
lineage_json AS (
    SELECT product_id,
           jsonb_agg(jsonb_build_object('arrival_id', arrival_id, 'record_index', record_index)
                     ORDER BY arrival_id, record_index) AS bronze_lineage
    FROM lineage
    GROUP BY product_id
)
INSERT INTO gold.product_performance
    (release_id, product_id, sku, product_name, brand, category_id, units, revenue,
     catalog_price_usd, avg_order_unit_price, price_variance, bronze_lineage)
SELECT {{ release_id }},
       p.product_id, p.sku, p.name, p.brand, p.category_id,
       COALESCE(s.units, 0),
       COALESCE(s.revenue, 0),
       usd.amount,
       ROUND(s.revenue / NULLIF(s.units, 0), 2),
       ROUND(s.revenue / NULLIF(s.units, 0), 2) - usd.amount,
       lj.bronze_lineage
FROM silver.products p
LEFT JOIN sales s ON s.product_id = p.product_id
-- one current USD price per product (silver check guarantees at most one)
LEFT JOIN silver.product_prices usd ON usd.product_id = p.product_id AND usd.currency = 'USD' AND usd.is_current
JOIN lineage_json lj ON lj.product_id = p.product_id
WHERE p.is_current;
