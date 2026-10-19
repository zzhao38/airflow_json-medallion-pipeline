-- Gold vs silver reconciliation, scoped to the release being built.
-- SQLCheckOperator passes only if every column of the first row is true.
-- Expected = silver items whose product is CURRENT. Parked items (unknown or rejected product) are
-- deliberately not in gold, so they are not expected here either.
{% set release_id = ti.xcom_pull(task_ids='create_release') %}
WITH expected AS (
    SELECT COUNT(*) AS items, COALESCE(SUM(i.line_amount), 0) AS revenue
    FROM silver.order_items i
    WHERE EXISTS (SELECT 1 FROM silver.products p WHERE p.product_id = i.product_id AND p.is_current)
)
SELECT
    (SELECT COUNT(*) FROM gold.order_lines WHERE release_id = {{ release_id }})
      = (SELECT items FROM expected)                                                AS line_count_matches,
    (SELECT COALESCE(SUM(line_revenue), 0) FROM gold.order_lines WHERE release_id = {{ release_id }})
      = (SELECT revenue FROM expected)                                              AS line_revenue_matches,
    (SELECT COALESCE(SUM(revenue), 0) FROM gold.daily_sales_by_category WHERE release_id = {{ release_id }})
      = (SELECT revenue FROM expected)                                              AS daily_sales_revenue_matches,
    (SELECT COALESCE(SUM(revenue), 0) FROM gold.customer_summary WHERE release_id = {{ release_id }})
      = (SELECT revenue FROM expected)                                              AS customer_revenue_matches,
    (SELECT COALESCE(SUM(revenue), 0) FROM gold.product_performance WHERE release_id = {{ release_id }})
      = (SELECT revenue FROM expected)                                              AS product_revenue_matches;
