-- Silver checks for products. SQLCheckOperator passes only if every column of the first row is true.
-- Safety net behind the business rules in silver/products.sql: rejected products never get here, so
-- these must always hold for what is in silver.
SELECT
    EXISTS (SELECT 1 FROM silver.products WHERE is_current)                    AS has_current_rows,
    NOT EXISTS (SELECT 1 FROM silver.products WHERE is_current
                GROUP BY product_id HAVING COUNT(*) > 1)                       AS one_current_row_per_key,
    NOT EXISTS (SELECT 1 FROM silver.products p
                WHERE p.is_current
                  AND NOT EXISTS (SELECT 1 FROM silver.categories c WHERE c.category_id = p.category_id))
                                                                               AS category_exists,
    NOT EXISTS (SELECT 1 FROM silver.product_prices pp
                JOIN silver.products p ON p.product_id = pp.product_id AND p.is_current
                GROUP BY pp.product_id, pp.currency
                HAVING COUNT(*) FILTER (WHERE pp.is_current) > 1)              AS at_most_one_current_price_per_currency,
    NOT EXISTS (SELECT 1 FROM silver.products p
                WHERE p.is_current
                  AND NOT EXISTS (SELECT 1 FROM silver.product_prices pp
                                  WHERE pp.product_id = p.product_id AND pp.is_current))
                                                                               AS every_product_has_a_current_price;
