-- Silver checks for orders. SQLCheckOperator passes only if every column of the first row is true.
-- Rejected orders are not a failure here: they are recorded in silver.rejected_record / ops.quality_issue.
SELECT
    NOT EXISTS (SELECT 1 FROM silver.customers WHERE is_current
                GROUP BY customer_id HAVING COUNT(*) > 1)                      AS one_current_customer_per_key,
    NOT EXISTS (SELECT 1 FROM silver.order_items WHERE quantity <= 0 OR unit_price < 0)
                                                                               AS items_valid,
    NOT EXISTS (SELECT 1 FROM silver.orders o
                WHERE NOT EXISTS (SELECT 1 FROM silver.order_items i WHERE i.order_id = o.order_id))
                                                                               AS every_order_has_items,
    NOT EXISTS (SELECT 1 FROM silver.orders o
                WHERE NOT EXISTS (SELECT 1 FROM silver.customers c
                                  WHERE c.customer_id = o.customer_id
                                    AND c.valid_from <= o.ordered_at
                                    AND (c.valid_to IS NULL OR o.ordered_at < c.valid_to)))
                                                                               AS every_order_has_a_customer;
