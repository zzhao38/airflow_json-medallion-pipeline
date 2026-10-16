-- Reconciliation: the flattened table must have exactly one row per silver variation,
-- and the category rollup must account for the same total inventory.
SELECT (SELECT COUNT(*) FROM gold.product_variation_flat)
     = (SELECT COUNT(*) FROM silver.product_variations)                                AS flat_rowcount_match,
       (SELECT COALESCE(SUM(total_inventory), 0) FROM gold.inventory_value_by_category)
     = (SELECT COALESCE(SUM(inventory), 0)       FROM silver.product_variations)      AS inventory_totals_match,
       (SELECT COUNT(*) FROM gold.product_total_value)
     = (SELECT COUNT(*) FROM silver.products)                                          AS product_counts_match;
