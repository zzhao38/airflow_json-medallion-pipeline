-- Inventory position per store x category (price * on-hand across all variations).
TRUNCATE gold.inventory_value_by_category;

INSERT INTO gold.inventory_value_by_category (
    store_name, product_category, product_count, variation_count,
    total_inventory, inventory_value, avg_price
)
SELECT s.store_name,
       p.category,
       COUNT(DISTINCT p.product_id),
       COUNT(*),
       SUM(v.inventory),
       SUM(v.inventory * v.price),
       ROUND(AVG(v.price), 2)
FROM silver.product_variations v
JOIN silver.store_products p USING (product_id)
JOIN silver.stores s USING (store_id)
GROUP BY s.store_name, p.category
ORDER BY s.store_name, p.category;
