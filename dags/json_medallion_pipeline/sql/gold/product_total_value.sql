-- Mirrors the expected output of the "products total value" exercise: price * quantity.
TRUNCATE gold.product_total_value;

INSERT INTO gold.product_total_value (product_id, name, category, total_value)
SELECT product_id,
       product_name,
       category,
       price * quantity
FROM silver.products
ORDER BY product_id;
