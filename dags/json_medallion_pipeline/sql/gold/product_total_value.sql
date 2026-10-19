-- Mirrors the expected output of the "products total value" exercise: price * quantity.
-- Grain/key: product_id. Rerun: full refresh (TRUNCATE + INSERT).
-- Lineage: bronze_* columns copied from the silver row (one bronze record per product).
TRUNCATE gold.product_total_value;

INSERT INTO gold.product_total_value
    (product_id, name, category, total_value, bronze_logical_date, bronze_record_index)
SELECT product_id,
       product_name,
       category,
       price * quantity,
       bronze_logical_date,
       bronze_record_index
FROM silver.products
ORDER BY product_id;
