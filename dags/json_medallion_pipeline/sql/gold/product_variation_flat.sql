-- Mirrors the expected output of the "flatten store JSON" exercise: one row per variation.
-- Grain/key: (product_id, variation_seq). Rerun: full refresh (TRUNCATE + INSERT).
-- Lineage: bronze_* columns copied from the silver variation.
-- MAX(...) FILTER pivots the EAV specifications back into columns at the consumer layer.
TRUNCATE gold.product_variation_flat;

INSERT INTO gold.product_variation_flat (
    store_name, store_location, product_id, product_name, product_category, variation_seq,
    variation_color, variation_size, variation_model,
    variation_specifications_ram, variation_specifications_storage,
    variation_price, variation_inventory,
    bronze_logical_date, bronze_record_index
)
SELECT s.store_name,
       s.location,
       p.product_id,
       p.product_name,
       p.category,
       v.variation_seq,
       v.color,
       v.size,
       v.model,
       MAX(sp.spec_value) FILTER (WHERE sp.spec_name = 'ram'),
       MAX(sp.spec_value) FILTER (WHERE sp.spec_name = 'storage'),
       v.price,
       v.inventory,
       v.bronze_logical_date,
       v.bronze_record_index
FROM silver.product_variations v
JOIN silver.store_products p USING (product_id)
JOIN silver.stores s USING (store_id)
LEFT JOIN silver.variation_specifications sp
       ON sp.product_id = v.product_id AND sp.variation_seq = v.variation_seq
GROUP BY s.store_name, s.location, p.product_id, p.product_name, p.category,
         v.variation_seq, v.color, v.size, v.model, v.price, v.inventory,
         v.bronze_logical_date, v.bronze_record_index
ORDER BY p.product_id, v.variation_seq;
