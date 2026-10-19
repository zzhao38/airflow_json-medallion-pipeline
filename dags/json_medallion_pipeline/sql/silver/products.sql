-- products.json -> silver.products. Grain/key: product_id.
-- Rerun: upsert on product_id, so replaying a logical_date converges to the same rows.
-- Lineage: bronze_* columns point at the winning bronze record.
INSERT INTO silver.products
    (product_id, product_name, category, price, quantity,
     bronze_logical_date, bronze_record_index, updated_at)
-- DISTINCT ON + ORDER BY record_index DESC: the last record wins for a duplicated id, and
-- the upsert never hits "ON CONFLICT ... cannot affect row a second time".
SELECT DISTINCT ON ((payload ->> 'id')::INTEGER)
       (payload ->> 'id')::INTEGER,
       payload ->> 'name',
       payload ->> 'category',
       (payload ->> 'price')::NUMERIC(12, 2),
       (payload ->> 'quantity')::INTEGER,
       logical_date,
       record_index,
       now()
FROM bronze.raw_products
WHERE logical_date = '{{ ds }}'
ORDER BY (payload ->> 'id')::INTEGER, record_index DESC
ON CONFLICT (product_id) DO UPDATE
SET product_name        = EXCLUDED.product_name,
    category            = EXCLUDED.category,
    price               = EXCLUDED.price,
    quantity            = EXCLUDED.quantity,
    bronze_logical_date = EXCLUDED.bronze_logical_date,
    bronze_record_index = EXCLUDED.bronze_record_index,
    updated_at          = now();
