-- products.json -> silver.products
-- DISTINCT ON keeps the last occurrence of a duplicated id within the batch so the
-- upsert never hits "ON CONFLICT ... cannot affect row a second time".
INSERT INTO silver.products (product_id, product_name, category, price, quantity, logical_date, updated_at)
SELECT DISTINCT ON ((payload ->> 'id')::INTEGER)
       (payload ->> 'id')::INTEGER,
       payload ->> 'name',
       payload ->> 'category',
       (payload ->> 'price')::NUMERIC(12, 2),
       (payload ->> 'quantity')::INTEGER,
       logical_date,
       now()
FROM bronze.raw_products
WHERE logical_date = '{{ ds }}'
ORDER BY (payload ->> 'id')::INTEGER, record_index DESC
ON CONFLICT (product_id) DO UPDATE
SET product_name = EXCLUDED.product_name,
    category     = EXCLUDED.category,
    price        = EXCLUDED.price,
    quantity     = EXCLUDED.quantity,
    logical_date = EXCLUDED.logical_date,
    updated_at   = now();
