-- store.json -> silver.stores / store_products (upsert)
--             -> product_variations / variation_specifications (replace per product)
CREATE TEMP TABLE batch_stores ON COMMIT DROP AS
SELECT DISTINCT ON (payload ->> 'name')
       payload ->> 'name' AS store_name,
       payload,
       logical_date
FROM bronze.raw_store_catalog
WHERE logical_date = '{{ ds }}'
ORDER BY payload ->> 'name', record_index DESC;

INSERT INTO silver.stores (store_name, location, logical_date, updated_at)
SELECT store_name, payload ->> 'location', logical_date, now()
FROM batch_stores
ON CONFLICT (store_name) DO UPDATE
SET location     = EXCLUDED.location,
    logical_date = EXCLUDED.logical_date,
    updated_at   = now();

-- Explode products once; reused by every statement below.
CREATE TEMP TABLE batch_products ON COMMIT DROP AS
SELECT DISTINCT ON (p.value ->> 'id')
       p.value ->> 'id' AS product_id,
       s.store_id,
       p.value          AS product,
       b.logical_date
FROM batch_stores b
JOIN silver.stores s ON s.store_name = b.store_name
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(b.payload -> 'products', '[]'::JSONB))
    WITH ORDINALITY AS p (value, ordinality)
ORDER BY p.value ->> 'id', p.ordinality DESC;

INSERT INTO silver.store_products (product_id, store_id, product_name, category, logical_date, updated_at)
SELECT product_id, store_id, product ->> 'name', product ->> 'category', logical_date, now()
FROM batch_products
ON CONFLICT (product_id) DO UPDATE
SET store_id     = EXCLUDED.store_id,
    product_name = EXCLUDED.product_name,
    category     = EXCLUDED.category,
    logical_date = EXCLUDED.logical_date,
    updated_at   = now();

-- Children: delete grandchildren first (FK), then children, scoped to products in this batch.
DELETE FROM silver.variation_specifications vs
USING batch_products bp
WHERE vs.product_id = bp.product_id;

DELETE FROM silver.product_variations pv
USING batch_products bp
WHERE pv.product_id = bp.product_id;

INSERT INTO silver.product_variations
    (product_id, variation_seq, color, size, model, price, inventory, logical_date)
SELECT bp.product_id,
       v.ordinality,
       v.value ->> 'color',
       v.value ->> 'size',
       v.value ->> 'model',
       (v.value ->> 'price')::NUMERIC(12, 2),
       (v.value ->> 'inventory')::INTEGER,
       bp.logical_date
FROM batch_products bp
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(bp.product -> 'variations', '[]'::JSONB))
    WITH ORDINALITY AS v (value, ordinality);

INSERT INTO silver.variation_specifications
    (product_id, variation_seq, spec_name, spec_value, logical_date)
SELECT bp.product_id,
       v.ordinality,
       spec.key,
       spec.value,
       bp.logical_date
FROM batch_products bp
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(bp.product -> 'variations', '[]'::JSONB))
    WITH ORDINALITY AS v (value, ordinality)
CROSS JOIN LATERAL jsonb_each_text(COALESCE(v.value -> 'specifications', '{}'::JSONB)) AS spec (key, value);
