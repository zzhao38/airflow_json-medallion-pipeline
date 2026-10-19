-- store.json -> silver.stores / store_products (upsert)
--             -> product_variations / variation_specifications (replace per product)
-- Grain/key: store_name, product_id, (product_id, variation_seq), (..., spec_name).
-- Rerun: parents upsert on their natural key; children of each batch product are deleted and re-inserted.
-- Lineage: bronze_* columns carry the winning bronze record; all children inherit their store's.

-- Temp tables are dropped at commit. DISTINCT ON + ORDER BY record_index DESC: last record wins.
CREATE TEMP TABLE batch_stores ON COMMIT DROP AS
SELECT DISTINCT ON (payload ->> 'name')
       payload ->> 'name' AS store_name,
       payload,
       logical_date,
       record_index
FROM bronze.raw_store_catalog
WHERE logical_date = '{{ ds }}'
ORDER BY payload ->> 'name', record_index DESC;

INSERT INTO silver.stores
    (store_name, location, bronze_logical_date, bronze_record_index, updated_at)
SELECT store_name, payload ->> 'location', logical_date, record_index, now()
FROM batch_stores
ON CONFLICT (store_name) DO UPDATE
SET location            = EXCLUDED.location,
    bronze_logical_date = EXCLUDED.bronze_logical_date,
    bronze_record_index = EXCLUDED.bronze_record_index,
    updated_at          = now();

-- Explode products once; reused by every statement below.
-- WITH ORDINALITY gives the array position; the highest one wins for a duplicated product id.
CREATE TEMP TABLE batch_products ON COMMIT DROP AS
SELECT DISTINCT ON (p.value ->> 'id')
       p.value ->> 'id' AS product_id,
       s.store_id,
       p.value          AS product,
       b.logical_date,
       b.record_index
FROM batch_stores b
JOIN silver.stores s ON s.store_name = b.store_name
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(b.payload -> 'products', '[]'::JSONB))
    WITH ORDINALITY AS p (value, ordinality)
ORDER BY p.value ->> 'id', p.ordinality DESC;

INSERT INTO silver.store_products
    (product_id, store_id, product_name, category,
     bronze_logical_date, bronze_record_index, updated_at)
SELECT product_id, store_id, product ->> 'name', product ->> 'category',
       logical_date, record_index, now()
FROM batch_products
ON CONFLICT (product_id) DO UPDATE
SET store_id            = EXCLUDED.store_id,
    product_name        = EXCLUDED.product_name,
    category            = EXCLUDED.category,
    bronze_logical_date = EXCLUDED.bronze_logical_date,
    bronze_record_index = EXCLUDED.bronze_record_index,
    updated_at          = now();

-- Children: delete grandchildren first (FK order), then children, scoped to products in this batch.
DELETE FROM silver.variation_specifications vs
USING batch_products bp
WHERE vs.product_id = bp.product_id;

DELETE FROM silver.product_variations pv
USING batch_products bp
WHERE pv.product_id = bp.product_id;

INSERT INTO silver.product_variations
    (product_id, variation_seq, color, size, model, price, inventory,
     bronze_logical_date, bronze_record_index)
SELECT bp.product_id,
       v.ordinality,
       v.value ->> 'color',
       v.value ->> 'size',
       v.value ->> 'model',
       (v.value ->> 'price')::NUMERIC(12, 2),
       (v.value ->> 'inventory')::INTEGER,
       bp.logical_date,
       bp.record_index
FROM batch_products bp
-- WITH ORDINALITY: the array position becomes variation_seq (variations have no natural key).
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(bp.product -> 'variations', '[]'::JSONB))
    WITH ORDINALITY AS v (value, ordinality);

INSERT INTO silver.variation_specifications
    (product_id, variation_seq, spec_name, spec_value,
     bronze_logical_date, bronze_record_index)
SELECT bp.product_id,
       v.ordinality,
       spec.key,
       spec.value,
       bp.logical_date,
       bp.record_index
FROM batch_products bp
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(bp.product -> 'variations', '[]'::JSONB))
    WITH ORDINALITY AS v (value, ordinality)
-- jsonb_each_text explodes the open-ended specifications object into EAV (name, value) rows.
CROSS JOIN LATERAL jsonb_each_text(COALESCE(v.value -> 'specifications', '{}'::JSONB)) AS spec (key, value);
