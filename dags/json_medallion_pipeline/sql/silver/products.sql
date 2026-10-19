-- products.json -> silver.products (SCD-2) + silver.categories (upsert) + silver.product_prices
-- (current-state children). Grain/key: one version per product_id; prices keyed (product_id, price_seq).
-- Rerun: an unchanged row_hash (attributes + prices) changes nothing; only products whose hash
--        changed get a new version, and only their prices are replaced.
-- Lineage: bronze_* columns carry the record behind the version; prices inherit their product's.
-- valid_from is the delivery's arrived_at (not now()), so a replay of the same delivery is deterministic.
--
-- Business rules live here (not in the JSON Schema) so one bad product rejects ONE record, not the file.
-- A rejected product gets a silver.rejected_record row and an ERROR quality issue; it is neither
-- versioned nor upserted, so any existing silver version of that product_id stays as it was.
-- Rules, first match wins:
--   1. a price amount is < 0
--   2. the product has no price with is_current = true
--   3. more than one is_current price for the same currency
--   4. the product_id appears more than once with DIFFERENT content: ALL versions are rejected
-- WARN only: identical duplicates collapse to one record.

-- Every record of the latest accepted delivery for this logical date.
CREATE TEMP TABLE batch_raw ON COMMIT DROP AS
SELECT r.arrival_id, r.record_index, r.payload, r.payload ->> 'product_id' AS product_id, l.delivered_at
FROM bronze.record r
JOIN bronze.v_latest_accepted l ON l.arrival_id = r.arrival_id
WHERE l.source = 'products' AND l.logical_date = '{{ ds }}'::DATE;

-- product_ids sent more than once; distinct_payloads = 1 means identical, > 1 means conflicting.
CREATE TEMP TABLE duplicate_products ON COMMIT DROP AS
SELECT product_id, COUNT(*) AS n, COUNT(DISTINCT payload) AS distinct_payloads, MIN(arrival_id) AS arrival_id
FROM batch_raw
GROUP BY product_id
HAVING COUNT(*) > 1;

-- Temp tables are dropped at commit. DISTINCT ON + ORDER BY record_index DESC: last record wins
-- for an identically duplicated product_id; conflicting duplicates are excluded (all rejected below).
-- Prices are normalised (amount to 2 dp) before hashing so a re-formatted 10.0 vs 10.00 never
-- creates a version; jsonb_build_array keeps NULLs and positions.
CREATE TEMP TABLE batch_products ON COMMIT DROP AS
SELECT s.*,
       md5(jsonb_build_array(s.sku, s.name, s.brand, s.category_id, s.prices)::TEXT) AS row_hash
FROM (
    SELECT DISTINCT ON (r.product_id)
           r.product_id,
           r.payload -> 'details' ->> 'sku'                 AS sku,
           r.payload -> 'details' ->> 'name'                AS name,
           r.payload -> 'details' ->> 'brand'               AS brand,
           r.payload -> 'category' ->> 'id'                 AS category_id,
           r.payload -> 'category' -> 'path'                AS category_path,
           COALESCE((SELECT jsonb_agg(jsonb_build_array(pr.value ->> 'currency',
                                                        (pr.value ->> 'amount')::NUMERIC(12, 2),
                                                        (pr.value ->> 'is_current')::BOOLEAN)
                                      ORDER BY pr.ordinality)
                     FROM jsonb_array_elements(r.payload -> 'prices') WITH ORDINALITY AS pr (value, ordinality)),
                    '[]'::JSONB)                            AS prices,
           r.payload,
           r.arrival_id,
           r.record_index,
           r.delivered_at
    FROM batch_raw r
    WHERE NOT EXISTS (SELECT 1 FROM duplicate_products d
                      WHERE d.product_id = r.product_id AND d.distinct_payloads > 1)
    ORDER BY r.product_id, r.record_index DESC
) s;

-- WARN (once per run): identical duplicates that were collapsed.
INSERT INTO ops.quality_issue (dag_run_id, logical_date, layer, source_or_model, severity, reason, arrival_id)
SELECT '{{ run_id }}', '{{ ds }}'::DATE, 'silver', 'products', 'WARN', d.reason, d.arrival_id
FROM (
    SELECT string_agg(format('%s records for product_id %s (identical)', n, product_id), '; '
                      ORDER BY product_id) AS reason,
           MIN(arrival_id) AS arrival_id
    FROM duplicate_products
    WHERE distinct_payloads = 1
) d
WHERE d.reason IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM ops.quality_issue q
                  WHERE q.dag_run_id = '{{ run_id }}' AND q.layer = 'silver' AND q.source_or_model = 'products'
                    AND q.severity = 'WARN' AND q.reason = d.reason);

-- The normalised prices, one row per price (price_seq = array position).
CREATE TEMP TABLE price_rows ON COMMIT DROP AS
SELECT b.product_id,
       pr.ordinality                    AS price_seq,
       pr.value ->> 0                   AS currency,
       (pr.value ->> 1)::NUMERIC(12, 2) AS amount,
       (pr.value ->> 2)::BOOLEAN        AS is_current
FROM batch_products b
CROSS JOIN LATERAL jsonb_array_elements(b.prices) WITH ORDINALITY AS pr (value, ordinality);

-- Business-rule violations: one row per product (the first rule that applies).
CREATE TEMP TABLE product_violations ON COMMIT DROP AS
SELECT DISTINCT ON (product_id) product_id, reason
FROM (
    SELECT product_id, 1 AS priority, price_seq,
           format('price amount %s must be >= 0 (%s)', amount, currency) AS reason
    FROM price_rows
    WHERE amount < 0
    UNION ALL
    SELECT b.product_id, 2, 0, 'no current price'
    FROM batch_products b
    WHERE NOT EXISTS (SELECT 1 FROM price_rows p WHERE p.product_id = b.product_id AND p.is_current)
    UNION ALL
    SELECT product_id, 3, 0, format('%s current prices for currency %s', COUNT(*), currency)
    FROM price_rows
    WHERE is_current
    GROUP BY product_id, currency
    HAVING COUNT(*) > 1
) v
ORDER BY product_id, priority, price_seq, reason;

-- Every rejected RECORD: one per violating product, and every version of a conflicting duplicate.
CREATE TEMP TABLE rejects ON COMMIT DROP AS
SELECT b.arrival_id, b.record_index, b.product_id, v.reason, b.payload
FROM product_violations v
JOIN batch_products b ON b.product_id = v.product_id
UNION ALL
SELECT r.arrival_id, r.record_index, r.product_id,
       format('conflicting duplicates for product_id %s (%s versions)', d.product_id, d.n), r.payload
FROM duplicate_products d
JOIN batch_raw r ON r.product_id = d.product_id
WHERE d.distinct_payloads > 1;

INSERT INTO silver.rejected_record (arrival_id, record_index, source, entity_key, reason, payload)
SELECT arrival_id, record_index, 'products', product_id, reason, payload
FROM rejects
ON CONFLICT (arrival_id, record_index) DO NOTHING;

-- ERROR (once per run + product): a rejected product is visible in ops.quality_issue.
INSERT INTO ops.quality_issue (dag_run_id, logical_date, layer, source_or_model, severity, reason, arrival_id)
SELECT DISTINCT '{{ run_id }}', '{{ ds }}'::DATE, 'silver', 'products', 'ERROR',
       format('product %s rejected: %s', x.product_id, x.reason), x.arrival_id
FROM rejects x
WHERE NOT EXISTS (SELECT 1 FROM ops.quality_issue q
                  WHERE q.dag_run_id = '{{ run_id }}' AND q.layer = 'silver' AND q.source_or_model = 'products'
                    AND q.severity = 'ERROR'
                    AND q.reason = format('product %s rejected: %s', x.product_id, x.reason));

-- Only products that passed every rule go on to categories and versions.
CREATE TEMP TABLE valid_products ON COMMIT DROP AS
SELECT b.*
FROM batch_products b
WHERE NOT EXISTS (SELECT 1 FROM product_violations v WHERE v.product_id = b.product_id);

-- Categories: current-state, upserted from every valid product in the batch (last record wins).
INSERT INTO silver.categories (category_id, path, level_1, level_2, bronze_arrival_id, bronze_record_index)
SELECT c.category_id, c.path, c.path[1], c.path[2], c.arrival_id, c.record_index
FROM (
    SELECT DISTINCT ON (b.category_id)
           b.category_id,
           (SELECT array_agg(p.value ORDER BY p.ordinality)
            FROM jsonb_array_elements_text(b.category_path) WITH ORDINALITY AS p (value, ordinality)) AS path,
           b.arrival_id,
           b.record_index
    FROM valid_products b
    ORDER BY b.category_id, b.record_index DESC
) c
ON CONFLICT (category_id) DO UPDATE
SET path = EXCLUDED.path, level_1 = EXCLUDED.level_1, level_2 = EXCLUDED.level_2,
    bronze_arrival_id = EXCLUDED.bronze_arrival_id, bronze_record_index = EXCLUDED.bronze_record_index
WHERE (silver.categories.path, silver.categories.level_1, silver.categories.level_2)
      IS DISTINCT FROM (EXCLUDED.path, EXCLUDED.level_1, EXCLUDED.level_2);

-- New keys and keys whose attributes or prices changed.
CREATE TEMP TABLE changed_products ON COMMIT DROP AS
SELECT b.*
FROM valid_products b
LEFT JOIN silver.products p ON p.product_id = b.product_id AND p.is_current
WHERE p.product_id IS NULL OR p.row_hash <> b.row_hash;

UPDATE silver.products p
SET valid_to = c.delivered_at, is_current = FALSE
FROM changed_products c
WHERE p.product_id = c.product_id AND p.is_current;

INSERT INTO silver.products
    (product_id, sku, name, brand, category_id, row_hash, valid_from, bronze_arrival_id, bronze_record_index)
SELECT product_id, sku, name, brand, category_id, row_hash, delivered_at, arrival_id, record_index
FROM changed_products;

-- Prices carry no natural key, so the child set is replaced for every changed product.
DELETE FROM silver.product_prices pp
USING changed_products c
WHERE pp.product_id = c.product_id;

INSERT INTO silver.product_prices
    (product_id, price_seq, currency, amount, is_current, bronze_arrival_id, bronze_record_index)
SELECT c.product_id,
       pr.ordinality,
       pr.value ->> 0,
       (pr.value ->> 1)::NUMERIC(12, 2),
       (pr.value ->> 2)::BOOLEAN,
       c.arrival_id,
       c.record_index
FROM changed_products c
-- WITH ORDINALITY: the array position becomes the child key (price_seq).
CROSS JOIN LATERAL jsonb_array_elements(c.prices) WITH ORDINALITY AS pr (value, ordinality);
