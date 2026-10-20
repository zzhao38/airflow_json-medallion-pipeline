-- orders.json -> silver.customers (SCD-2) + silver.orders / silver.order_items (current-state facts)
-- + silver.rejected_record. Grain/key: one customer version per customer_id; one row per valid order_id.
-- Rerun: unchanged customers get no new version; orders are upserted (only a real change rewrites
--        a row); items are replaced per order; rejects and quality issues are insert-once.
-- Lineage: bronze_* columns carry the order record behind every row (items inherit their order's).
-- Customer valid_from uses customer.effective_at; legacy records fall back to order.ordered_at. Timelines
-- are rebuilt by effective time, so late changes can split an existing SCD-2 interval.
--
-- Business rules live here (not in the JSON Schema) so one bad order rejects ONE record, not the file.
-- A rejected order (whole order, never a single item) gets a silver.rejected_record row and an ERROR
-- quality issue; any existing silver row for that order_id is left untouched. Rules, first match wins:
--   1. an item has quantity <= 0
--   2. an item has unit_price < 0
--   3. an order_item_id is reused: by an EARLIER order of this batch, or twice in the same order
--   4. an order_item_id already belongs to a DIFFERENT order in silver
--   5. ordered_at (UTC date) is after the batch's logical date (future-dated)
--   6. the order_id appears more than once with DIFFERENT content: ALL versions are rejected
-- WARN only (order kept): identical duplicates collapse to one; different names for one customer at the
-- same effective time use the last record; an item whose product has no CURRENT silver.products row is PARKED -- it stays in
-- silver (view silver.v_parked_order_items) but is excluded from gold until the product arrives.

-- Every record of the latest accepted delivery for this logical date.
CREATE TEMP TABLE batch_raw ON COMMIT DROP AS
SELECT r.arrival_id, r.record_index, r.payload, r.payload ->> 'order_id' AS order_id, l.delivered_at
FROM bronze.record r
JOIN bronze.v_latest_accepted l ON l.arrival_id = r.arrival_id
WHERE l.source = 'orders' AND l.logical_date = '{{ ds }}'::DATE;

-- order_ids sent more than once; distinct_payloads = 1 means identical, > 1 means conflicting.
CREATE TEMP TABLE duplicate_orders ON COMMIT DROP AS
SELECT order_id, COUNT(*) AS n, COUNT(DISTINCT payload) AS distinct_payloads, MIN(arrival_id) AS arrival_id
FROM batch_raw
GROUP BY order_id
HAVING COUNT(*) > 1;

-- One row per order_id (last record wins; identical duplicates collapse), flattened.
-- Conflicting duplicates are excluded here: every version of them is rejected below.
CREATE TEMP TABLE batch_orders ON COMMIT DROP AS
SELECT DISTINCT ON (order_id)
       order_id,
       payload -> 'customer' ->> 'customer_id'                             AS customer_id,
       payload -> 'customer' ->> 'name'                                    AS customer_name,
       COALESCE((payload -> 'customer' ->> 'effective_at')::TIMESTAMPTZ,
                (payload ->> 'ordered_at')::TIMESTAMPTZ)                   AS customer_effective_at,
       (payload ->> 'ordered_at')::TIMESTAMPTZ                             AS ordered_at,
       ((payload ->> 'ordered_at')::TIMESTAMPTZ AT TIME ZONE 'UTC')::DATE  AS order_date,
       payload -> 'shipping' ->> 'city'                                    AS ship_city,
       payload -> 'shipping' ->> 'state'                                   AS ship_state,
       payload -> 'shipping' ->> 'method'                                  AS ship_method,
       payload,
       arrival_id,
       record_index,
       delivered_at
FROM batch_raw r
WHERE NOT EXISTS (SELECT 1 FROM duplicate_orders d WHERE d.order_id = r.order_id AND d.distinct_payloads > 1)
ORDER BY order_id, record_index DESC;

-- WARN (once per run): identical duplicates that were collapsed.
INSERT INTO ops.quality_issue (dag_run_id, logical_date, layer, source_or_model, severity, reason, arrival_id)
SELECT '{{ run_id }}', '{{ ds }}'::DATE, 'silver', 'orders', 'WARN', d.reason, d.arrival_id
FROM (
    SELECT string_agg(format('%s records for order_id %s (identical)', n, order_id), '; ' ORDER BY order_id) AS reason,
           MIN(arrival_id) AS arrival_id
    FROM duplicate_orders
    WHERE distinct_payloads = 1
) d
WHERE d.reason IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM ops.quality_issue q
                  WHERE q.dag_run_id = '{{ run_id }}' AND q.layer = 'silver' AND q.source_or_model = 'orders'
                    AND q.severity = 'WARN' AND q.reason = d.reason);

-- Items, unnested with their position (item_seq picks the first offender deterministically).
CREATE TEMP TABLE batch_items ON COMMIT DROP AS
SELECT o.order_id,
       o.record_index,
       i.value ->> 'order_item_id'                       AS order_item_id,
       i.value ->> 'product_id'                          AS product_id,
       (i.value ->> 'quantity')::NUMERIC::INTEGER        AS quantity,
       (i.value ->> 'unit_price')::NUMERIC(12, 2)        AS unit_price,
       i.ordinality                                      AS item_seq
FROM batch_orders o
CROSS JOIN LATERAL jsonb_array_elements(o.payload -> 'items') WITH ORDINALITY AS i (value, ordinality);

-- Business-rule violations: one row per order (the first rule that applies, then the first item).
CREATE TEMP TABLE order_violations ON COMMIT DROP AS
SELECT DISTINCT ON (order_id) order_id, reason
FROM (
    SELECT order_id, 1 AS priority, item_seq,
           format('item %s: quantity %s must be > 0', order_item_id, quantity) AS reason
    FROM batch_items
    WHERE quantity <= 0
    UNION ALL
    SELECT order_id, 2, item_seq,
           format('item %s: unit_price %s must be >= 0', order_item_id, unit_price)
    FROM batch_items
    WHERE unit_price < 0
    UNION ALL
    -- the earliest use of an order_item_id in the batch is fine; every later use is the violation
    SELECT bi.order_id, 3, bi.item_seq,
           CASE WHEN fu.order_id = bi.order_id
                THEN format('item %s appears more than once in the order', bi.order_item_id)
                ELSE format('item %s already used by order %s', bi.order_item_id, fu.order_id)
           END
    FROM batch_items bi
    JOIN LATERAL (SELECT f.order_id, f.record_index, f.item_seq
                  FROM batch_items f
                  WHERE f.order_item_id = bi.order_item_id
                  ORDER BY f.record_index, f.item_seq
                  LIMIT 1) fu ON (fu.record_index, fu.item_seq) <> (bi.record_index, bi.item_seq)
    UNION ALL
    SELECT bi.order_id, 4, bi.item_seq,
           format('item %s already belongs to order %s in silver', bi.order_item_id, si.order_id)
    FROM batch_items bi
    JOIN silver.order_items si ON si.order_item_id = bi.order_item_id AND si.order_id <> bi.order_id
    UNION ALL
    SELECT order_id, 5, 0,
           format('ordered_at %s is after the batch date %s', payload ->> 'ordered_at', '{{ ds }}')
    FROM batch_orders
    WHERE order_date > '{{ ds }}'::DATE
) v
ORDER BY order_id, priority, item_seq;

-- Every rejected RECORD: one per violating order, and every version of a conflicting duplicate.
CREATE TEMP TABLE rejects ON COMMIT DROP AS
SELECT o.arrival_id, o.record_index, o.order_id, v.reason, o.payload
FROM order_violations v
JOIN batch_orders o ON o.order_id = v.order_id
UNION ALL
SELECT r.arrival_id, r.record_index, r.order_id,
       format('conflicting duplicates for order_id %s (%s versions)', d.order_id, d.n), r.payload
FROM duplicate_orders d
JOIN batch_raw r ON r.order_id = d.order_id
WHERE d.distinct_payloads > 1;

INSERT INTO silver.rejected_record (arrival_id, record_index, source, entity_key, reason, payload)
SELECT arrival_id, record_index, 'orders', order_id, reason, payload
FROM rejects
ON CONFLICT (arrival_id, record_index) DO NOTHING;

-- ERROR (once per run + order): a rejected order is visible in ops.quality_issue.
INSERT INTO ops.quality_issue (dag_run_id, logical_date, layer, source_or_model, severity, reason, arrival_id)
SELECT DISTINCT '{{ run_id }}', '{{ ds }}'::DATE, 'silver', 'orders', 'ERROR',
       format('order %s rejected: %s', x.order_id, x.reason), x.arrival_id
FROM rejects x
WHERE NOT EXISTS (SELECT 1 FROM ops.quality_issue q
                  WHERE q.dag_run_id = '{{ run_id }}' AND q.layer = 'silver' AND q.source_or_model = 'orders'
                    AND q.severity = 'ERROR' AND q.reason = format('order %s rejected: %s', x.order_id, x.reason));

-- Customers come from every non-conflicting order record, rejected orders included (the customer
-- exists even if one order is bad). Same-time name conflicts warn; different effective times form history.
CREATE TEMP TABLE batch_customers ON COMMIT DROP AS
SELECT DISTINCT ON (customer_id, customer_effective_at)
       customer_id, customer_name AS name, customer_effective_at AS valid_from,
       md5(jsonb_build_array(customer_name)::TEXT) AS row_hash,
       arrival_id, record_index
FROM batch_orders
ORDER BY customer_id, customer_effective_at, record_index DESC;

-- WARN only when names conflict at the SAME effective instant; different times are ordinary history.
INSERT INTO ops.quality_issue (dag_run_id, logical_date, layer, source_or_model, severity, reason, arrival_id)
SELECT '{{ run_id }}', '{{ ds }}'::DATE, 'silver', 'orders', 'WARN', c.reason, c.arrival_id
FROM (
    SELECT format('customer %s has %s names in batch at effective_at %s (%s); last record kept',
                  customer_id, COUNT(DISTINCT customer_name), customer_effective_at,
                  string_agg(DISTINCT customer_name, ', ')) AS reason,
           MIN(arrival_id) AS arrival_id
    FROM batch_orders
    GROUP BY customer_id, customer_effective_at
    HAVING COUNT(DISTINCT customer_name) > 1
) c
WHERE NOT EXISTS (SELECT 1 FROM ops.quality_issue q
                  WHERE q.dag_run_id = '{{ run_id }}' AND q.layer = 'silver' AND q.source_or_model = 'orders'
                    AND q.severity = 'WARN' AND q.reason = c.reason);

-- Merge known versions with new observations, preferring the latest delivery at an identical
-- effective time. Remove adjacent versions with identical attributes after inserting late events.
CREATE TEMP TABLE customer_timeline_candidates ON COMMIT DROP AS
SELECT c.customer_id, c.name, c.row_hash, c.valid_from,
       c.bronze_arrival_id, c.bronze_record_index, FALSE AS incoming
FROM silver.customers c
WHERE EXISTS (SELECT 1 FROM batch_customers b WHERE b.customer_id = c.customer_id)
UNION ALL
SELECT b.customer_id, b.name, b.row_hash, b.valid_from,
       b.arrival_id, b.record_index, TRUE AS incoming
FROM batch_customers b;

CREATE TEMP TABLE customer_timeline_latest ON COMMIT DROP AS
SELECT DISTINCT ON (customer_id, valid_from)
       customer_id, name, row_hash, valid_from, bronze_arrival_id, bronze_record_index
FROM customer_timeline_candidates
ORDER BY customer_id, valid_from, incoming DESC, bronze_arrival_id DESC, bronze_record_index DESC;

CREATE TEMP TABLE customer_timeline ON COMMIT DROP AS
SELECT customer_id, name, row_hash, valid_from, bronze_arrival_id, bronze_record_index
FROM (
    SELECT t.*,
           LAG(row_hash) OVER (PARTITION BY customer_id ORDER BY valid_from) AS previous_hash
    FROM customer_timeline_latest t
) ordered
WHERE previous_hash IS DISTINCT FROM row_hash;

-- Temporarily close touched rows so the one-current-row index remains valid during the rebuild.
UPDATE silver.customers c
SET valid_to = COALESCE(c.valid_to, c.valid_from), is_current = FALSE
WHERE EXISTS (SELECT 1 FROM batch_customers b WHERE b.customer_id = c.customer_id);

DELETE FROM silver.customers c
WHERE EXISTS (SELECT 1 FROM batch_customers b WHERE b.customer_id = c.customer_id)
  AND NOT EXISTS (SELECT 1 FROM customer_timeline t
                  WHERE t.customer_id = c.customer_id AND t.valid_from = c.valid_from);

UPDATE silver.customers c
SET name = t.name, row_hash = t.row_hash,
    bronze_arrival_id = t.bronze_arrival_id, bronze_record_index = t.bronze_record_index
FROM customer_timeline t
WHERE c.customer_id = t.customer_id AND c.valid_from = t.valid_from;

INSERT INTO silver.customers
    (customer_id, name, row_hash, valid_from, valid_to, is_current, bronze_arrival_id, bronze_record_index)
SELECT t.customer_id, t.name, t.row_hash, t.valid_from, t.valid_from, FALSE,
       t.bronze_arrival_id, t.bronze_record_index
FROM customer_timeline t
WHERE NOT EXISTS (SELECT 1 FROM silver.customers c
                  WHERE c.customer_id = t.customer_id AND c.valid_from = t.valid_from);

WITH intervals AS (
    SELECT customer_sk,
           LEAD(valid_from) OVER (PARTITION BY customer_id ORDER BY valid_from) AS next_valid_from
    FROM silver.customers c
    WHERE EXISTS (SELECT 1 FROM batch_customers b WHERE b.customer_id = c.customer_id)
)
UPDATE silver.customers c
SET valid_to = i.next_valid_from, is_current = (i.next_valid_from IS NULL)
FROM intervals i
WHERE c.customer_sk = i.customer_sk;

-- Valid orders: upsert; a row is rewritten only when something actually changed.
CREATE TEMP TABLE valid_orders ON COMMIT DROP AS
SELECT o.*
FROM batch_orders o
WHERE NOT EXISTS (SELECT 1 FROM order_violations v WHERE v.order_id = o.order_id);

INSERT INTO silver.orders
    (order_id, customer_id, ordered_at, order_date, ship_city, ship_state, ship_method,
     bronze_arrival_id, bronze_record_index)
SELECT order_id, customer_id, ordered_at, order_date, ship_city, ship_state, ship_method, arrival_id, record_index
FROM valid_orders
ON CONFLICT (order_id) DO UPDATE
SET customer_id = EXCLUDED.customer_id, ordered_at = EXCLUDED.ordered_at, order_date = EXCLUDED.order_date,
    ship_city = EXCLUDED.ship_city, ship_state = EXCLUDED.ship_state, ship_method = EXCLUDED.ship_method,
    bronze_arrival_id = EXCLUDED.bronze_arrival_id, bronze_record_index = EXCLUDED.bronze_record_index,
    updated_at = now()
WHERE (silver.orders.customer_id, silver.orders.ordered_at, silver.orders.ship_city, silver.orders.ship_state,
       silver.orders.ship_method, silver.orders.bronze_arrival_id, silver.orders.bronze_record_index)
      IS DISTINCT FROM
      (EXCLUDED.customer_id, EXCLUDED.ordered_at, EXCLUDED.ship_city, EXCLUDED.ship_state,
       EXCLUDED.ship_method, EXCLUDED.bronze_arrival_id, EXCLUDED.bronze_record_index);

-- Items are replaced for every valid order in the batch (a re-sent order may drop or add items).
DELETE FROM silver.order_items i
USING valid_orders o
WHERE i.order_id = o.order_id;

INSERT INTO silver.order_items
    (order_item_id, order_id, product_id, quantity, unit_price, bronze_arrival_id, bronze_record_index)
SELECT bi.order_item_id, bi.order_id, bi.product_id, bi.quantity, bi.unit_price, o.arrival_id, o.record_index
FROM batch_items bi
JOIN valid_orders o ON o.order_id = bi.order_id;

-- WARN (once per run + item): the item's product has no CURRENT silver.products row, so the item is
-- PARKED: kept in silver (silver.v_parked_order_items), excluded from every gold model.
INSERT INTO ops.quality_issue (dag_run_id, logical_date, layer, source_or_model, severity, reason, arrival_id)
SELECT '{{ run_id }}', '{{ ds }}'::DATE, 'silver', 'orders', 'WARN',
       format('item %s references unknown product %s (parked)', bi.order_item_id, bi.product_id), o.arrival_id
FROM batch_items bi
JOIN valid_orders o ON o.order_id = bi.order_id
WHERE NOT EXISTS (SELECT 1 FROM silver.products p WHERE p.product_id = bi.product_id AND p.is_current)
  AND NOT EXISTS (SELECT 1 FROM ops.quality_issue q
                  WHERE q.dag_run_id = '{{ run_id }}' AND q.layer = 'silver' AND q.source_or_model = 'orders'
                    AND q.severity = 'WARN'
                    AND q.reason = format('item %s references unknown product %s (parked)',
                                          bi.order_item_id, bi.product_id));
