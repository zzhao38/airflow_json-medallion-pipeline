-- Row-level lineage: resolve any gold row back to the raw bronze record(s) it came from.
-- Views (not tables), so they survive the gold TRUNCATE + INSERT refreshes and are always current.
-- Every view exposes the gold key columns plus the bronze evidence:
--   source_table, logical_date, record_index, source_file, file_sha256, dag_run_id, payload.
-- Per-row models join on their own (bronze_logical_date, bronze_record_index); only the
-- category aggregate expands bronze_lineage (JSONB array) into one row per contributing record.

CREATE OR REPLACE VIEW gold.v_trace_product_total_value AS
SELECT g.product_id, g.name, g.category, g.total_value,
       'raw_products'::TEXT AS source_table,
       b.logical_date, b.record_index, b.source_file, b.file_sha256, b.dag_run_id, b.payload
FROM gold.product_total_value g
JOIN bronze.raw_products b
  ON b.logical_date = g.bronze_logical_date
 AND b.record_index = g.bronze_record_index;

CREATE OR REPLACE VIEW gold.v_trace_product_variation_flat AS
SELECT g.product_id, g.variation_seq, g.store_name, g.product_name, g.variation_price,
       'raw_store_catalog'::TEXT AS source_table,
       b.logical_date, b.record_index, b.source_file, b.file_sha256, b.dag_run_id, b.payload
FROM gold.product_variation_flat g
JOIN bronze.raw_store_catalog b
  ON b.logical_date = g.bronze_logical_date
 AND b.record_index = g.bronze_record_index;

CREATE OR REPLACE VIEW gold.v_trace_user_activity_summary AS
SELECT g.user_id, g.name, g.activity_count, g.total_activity_duration,
       'raw_users'::TEXT AS source_table,
       b.logical_date, b.record_index, b.source_file, b.file_sha256, b.dag_run_id, b.payload
FROM gold.user_activity_summary g
JOIN bronze.raw_users b
  ON b.logical_date = g.bronze_logical_date
 AND b.record_index = g.bronze_record_index;

CREATE OR REPLACE VIEW gold.v_trace_inventory_value_by_category AS
SELECT g.store_name, g.product_category, g.product_count, g.inventory_value,
       'raw_store_catalog'::TEXT AS source_table,
       b.logical_date, b.record_index, b.source_file, b.file_sha256, b.dag_run_id, b.payload
FROM gold.inventory_value_by_category g
CROSS JOIN LATERAL jsonb_array_elements(g.bronze_lineage) AS l (ref)
JOIN bronze.raw_store_catalog b
  ON b.logical_date = (l.ref ->> 'logical_date')::DATE
 AND b.record_index = (l.ref ->> 'record_index')::INTEGER;
