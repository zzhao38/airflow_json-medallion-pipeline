-- Trace views: gold row -> release -> bronze arrival + raw payload (the "why is this number X" path).
-- Grain: same as the gold table each view wraps (aggregates: one row per contributing bronze record).
-- Read-only; nothing here is written by the DAG.
-- Lineage: join on (bronze_arrival_id, bronze_record_index) -> bronze.record / bronze.arrival.

CREATE OR REPLACE VIEW gold.v_trace_order_lines AS
SELECT g.release_id, rel.release_version, g.order_item_id, g.order_id, g.product_id, g.line_revenue,
       g.bronze_arrival_id, g.bronze_record_index,
       a.source_file, a.file_sha256, a.schema_version, a.batch_id, a.arrived_at, a.dag_run_id,
       r.payload AS bronze_payload
FROM gold.order_lines g
JOIN ops.release rel     ON rel.release_id = g.release_id
JOIN bronze.record r     ON r.arrival_id = g.bronze_arrival_id AND r.record_index = g.bronze_record_index
JOIN bronze.arrival a    ON a.arrival_id = r.arrival_id;

CREATE OR REPLACE VIEW gold.v_trace_daily_sales_by_category AS
SELECT g.release_id, rel.release_version, g.order_date, g.category_id, g.revenue,
       (l.value ->> 'arrival_id')::BIGINT    AS bronze_arrival_id,
       (l.value ->> 'record_index')::INTEGER AS bronze_record_index,
       a.source_file, a.file_sha256, a.schema_version, a.batch_id, a.arrived_at, a.dag_run_id,
       r.payload AS bronze_payload
FROM gold.daily_sales_by_category g
JOIN ops.release rel     ON rel.release_id = g.release_id
CROSS JOIN LATERAL jsonb_array_elements(g.bronze_lineage) AS l (value)
JOIN bronze.record r     ON r.arrival_id = (l.value ->> 'arrival_id')::BIGINT
                        AND r.record_index = (l.value ->> 'record_index')::INTEGER
JOIN bronze.arrival a    ON a.arrival_id = r.arrival_id;

CREATE OR REPLACE VIEW gold.v_trace_customer_summary AS
SELECT g.release_id, rel.release_version, g.customer_id, g.customer_name, g.revenue,
       (l.value ->> 'arrival_id')::BIGINT    AS bronze_arrival_id,
       (l.value ->> 'record_index')::INTEGER AS bronze_record_index,
       a.source_file, a.file_sha256, a.schema_version, a.batch_id, a.arrived_at, a.dag_run_id,
       r.payload AS bronze_payload
FROM gold.customer_summary g
JOIN ops.release rel     ON rel.release_id = g.release_id
CROSS JOIN LATERAL jsonb_array_elements(g.bronze_lineage) AS l (value)
JOIN bronze.record r     ON r.arrival_id = (l.value ->> 'arrival_id')::BIGINT
                        AND r.record_index = (l.value ->> 'record_index')::INTEGER
JOIN bronze.arrival a    ON a.arrival_id = r.arrival_id;

-- Lineage = the product's own catalog record + the order records that sold it.
CREATE OR REPLACE VIEW gold.v_trace_product_performance AS
SELECT g.release_id, rel.release_version, g.product_id, g.product_name, g.revenue,
       (l.value ->> 'arrival_id')::BIGINT    AS bronze_arrival_id,
       (l.value ->> 'record_index')::INTEGER AS bronze_record_index,
       a.source_file, a.file_sha256, a.schema_version, a.batch_id, a.arrived_at, a.dag_run_id,
       r.payload AS bronze_payload
FROM gold.product_performance g
JOIN ops.release rel     ON rel.release_id = g.release_id
CROSS JOIN LATERAL jsonb_array_elements(g.bronze_lineage) AS l (value)
JOIN bronze.record r     ON r.arrival_id = (l.value ->> 'arrival_id')::BIGINT
                        AND r.record_index = (l.value ->> 'record_index')::INTEGER
JOIN bronze.arrival a    ON a.arrival_id = r.arrival_id;
