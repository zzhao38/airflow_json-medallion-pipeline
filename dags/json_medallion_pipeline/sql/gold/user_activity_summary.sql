-- Mirrors the expected output of the "nested users" exercise: total minutes per user.
-- Grain/key: user_id. Rerun: full refresh (TRUNCATE + INSERT in one transaction).
-- Lineage: one user = one bronze record (activities inherit it), so plain bronze_* columns.
TRUNCATE gold.user_activity_summary;

INSERT INTO gold.user_activity_summary
    (user_id, name, email, activity_count, total_activity_duration,
     bronze_logical_date, bronze_record_index)
SELECT u.user_id,
       u.user_name,
       u.email,
       COUNT(a.activity_seq),
       COALESCE(SUM(a.duration_minutes), 0),
       u.bronze_logical_date,
       u.bronze_record_index
FROM silver.users u
LEFT JOIN silver.user_activities a USING (user_id)
GROUP BY u.user_id, u.user_name, u.email, u.bronze_logical_date, u.bronze_record_index
ORDER BY u.user_id;
