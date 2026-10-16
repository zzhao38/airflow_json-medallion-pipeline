-- Mirrors the expected output of the "nested users" exercise: total minutes per user.
TRUNCATE gold.user_activity_summary;

INSERT INTO gold.user_activity_summary (user_id, name, email, activity_count, total_activity_duration)
SELECT u.user_id,
       u.user_name,
       u.email,
       COUNT(a.activity_seq),
       COALESCE(SUM(a.duration_minutes), 0)
FROM silver.users u
LEFT JOIN silver.user_activities a USING (user_id)
GROUP BY u.user_id, u.user_name, u.email
ORDER BY u.user_id;
