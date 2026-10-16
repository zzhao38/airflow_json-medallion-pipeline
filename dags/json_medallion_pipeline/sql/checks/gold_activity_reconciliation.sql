-- Reconciliation: gold totals must equal the sum of silver activity minutes.
-- SQLCheckOperator fails if any returned value is falsy (0 / NULL / false).
SELECT (SELECT COALESCE(SUM(total_activity_duration), 0) FROM gold.user_activity_summary)
     = (SELECT COALESCE(SUM(duration_minutes), 0)        FROM silver.user_activities) AS totals_match,
       (SELECT COUNT(*) FROM gold.user_activity_summary)
     = (SELECT COUNT(*) FROM silver.users)                                             AS user_counts_match;
