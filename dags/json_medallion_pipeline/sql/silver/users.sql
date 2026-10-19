-- users.json -> silver.users (upsert) + silver.user_activities (replace per user)
-- Grain/key: user_id; activities keyed (user_id, activity_seq).
-- Rerun: users upsert on user_id; each batch user's activities are deleted and re-inserted.
-- Lineage: bronze_* columns carry the winning bronze record; activities inherit their user's.

-- Temp table is dropped at commit; it holds the deduplicated batch reused by every statement below.
-- DISTINCT ON + ORDER BY record_index DESC: the last record wins for a duplicated id.
CREATE TEMP TABLE batch_users ON COMMIT DROP AS
SELECT DISTINCT ON ((payload ->> 'id')::INTEGER)
       (payload ->> 'id')::INTEGER AS user_id,
       payload,
       logical_date,
       record_index
FROM bronze.raw_users
WHERE logical_date = '{{ ds }}'
ORDER BY (payload ->> 'id')::INTEGER, record_index DESC;

INSERT INTO silver.users
    (user_id, user_name, age, email, bronze_logical_date, bronze_record_index, updated_at)
SELECT user_id,
       payload ->> 'name',
       (payload -> 'profile' ->> 'age')::INTEGER,
       lower(payload -> 'profile' ->> 'email'),
       logical_date,
       record_index,
       now()
FROM batch_users
ON CONFLICT (user_id) DO UPDATE
SET user_name           = EXCLUDED.user_name,
    age                 = EXCLUDED.age,
    email               = EXCLUDED.email,
    bronze_logical_date = EXCLUDED.bronze_logical_date,
    bronze_record_index = EXCLUDED.bronze_record_index,
    updated_at          = now();

-- Activities carry no natural key, so the child set is replaced for every user in the batch.
DELETE FROM silver.user_activities a
USING batch_users b
WHERE a.user_id = b.user_id;

INSERT INTO silver.user_activities
    (user_id, activity_seq, activity_type, duration_minutes,
     bronze_logical_date, bronze_record_index)
SELECT b.user_id,
       act.ordinality,
       act.value ->> 'type',
       (act.value ->> 'duration_minutes')::INTEGER,
       b.logical_date,
       b.record_index
FROM batch_users b
-- WITH ORDINALITY: the array position becomes the child key (activity_seq).
CROSS JOIN LATERAL jsonb_array_elements(
    COALESCE(b.payload -> 'profile' -> 'activities', '[]'::JSONB)
) WITH ORDINALITY AS act (value, ordinality);
