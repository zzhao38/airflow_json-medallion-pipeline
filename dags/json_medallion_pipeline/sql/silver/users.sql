-- users.json -> silver.users (upsert) + silver.user_activities (replace per user)
CREATE TEMP TABLE batch_users ON COMMIT DROP AS
SELECT DISTINCT ON ((payload ->> 'id')::INTEGER)
       (payload ->> 'id')::INTEGER AS user_id,
       payload,
       logical_date
FROM bronze.raw_users
WHERE logical_date = '{{ ds }}'
ORDER BY (payload ->> 'id')::INTEGER, record_index DESC;

INSERT INTO silver.users (user_id, user_name, age, email, logical_date, updated_at)
SELECT user_id,
       payload ->> 'name',
       (payload -> 'profile' ->> 'age')::INTEGER,
       lower(payload -> 'profile' ->> 'email'),
       logical_date,
       now()
FROM batch_users
ON CONFLICT (user_id) DO UPDATE
SET user_name    = EXCLUDED.user_name,
    age          = EXCLUDED.age,
    email        = EXCLUDED.email,
    logical_date = EXCLUDED.logical_date,
    updated_at   = now();

-- Activities carry no natural key, so the child set is replaced for every user in the batch.
DELETE FROM silver.user_activities a
USING batch_users b
WHERE a.user_id = b.user_id;

INSERT INTO silver.user_activities (user_id, activity_seq, activity_type, duration_minutes, logical_date)
SELECT b.user_id,
       act.ordinality,
       act.value ->> 'type',
       (act.value ->> 'duration_minutes')::INTEGER,
       b.logical_date
FROM batch_users b
CROSS JOIN LATERAL jsonb_array_elements(
    COALESCE(b.payload -> 'profile' -> 'activities', '[]'::JSONB)
) WITH ORDINALITY AS act (value, ordinality);
