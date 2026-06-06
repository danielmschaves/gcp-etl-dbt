WITH events AS (
    SELECT
        event_type,
        session_id,
        user_id
    FROM {{ ref('stg_events') }}
)

SELECT
    event_type,
    COUNT(*)                    AS event_count,
    COUNT(DISTINCT session_id)  AS unique_sessions,
    COUNT(DISTINCT user_id)     AS unique_users
FROM events
GROUP BY event_type
ORDER BY event_count DESC
