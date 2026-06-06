WITH stg_events AS (
    SELECT
        CAST(id AS INTEGER)               AS event_id,
        CAST(user_id AS INTEGER)          AS user_id,
        CAST(sequence_number AS INTEGER)  AS sequence_number,
        CAST(session_id AS VARCHAR)       AS session_id,
        CAST(created_at AS TIMESTAMP)     AS created_at,
        CAST(ip_address AS VARCHAR)       AS ip_address,
        CAST(city AS VARCHAR)             AS city,
        CAST(state AS VARCHAR)            AS state,
        CAST(postal_code AS VARCHAR)      AS postal_code,
        CAST(browser AS VARCHAR)          AS browser,
        CAST(traffic_source AS VARCHAR)   AS traffic_source,
        CAST(uri AS VARCHAR)              AS uri,
        CAST(event_type AS VARCHAR)       AS event_type
    FROM {{ source('ecommerce', 'events') }}
)

SELECT * FROM stg_events
