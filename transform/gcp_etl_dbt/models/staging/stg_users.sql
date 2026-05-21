WITH stg_users AS (
    SELECT
        CAST(id AS INTEGER)           AS user_id,
        CAST(first_name AS VARCHAR)   AS first_name,
        CAST(last_name AS VARCHAR)    AS last_name,
        CAST(email AS VARCHAR)        AS email,
        CAST(age AS INTEGER)          AS age,
        CAST(gender AS VARCHAR)       AS gender,
        CAST(state AS VARCHAR)        AS state,
        CAST(street_address AS VARCHAR) AS street_address,
        CAST(postal_code AS VARCHAR)  AS postal_code,
        CAST(city AS VARCHAR)         AS city,
        CAST(country AS VARCHAR)      AS country,
        CAST(latitude AS FLOAT)       AS latitude,
        CAST(longitude AS FLOAT)      AS longitude,
        CAST(traffic_source AS VARCHAR) AS traffic_source,
        CAST(created_at AS TIMESTAMP) AS created_at
    FROM {{ source('ecommerce', 'users') }}
)

SELECT * FROM stg_users
