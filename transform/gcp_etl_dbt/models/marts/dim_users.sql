WITH dim_users AS (
    SELECT
        user_id,
        first_name,
        last_name,
        email,
        age,
        gender,
        city,
        state,
        country,
        latitude,
        longitude,
        traffic_source
    FROM {{ ref('stg_users') }}
)

SELECT * FROM dim_users
