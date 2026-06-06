WITH dim_orders AS (
    SELECT
        order_id,
        user_id,
        status,
        created_at,
        shipped_at,
        delivered_at,
        returned_at,
        num_of_item
    FROM {{ ref('stg_orders') }}
)

SELECT * FROM dim_orders
