WITH order_items AS (
    SELECT
        created_at,
        shipped_at
    FROM {{ ref('stg_order_items') }}
    WHERE shipped_at IS NOT NULL
      AND created_at IS NOT NULL
)

SELECT
    ROUND(AVG(DATEDIFF('day', created_at, shipped_at)), 2) AS avg_days_to_ship
FROM order_items
