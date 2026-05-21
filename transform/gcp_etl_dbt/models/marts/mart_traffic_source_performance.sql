WITH fact_order_items AS (
    SELECT
        user_id,
        order_id,
        sale_price
    FROM {{ ref('fact_order_items') }}
),

dim_users AS (
    SELECT
        user_id,
        traffic_source
    FROM {{ ref('dim_users') }}
)

SELECT
    du.traffic_source,
    SUM(foi.sale_price)                                       AS total_revenue,
    COUNT(DISTINCT foi.order_id)                              AS order_count,
    ROUND(SUM(foi.sale_price) / COUNT(DISTINCT foi.order_id), 2) AS avg_order_value,
    COUNT(DISTINCT foi.user_id)                               AS user_count
FROM fact_order_items foi
JOIN dim_users du ON foi.user_id = du.user_id
WHERE du.traffic_source IS NOT NULL
GROUP BY du.traffic_source
ORDER BY total_revenue DESC
