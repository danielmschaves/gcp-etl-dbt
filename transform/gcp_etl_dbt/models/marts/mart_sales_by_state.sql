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
        state
    FROM {{ ref('dim_users') }}
)

SELECT
    du.state,
    SUM(foi.sale_price)              AS total_sales,
    COUNT(DISTINCT foi.order_id)     AS order_count
FROM fact_order_items foi
JOIN dim_users du ON foi.user_id = du.user_id
WHERE du.state IS NOT NULL
GROUP BY du.state
ORDER BY total_sales DESC
