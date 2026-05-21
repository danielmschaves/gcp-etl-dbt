WITH fact_order_items AS (
    SELECT
        order_date_key,
        sale_price
    FROM {{ ref('fact_order_items') }}
),

dim_date AS (
    SELECT
        date_key,
        date
    FROM {{ ref('dim_date') }}
)

SELECT
    dd.date,
    SUM(foi.sale_price) AS total_revenue
FROM fact_order_items foi
JOIN dim_date dd ON foi.order_date_key = dd.date_key
GROUP BY dd.date
ORDER BY dd.date
