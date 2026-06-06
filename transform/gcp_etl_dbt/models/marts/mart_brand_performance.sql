WITH fact_order_items AS (
    SELECT
        product_id,
        sale_price,
        profit,
        margin
    FROM {{ ref('fact_order_items') }}
),

dim_products AS (
    SELECT
        product_id,
        brand,
        category
    FROM {{ ref('dim_products') }}
)

SELECT
    dp.brand,
    dp.category,
    SUM(foi.sale_price)              AS total_revenue,
    SUM(foi.profit)                  AS total_profit,
    ROUND(AVG(foi.margin) * 100, 2)  AS avg_margin_pct,
    COUNT(*)                         AS items_sold
FROM fact_order_items foi
JOIN dim_products dp ON foi.product_id = dp.product_id
GROUP BY dp.brand, dp.category
ORDER BY total_revenue DESC
