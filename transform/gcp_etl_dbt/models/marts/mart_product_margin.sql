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
        name  AS product_name,
        category,
        brand
    FROM {{ ref('dim_products') }}
)

SELECT
    dp.product_name,
    dp.category,
    dp.brand,
    SUM(foi.sale_price)              AS total_revenue,
    SUM(foi.profit)                  AS total_profit,
    ROUND(AVG(foi.margin) * 100, 2)  AS avg_margin_pct,
    COUNT(*)                         AS items_sold
FROM fact_order_items foi
JOIN dim_products dp ON foi.product_id = dp.product_id
GROUP BY dp.product_name, dp.category, dp.brand
ORDER BY total_profit DESC
LIMIT 20
