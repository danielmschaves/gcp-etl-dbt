WITH fact_order_items AS (
    SELECT
        product_id,
        status
    FROM {{ ref('fact_order_items') }}
),

dim_products AS (
    SELECT
        product_id,
        category
    FROM {{ ref('dim_products') }}
)

SELECT
    dp.category,
    COUNT(*)                                                            AS total_items,
    COUNT(*) FILTER (WHERE foi.status = 'Returned')                    AS returned_items,
    ROUND(
        COUNT(*) FILTER (WHERE foi.status = 'Returned') * 100.0 / COUNT(*),
        2
    )                                                                   AS return_rate_pct
FROM fact_order_items foi
JOIN dim_products dp ON foi.product_id = dp.product_id
GROUP BY dp.category
ORDER BY return_rate_pct DESC
