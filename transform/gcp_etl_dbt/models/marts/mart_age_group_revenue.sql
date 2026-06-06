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
        age,
        CASE
            WHEN age < 25              THEN '<25'
            WHEN age >= 25 AND age < 35 THEN '25-34'
            WHEN age >= 35 AND age < 45 THEN '35-44'
            WHEN age >= 45 AND age < 55 THEN '45-54'
            ELSE '55+'
        END AS age_group
    FROM {{ ref('dim_users') }}
    WHERE age IS NOT NULL
)

SELECT
    du.age_group,
    COUNT(DISTINCT foi.user_id)                               AS user_count,
    SUM(foi.sale_price)                                       AS total_revenue,
    ROUND(SUM(foi.sale_price) / COUNT(DISTINCT foi.order_id), 2) AS avg_order_value
FROM fact_order_items foi
JOIN dim_users du ON foi.user_id = du.user_id
GROUP BY du.age_group
ORDER BY
    CASE du.age_group
        WHEN '<25'   THEN 1
        WHEN '25-34' THEN 2
        WHEN '35-44' THEN 3
        WHEN '45-54' THEN 4
        ELSE 5
    END
