WITH dim_products AS (
    SELECT
        product_id,
        cost,
        name,
        category,
        brand,
        department,
        sku,
        retail_price
    FROM {{ ref('stg_products') }}
)

SELECT * FROM dim_products
