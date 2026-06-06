WITH stg_inventory_items AS (
    SELECT
        CAST(id AS INTEGER)                            AS id,
        CAST(product_id AS INTEGER)                    AS product_id,
        CAST(created_at AS TIMESTAMP)                  AS created_at,
        CAST(sold_at AS TIMESTAMP)                     AS sold_at,
        CAST(cost AS FLOAT)                            AS cost,
        CAST(product_category AS VARCHAR)              AS product_category,
        CAST(product_name AS VARCHAR)                  AS product_name,
        CAST(product_brand AS VARCHAR)                 AS product_brand,
        CAST(product_retail_price AS FLOAT)            AS product_retail_price,
        CAST(product_department AS VARCHAR)            AS product_department,
        CAST(product_sku AS VARCHAR)                   AS product_sku,
        CAST(product_distribution_center_id AS INTEGER) AS product_distribution_center_id
    FROM {{ source('ecommerce', 'inventory_items') }}
)

SELECT * FROM stg_inventory_items
