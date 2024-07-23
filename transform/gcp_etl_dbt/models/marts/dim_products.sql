{{ config(
    schema='gold',
    materialized='table'
) }}

with dim_products as (
    select
        product_id,
        cost,
        name,
        category,
        brand,
        department,
        sku,
        retail_price
    from {{ ref('stg_products') }}
)

select * from dim_products
