{{ config(materialized='view' )}}

with stg_products as (
    select 
        cast(id as int) as id,
        cast(cost as float) as cost,
        cast(product_category as string) as category,
        cast(product_name as string) as name,
        cast(product_brand as string) as brand,
        cast(product_retail_price as float) as retail_price,
        cast(product_department as string) as department,
        cast(product_sku as string) as sku,
        cast(product_distribution_center_id as int) as distribution_center_id

    from {{ source('ecommerce', 'products' ) }}
)

select * from stg_products