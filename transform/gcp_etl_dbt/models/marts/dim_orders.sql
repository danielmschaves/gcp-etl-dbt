-- model/dim_orders.sql

{{ config(
    schema='gold',
    materialized='table'
) }}

with dim_orders as (
    select
        order_item_id,
        order_id,
        user_id,
        product_id,
        status,
        created_at,
        shipped_at,
        delivered_at,
        returned_at,
        sale_price
    from {{ ref('stg_orders') }}
)

select * from dim_orders
