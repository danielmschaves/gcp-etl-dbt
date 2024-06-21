-- model/dim_orders.sql

{{ config(
    schema='gold',
    materialized='table'
) }}

with dim_orders as (
    select
        order_id,
        user_id,
        status,
        created_at,
        shipped_at,
        delivered_at,
        returned_at
)

select * from dim_orders