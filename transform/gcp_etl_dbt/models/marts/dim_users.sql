
{{ config(
    schema='gold',
    materialized='table'
) }}

with dim_users as (
    select
        user_id,
        first_name,
        last_name,
        email,
        age,
        gender,
        city,
        state,
        country,
        latitude,
        longitude,
        traffic_source
    from {{ ref('stg_users') }}
)

select * from dim_users