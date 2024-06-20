
{{ config(materialized='view' ) }}

with stg_distribution_centers as (
    select 
        cast(id as int) as id,
        cast(name as string) as name,
        cast(latitude as float) as latitude,
        cast(longitude as float) as longitude
    from {{ source('ecommerce', 'distribution_centers' ) }}
)

select * from stg_distribution_centers