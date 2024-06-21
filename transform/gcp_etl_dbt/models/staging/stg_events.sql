{{ config(materialized='view' )}}

with stg_events as (
    select 
        cast(id as int) as event_id,
        cast(user_id as int) as user_id,
        cast(sequence_number as int) as sequence_number,
        cast(session_id as string) as session_id,
        cast(created_at as timestamp) as created_at,
        cast(ip_address as string) as ip_address,
        cast(city as string) as city,
        cast(state as string) as state,
        cast(postal_code as string) as postal_code,
        cast(browser as string) as browser, 
        cast(traffic_source as string) as traffic_source,
        cast(uri as string) as uri,
        cast(event_type as string) as event_type
    
    from {{ source('ecommerce', 'events' ) }}
)

select * from stg_events