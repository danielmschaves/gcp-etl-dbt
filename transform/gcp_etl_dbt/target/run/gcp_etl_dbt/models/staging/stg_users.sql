
  
    
    

    create  table
      "dbt"."main"."stg_users__dbt_tmp"
  
    as (
      

with stg_users as (
    select 
        cast(id as int) as user_id,
        cast(first_name as string) as first_name,
        cast(last_name as string) as last_name,
        cast(email as string) as email,
        cast(age as int) as age,
        cast(gender as string) as gender,
        cast(state as string) as state,
        cast(street_address as string) as street_address,
        cast(postal_code as string) as postal_code,
        cast(city as string) as city,
        cast(country as string) as country,
        cast(latitude as float) as latitude,
        cast(longitude as float) as longitude,
        cast(traffic_source as string) as traffic_source,
        cast(created_at as timestamp) as created_at

    from read_parquet('s3://pypi-gcp-duckdb-dbt-197398273774/users.parquet')
)

select * from stg_users
    );
  
  