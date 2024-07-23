
  
    
    

    create  table
      "dbt"."main"."stg_distribution_centers__dbt_tmp"
  
    as (
      

with stg_distribution_centers as (
    select 
        cast(id as int) as id,
        cast(name as string) as name,
        cast(latitude as float) as latitude,
        cast(longitude as float) as longitude
    from read_parquet('s3://pypi-gcp-duckdb-dbt-197398273774/distribution_centers.parquet')
)

select * from stg_distribution_centers
    );
  
  