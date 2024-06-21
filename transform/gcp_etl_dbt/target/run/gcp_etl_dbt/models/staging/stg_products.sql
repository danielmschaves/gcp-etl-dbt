
  
  create view "dbt"."main"."stg_products__dbt_tmp" as (
    

with stg_products as (
    select 
        cast(id as int) as product_id,
        cast(cost as float) as cost,
        cast(category as string) as category,
        cast(name as string) as name,
        cast(brand as string) as brand,
        cast(retail_price as float) as retail_price,
        cast(department as string) as department,
        cast(sku as string) as sku,
        cast(distribution_center_id as int) as distribution_center_id

    from read_parquet('s3://pypi-gcp-duckdb-dbt-197398273774/products.parquet')
)

select * from stg_products
  );
