
  
  create view "dbt"."main"."stg_inventory_items__dbt_tmp" as (
    

with stg_invetory_items as (
    select 
        cast(id as int) as id,
        cast(product_id as int) as product_id,
        cast(created_at as timestamp) as created_at,
        cast(sold_at as timestamp) as sold_at,
        cast(cost as float) as cost,
        cast(product_category as string) as product_category,
        cast(product_name as string) as product_name,
        cast(product_brand as string) as product_brand,
        cast(product_retail_price as float) as product_retail_price,
        cast(product_department as string) as product_department,
        cast(product_sku as string) as product_sku,
        cast(product_distribution_center_id as int) as product_distribution_center_id
    from read_parquet('s3://pypi-gcp-duckdb-dbt-197398273774/inventory_items.parquet')
)

select * from stg_invetory_items
  );
