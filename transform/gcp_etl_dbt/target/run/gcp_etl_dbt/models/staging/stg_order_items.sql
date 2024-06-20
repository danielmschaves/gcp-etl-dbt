
  
  create view "ecommerce"."the_look_ecommerce"."stg_order_items__dbt_tmp" as (
    

with stg_order_items as (
    select 
        cast(id as int) as id,
        cast(order_id as int) as order_id,
        cast(user_id as int) as user_id,
        cast(product_id as int) as product_id,
        cast(inventory_item_id as int) as inventory_item_id,
        cast(status as string) as status,
        cast(created_at as timestamp) as created_at,
        cast(shipped_at as timestamp) as shipped_at,
        cast(delivered_at as timestamp) as delivered_at,
        cast(returned_at as timestamp) as returned_at,
        cast(sale_price as float) as sale_price
    from read_parquet('s3://pypi-gcp-duckdb-dbt-197398273774/order_items.parquet')
)

select * from stg_order_items
  );
