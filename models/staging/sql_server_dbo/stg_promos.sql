{{ config(materialized='view') }}

with source as (
  select * from alumno24_dev_bronze_db.sql_server_dbo.promos
),
renamed as (
  select
    md5(cast(coalesce(cast(promo_id as TEXT), '') as TEXT)) as promo_hash_key,
    CAST(promo_id AS VARCHAR) AS promo_id,
    CAST(discount AS INTEGER) AS discount,
    CAST(status AS VARCHAR) AS status,
    CAST(_fivetran_deleted AS BOOLEAN) AS is_deleted,
    CAST(_fivetran_synced AS TIMESTAMP) AS fivetran_synced
  from source
  where coalesce(_fivetran_deleted, false) = false
)
select * from renamed