{{ config(materialized='view') }}

with source as (
  select * from {{ source('sql_server_dbo', 'products') }}
),

renamed as (
  select
    product_id::string as product_id,
    price::float as price,
    name::string as name,
    inventory::int as inventory,
    _fivetran_synced::timestamp as _fivetran_synced
  from source
  where coalesce(_fivetran_deleted, false) = false
)

select * from renamed
