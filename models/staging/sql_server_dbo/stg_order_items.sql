{{ config(materialized='view') }}

with source as (
  select * from {{ source('sql_server_dbo', 'order_items') }}
),

renamed as (
  select
    order_id::string as order_id,
    product_id::string as product_id,
    quantity::int as quantity,
    _fivetran_synced::timestamp as _fivetran_synced
  from source
  where coalesce(_fivetran_deleted, false) = false
)

select * from renamed
