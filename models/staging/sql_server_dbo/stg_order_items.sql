{{ 
  config(
    materialized='incremental',
    unique_key=['order_id', 'product_id'],
    on_schema_change='fail'
  ) 
}}

with source as (
  select * from {{ source('sql_server_dbo', 'order_items') }}
  
  {% if is_incremental() %}
    -- Solo procesar registros que han sido actualizados desde la última ejecución
    where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})
  {% endif %}
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