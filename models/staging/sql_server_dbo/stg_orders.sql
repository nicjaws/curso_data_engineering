{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert'
    )
}}

with source as (
  select * from {{ source('sql_server_dbo', 'orders') }}
  
  {% if is_incremental() %}
  -- Only get records updated since the last run
  where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})
  {% endif %}
),

renamed as (
  select
    order_id::string as order_id,
    shipping_service::string as shipping_service,
    shipping_cost::float as shipping_cost,
    address_id::string as address_id,
    created_at::timestamp as created_at,
    promo_id::string as promo_id,
    estimated_delivery_at::timestamp as estimated_delivery_at,
    order_cost::float as order_cost,
    user_id::string as user_id,
    order_total::float as order_total,
    delivered_at::timestamp as delivered_at,
    tracking_id::string as tracking_id,
    status::string as status,
    _fivetran_synced::timestamp as _fivetran_synced
  from source
  where coalesce(_fivetran_deleted, false) = false
)

select * from renamed