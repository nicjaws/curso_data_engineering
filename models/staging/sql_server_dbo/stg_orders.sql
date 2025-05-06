{{ config(
    materialized = 'view'
) }}

with base as (

    select *
    from {{ source('sql_server_dbo', 'orders') }}

),

cleaned as (

    select
        order_id,
        shipping_service,
        shipping_cost,
        address_id,
        created_at,
        promo_id,
        estimated_delivery_at,
        order_cost,
        user_id,
        order_total,
        delivered_at,
        tracking_id,
        status,
        _fivetran_synced
    from base
    where _fivetran_deleted = false or _fivetran_deleted is null

)

select * from cleaned



