{{ config(materialized = 'view') }}

with source as (

    select * 
    from {{ source('sql_server_dbo', 'order_items') }}

),

cleaned as (

    select
        order_id,
        product_id,
        quantity,
        _fivetran_synced
    from source
    where _fivetran_deleted is null or _fivetran_deleted = false

)

select * from cleaned
