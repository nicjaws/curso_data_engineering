{{ config(materialized = 'view') }}

with source as (

    select * 
    from {{ source('sql_server_dbo', 'events') }}

),

cleaned as (

    select
        event_id,
        event_type,
        page_url,
        product_id,
        user_id,
        session_id,
        created_at,
        order_id,
        _fivetran_synced
    from source
    where _fivetran_deleted is null or _fivetran_deleted = false

)

select * from cleaned
