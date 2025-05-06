{{ config(materialized = 'view') }}

with source as (

    select * 
    from {{ source('sql_server_dbo', 'users') }}

),

cleaned as (

    select
        user_id,
        first_name,
        last_name,
        email,
        phone_number,
        address_id,
        total_orders,
        created_at,
        updated_at,
        _fivetran_synced
    from source
    where _fivetran_deleted is null or _fivetran_deleted = false

)

select * from cleaned
