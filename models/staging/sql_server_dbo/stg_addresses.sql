{{ config(materialized = 'view') }}

with source as (

    select * 
    from {{ source('sql_server_dbo', 'addresses') }}

),

cleaned as (

    select
        address_id,
        address,
        zipcode,
        country,
        state,
        _fivetran_synced
    from source
    where _fivetran_deleted is null or _fivetran_deleted = false

)

select * from cleaned
