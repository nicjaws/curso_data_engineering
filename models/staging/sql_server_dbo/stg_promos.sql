{{ config(materialized = 'view') }}

with source as (

    select * 
    from {{ source('sql_server_dbo', 'promos') }}

),

cleaned as (

    select
        promo_id,
        discount,
        status,
        _fivetran_synced
    from source
    where _fivetran_deleted is null or _fivetran_deleted = false

)

select * from cleaned
