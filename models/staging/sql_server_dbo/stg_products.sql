{{ config(materialized = 'view') }}

with source as (

    select * 
    from {{ source('sql_server_dbo', 'products') }}

),

cleaned as (

    select
        product_id,
        name as product_name,
        price,
        inventory,
        _fivetran_synced
    from source
    where _fivetran_deleted is null or _fivetran_deleted = false

)

select * from cleaned
