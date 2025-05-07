{{ config(materialized='view') }}

with source as (
  select * from {{ source('sql_server_dbo', 'users') }}
),

renamed as (
  select
    user_id::string as user_id,
    updated_at::timestamp as updated_at,
    address_id::string as address_id,
    last_name::string as last_name,
    created_at::timestamp as created_at,
    phone_number::string as phone_number,
    total_orders::int as total_orders,
    first_name::string as first_name,
    email::string as email,
    _fivetran_synced::timestamp as _fivetran_synced
  from source
  where coalesce(_fivetran_deleted, false) = false
)

select * from renamed
