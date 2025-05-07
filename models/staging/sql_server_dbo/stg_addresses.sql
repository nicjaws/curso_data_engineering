{{ config(materialized='view') }}

with source as (
  select * from {{ source('sql_server_dbo', 'addresses') }}
),

renamed as (
  select
    address_id::string as address_id,
    zipcode::int as zipcode,
    country::string as country,
    address::string as address,
    state::string as state,
    _fivetran_synced::timestamp as _fivetran_synced
  from source
  where coalesce(_fivetran_deleted, false) = false
)

select * from renamed
