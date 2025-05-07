{{ config(materialized='view') }}

with source as (
  select * from {{ source('sql_server_dbo', 'promos') }}
),

renamed as (
  select
    {{ dbt_utils.surrogate_key(['promo_id']) }} as promo_hash_key
    promo_id::string as promo_id,
    discount::float as discount,
    status::string as status,
    _fivetran_synced::timestamp as _fivetran_synced
  from source
  where coalesce(_fivetran_deleted, false) = false
)

select * from renamed
