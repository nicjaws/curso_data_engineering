with source as (
  select * from {{ source('sql_server_dbo', 'events') }}

  {% if is_incremental() %}
  where _fivetran_synced > (select max(_fivetran_synced) from {{ this }})
  {% endif %}
),

renamed as (
  select
    event_id::string as event_id,
    page_url::string as page_url,
    event_type::string as event_type,
    user_id::string as user_id,
    product_id::string as product_id,
    session_id::string as session_id,
    created_at::timestamp as created_at,
    order_id::string as order_id,
    _fivetran_synced::timestamp as _fivetran_synced
  from source
  where coalesce(_fivetran_deleted, false) = false
)

select * from renamed